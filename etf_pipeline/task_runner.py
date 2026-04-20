"""
任务编排模块。

TaskRunner 负责按顺序编排各数据管道模块，支持多表多管道并行执行，
记录运行日志与汇总统计。
"""

from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd

from etf_pipeline.cleaner import DataCleaner, EmptyDataError
from etf_pipeline.config import PipelineConfig
from etf_pipeline.db_initializer import DBInitializer
from etf_pipeline.exporter import Exporter
from etf_pipeline.fetcher import AdjustFactorFetcher, AmountFetcher, DataFetcher, IndexFetcher
from etf_pipeline.fetcher_metadata import MetadataFetcher
from etf_pipeline.fetcher_nav import NavFetcher
from etf_pipeline.logger import get_error_logger, get_run_logger
from etf_pipeline.models import PipelineResult
from etf_pipeline.transformer import DataTransformer
from etf_pipeline.writer import DataWriter

__all__ = ["TaskRunner"]


class TaskRunner:
    """协调各模块完成 ETF 数据仓的完整更新任务。"""

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._run_logger: logging.Logger = get_run_logger(
            config.run_log_dir, config.log_retention_days
        )
        self._error_logger: logging.Logger = get_error_logger(
            config.error_log_dir, config.log_retention_days
        )
        self._writer = DataWriter(config, self._error_logger)
        self._fetcher = DataFetcher(config, self._error_logger)
        self._index_fetcher = IndexFetcher(config, self._error_logger)
        self._adjust_fetcher = AdjustFactorFetcher(config, self._error_logger)
        self._nav_fetcher = NavFetcher(config, self._error_logger)
        self._metadata_fetcher = MetadataFetcher(config, self._error_logger)
        self._amount_fetcher = AmountFetcher(config, self._error_logger)
        self._cleaner = DataCleaner(self._error_logger)
        self._transformer = DataTransformer()

    def run(self, trade_date: date) -> list[PipelineResult]:
        """完整任务编排，依次执行所有管道。

        管道执行顺序：
        1. DB 初始化（幂等）
        2. 缓存恢复（可选）
        3. ETF 日线行情
        4. 跟踪指数行情
        5. 复权因子
        6. ETF 净值
        7. 折溢价率（依赖 3+6）
        8. ETF 元数据
        9. Parquet 导出（可选）
        """
        start_time = time.time()
        self._run_logger.info(
            "任务开始：trade_date=%s, timestamp=%s",
            trade_date,
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)),
        )

        results: list[PipelineResult] = []

        # 1. DB 初始化
        try:
            session = self._writer._get_session()
            db_init = DBInitializer(session, self._config)
            db_init.ensure_initialized()
        except Exception as exc:
            self._run_logger.critical("DB 初始化失败：%s", exc)
            return results

        # 2. 缓存恢复
        if self._config.cache_recovery_on_startup:
            try:
                recovered = self._writer.recover_from_cache()
                if recovered > 0:
                    self._run_logger.info("缓存恢复完成，共 %d 条记录", recovered)
            except Exception as exc:
                self._error_logger.error("缓存恢复失败：%s", exc)

        # 3. ETF 日线行情
        print("\n[1/6] ETF 日线行情...")
        etf_daily_df = pd.DataFrame()
        result = self._run_etf_daily(trade_date)
        results.append(result)
        if result.success and hasattr(self, "_last_etf_daily_df"):
            etf_daily_df = self._last_etf_daily_df

        # 4. 跟踪指数行情
        print("\n[2/6] 跟踪指数行情...")
        result = self._run_index_daily(trade_date)
        results.append(result)

        # 5. 复权因子
        print("\n[3/6] 复权因子...")
        result = self._run_adjust_factor(trade_date)
        results.append(result)

        # 6. ETF 净值
        print("\n[4/6] ETF 净值...")
        nav_df = pd.DataFrame()
        result = self._run_nav(trade_date)
        results.append(result)
        if result.success and hasattr(self, "_last_nav_df"):
            nav_df = self._last_nav_df

        # 7. 折溢价率
        print("\n[5/6] 折溢价率...")
        result = self._run_premium(etf_daily_df, nav_df)
        results.append(result)

        # 8. ETF 元数据
        print("\n[6/6] ETF 元数据...")
        result = self._run_metadata()
        results.append(result)

        # 9. Parquet 导出
        if self._config.export.enabled:
            try:
                exporter = Exporter(self._config, session, self._error_logger)
                export_results = exporter.export_all()
                for er in export_results:
                    self._run_logger.info(
                        "导出完成：%s，%d 条记录，%d 个文件",
                        er.table_name, er.total_records, er.file_count,
                    )
            except Exception as exc:
                self._error_logger.error("Parquet 导出失败：%s", exc)

        elapsed = time.time() - start_time
        success_count = sum(1 for r in results if r.success)
        total_count = len(results)
        self._run_logger.info(
            "全部任务完成：成功=%d/%d，耗时=%.2fs",
            success_count, total_count, elapsed,
        )

        self._amount_fetcher.disconnect()

        return results

    def _run_etf_daily(self, trade_date: date) -> PipelineResult:
        """ETF 日线行情管道。"""
        start = time.time()
        try:
            raw_df = self._fetcher.fetch_all_etf(trade_date)

            if raw_df.empty and not self._fetcher.failed_symbols:
                return PipelineResult(
                    pipeline_name="etf_daily", table_name=self._config.tables.etf_daily,
                    success=True, record_count=0,
                    elapsed_seconds=time.time() - start,
                )

            write_count = 0
            total_drop = 0
            if not raw_df.empty:
                clean_df, clean_summary = self._cleaner.clean(raw_df, "etf_daily")
                std_df = self._transformer.transform_etf_daily(clean_df, trade_date)
                std_df, val_summary = self._cleaner.validate_etf_daily(std_df)
                total_drop = clean_summary.drop_count + val_summary.drop_count

                # 在写入前合并成交额（腾讯API不提供，从通达信pytdx获取）
                try:
                    raw_codes = self._fetcher.fetch_raw_codes()
                    if raw_codes:
                        amount_df = self._amount_fetcher.fetch_amount_by_date(raw_codes, trade_date)
                        if not amount_df.empty:
                            amt_map = dict(zip(amount_df["symbol"], amount_df["amount"]))
                            std_df["amount"] = std_df["symbol"].map(amt_map).fillna(0.0)
                except Exception as exc:
                    self._error_logger.warning("成交额获取失败: %s", exc)

                write_result = self._writer.write(std_df, self._config.tables.etf_daily)
                write_count = write_result.success_count
                self._last_etf_daily_df = std_df
            else:
                total_drop = 0

            if self._fetcher.failed_symbols:
                retry_df = self._fetcher.retry_failed(trade_date)
                if not retry_df.empty:
                    clean_retry, retry_clean_summary = self._cleaner.clean(retry_df, "etf_daily")
                    std_retry = self._transformer.transform_etf_daily(clean_retry, trade_date)
                    std_retry, retry_val_summary = self._cleaner.validate_etf_daily(std_retry)
                    total_drop += retry_clean_summary.drop_count + retry_val_summary.drop_count

                    try:
                        raw_codes = self._fetcher.fetch_raw_codes()
                        if raw_codes:
                            amount_df = self._amount_fetcher.fetch_amount_by_date(raw_codes, trade_date)
                            if not amount_df.empty:
                                amt_map = dict(zip(amount_df["symbol"], amount_df["amount"]))
                                std_retry["amount"] = std_retry["symbol"].map(amt_map).fillna(0.0)
                    except Exception as exc:
                        self._error_logger.warning("成交额获取失败(retry): %s", exc)

                    retry_result = self._writer.write(std_retry, self._config.tables.etf_daily)
                    write_count += retry_result.success_count

            return PipelineResult(
                pipeline_name="etf_daily",
                table_name=self._config.tables.etf_daily,
                success=True,
                record_count=write_count,
                skip_count=self._fetcher.skip_count,
                drop_count=total_drop,
                elapsed_seconds=time.time() - start,
            )
        except EmptyDataError as exc:
            return PipelineResult(
                pipeline_name="etf_daily", table_name=self._config.tables.etf_daily,
                success=False, error_message=str(exc),
                elapsed_seconds=time.time() - start,
            )
        except Exception as exc:
            return PipelineResult(
                pipeline_name="etf_daily", table_name=self._config.tables.etf_daily,
                success=False, error_message=str(exc),
                elapsed_seconds=time.time() - start,
            )

    def _run_index_daily(self, trade_date: date) -> PipelineResult:
        """跟踪指数行情管道。"""
        start = time.time()
        try:
            raw_df = self._index_fetcher.fetch_all_indices(trade_date)

            if raw_df.empty and not self._index_fetcher.failed_symbols:
                return PipelineResult(
                    pipeline_name="index_daily", table_name=self._config.tables.index_daily,
                    success=True, record_count=0,
                    elapsed_seconds=time.time() - start,
                )

            write_count = 0
            drop_count = 0
            if not raw_df.empty:
                clean_df, clean_summary = self._cleaner.clean(raw_df, "index_daily")
                std_df = self._transformer.transform_index_daily(clean_df, trade_date)
                write_result = self._writer.write(std_df, self._config.tables.index_daily)
                write_count = write_result.success_count
                drop_count = clean_summary.drop_count

            if self._index_fetcher.failed_symbols:
                retry_df = self._index_fetcher.retry_failed(trade_date)
                if not retry_df.empty:
                    clean_retry, _ = self._cleaner.clean(retry_df, "index_daily")
                    std_retry = self._transformer.transform_index_daily(clean_retry, trade_date)
                    retry_result = self._writer.write(std_retry, self._config.tables.index_daily)
                    write_count += retry_result.success_count

            return PipelineResult(
                pipeline_name="index_daily",
                table_name=self._config.tables.index_daily,
                success=True,
                record_count=write_count,
                skip_count=self._index_fetcher.skip_count,
                drop_count=drop_count,
                elapsed_seconds=time.time() - start,
            )
        except EmptyDataError as exc:
            return PipelineResult(
                pipeline_name="index_daily", table_name=self._config.tables.index_daily,
                success=False, error_message=str(exc),
                elapsed_seconds=time.time() - start,
            )
        except Exception as exc:
            return PipelineResult(
                pipeline_name="index_daily", table_name=self._config.tables.index_daily,
                success=False, error_message=str(exc),
                elapsed_seconds=time.time() - start,
            )

    def _run_adjust_factor(self, trade_date: date) -> PipelineResult:
        """复权因子管道。"""
        start = time.time()
        try:
            raw_df = self._adjust_fetcher.fetch_all_factors(trade_date)

            if raw_df.empty and not self._adjust_fetcher.failed_symbols:
                return PipelineResult(
                    pipeline_name="adjust_factor", table_name=self._config.tables.adjust_factor,
                    success=True, record_count=0,
                    elapsed_seconds=time.time() - start,
                )

            write_count = 0
            drop_count = 0
            if not raw_df.empty:
                clean_df, clean_summary = self._cleaner.clean(raw_df, "adjust_factor")
                std_df = self._transformer.transform_adjust_factor(clean_df, trade_date)
                write_result = self._writer.write(std_df, self._config.tables.adjust_factor)
                write_count = write_result.success_count
                drop_count = clean_summary.drop_count

            if self._adjust_fetcher.failed_symbols:
                retry_df = self._adjust_fetcher.retry_failed(trade_date)
                if not retry_df.empty:
                    clean_retry, _ = self._cleaner.clean(retry_df, "adjust_factor")
                    std_retry = self._transformer.transform_adjust_factor(clean_retry, trade_date)
                    retry_result = self._writer.write(std_retry, self._config.tables.adjust_factor)
                    write_count += retry_result.success_count

            return PipelineResult(
                pipeline_name="adjust_factor",
                table_name=self._config.tables.adjust_factor,
                success=True,
                record_count=write_count,
                skip_count=self._adjust_fetcher.skip_count,
                drop_count=drop_count,
                elapsed_seconds=time.time() - start,
            )
        except EmptyDataError:
            return PipelineResult(
                pipeline_name="adjust_factor", table_name=self._config.tables.adjust_factor,
                success=True, record_count=0,
                elapsed_seconds=time.time() - start,
            )
        except Exception as exc:
            return PipelineResult(
                pipeline_name="adjust_factor", table_name=self._config.tables.adjust_factor,
                success=False, error_message=str(exc),
                elapsed_seconds=time.time() - start,
            )

    def _run_nav(self, trade_date: date) -> PipelineResult:
        """ETF 净值管道。"""
        start = time.time()
        try:
            etf_codes = self._get_etf_raw_codes()
            if not etf_codes:
                return PipelineResult(
                    pipeline_name="etf_nav", table_name=self._config.tables.etf_nav,
                    success=False, error_message="ETF 列表为空",
                )

            raw_df = self._nav_fetcher.fetch_all_nav(etf_codes, trade_date)
            if raw_df.empty:
                return PipelineResult(
                    pipeline_name="etf_nav", table_name=self._config.tables.etf_nav,
                    success=True, record_count=0,
                    elapsed_seconds=time.time() - start,
                )

            clean_df, clean_summary = self._cleaner.clean(raw_df, "etf_nav")
            std_df = self._transformer.transform_nav(clean_df, trade_date)
            write_result = self._writer.write(std_df, self._config.tables.etf_nav)

            self._last_nav_df = std_df

            return PipelineResult(
                pipeline_name="etf_nav",
                table_name=self._config.tables.etf_nav,
                success=True,
                record_count=write_result.success_count,
                skip_count=self._nav_fetcher.skip_count,
                drop_count=clean_summary.drop_count,
                elapsed_seconds=time.time() - start,
            )
        except EmptyDataError:
            return PipelineResult(
                pipeline_name="etf_nav", table_name=self._config.tables.etf_nav,
                success=True, record_count=0,
                elapsed_seconds=time.time() - start,
            )
        except Exception as exc:
            return PipelineResult(
                pipeline_name="etf_nav", table_name=self._config.tables.etf_nav,
                success=False, error_message=str(exc),
                elapsed_seconds=time.time() - start,
            )

    def _run_premium(self, etf_daily_df: pd.DataFrame, nav_df: pd.DataFrame) -> PipelineResult:
        """折溢价率计算管道。"""
        start = time.time()
        try:
            premium_df = self._transformer.compute_premium(etf_daily_df, nav_df)
            if premium_df.empty:
                return PipelineResult(
                    pipeline_name="etf_premium", table_name=self._config.tables.etf_premium,
                    success=True, record_count=0,
                    elapsed_seconds=time.time() - start,
                )

            write_result = self._writer.write(premium_df, self._config.tables.etf_premium)

            return PipelineResult(
                pipeline_name="etf_premium",
                table_name=self._config.tables.etf_premium,
                success=True,
                record_count=write_result.success_count,
                elapsed_seconds=time.time() - start,
            )
        except Exception as exc:
            return PipelineResult(
                pipeline_name="etf_premium", table_name=self._config.tables.etf_premium,
                success=False, error_message=str(exc),
                elapsed_seconds=time.time() - start,
            )

    def _run_metadata(self) -> PipelineResult:
        """ETF 元数据管道。"""
        start = time.time()
        try:
            raw_df = self._metadata_fetcher.fetch_metadata()
            if raw_df.empty:
                return PipelineResult(
                    pipeline_name="etf_metadata", table_name=self._config.tables.etf_metadata,
                    success=False, error_message="元数据为空",
                )

            write_result = self._writer.write_metadata(raw_df, self._config.tables.etf_metadata)

            return PipelineResult(
                pipeline_name="etf_metadata",
                table_name=self._config.tables.etf_metadata,
                success=True,
                record_count=write_result.success_count,
                elapsed_seconds=time.time() - start,
            )
        except Exception as exc:
            return PipelineResult(
                pipeline_name="etf_metadata", table_name=self._config.tables.etf_metadata,
                success=False, error_message=str(exc),
                elapsed_seconds=time.time() - start,
            )

    def _get_etf_raw_codes(self) -> list[str]:
        """获取 6 位数字 ETF 代码列表（用于净值查询）。"""
        try:
            return self._fetcher.fetch_raw_codes()
        except Exception:
            return []
