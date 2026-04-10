"""
任务编排模块。

TaskRunner 负责按顺序编排各数据管道模块，记录运行日志与汇总统计。
"""

from __future__ import annotations

import logging
import time
from datetime import date

from etf_pipeline.cleaner import DataCleaner, EmptyDataError
from etf_pipeline.config import PipelineConfig
from etf_pipeline.db_initializer import DBInitializer
from etf_pipeline.fetcher import DataFetcher
from etf_pipeline.logger import get_error_logger, get_run_logger
from etf_pipeline.transformer import DataTransformer
from etf_pipeline.writer import DataWriter

__all__ = ["TaskRunner"]


class TaskRunner:
    """协调各模块完成一次完整的 ETF 数据更新任务。"""

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
        self._cleaner = DataCleaner(self._error_logger)
        self._transformer = DataTransformer()
        # 通过 writer 获取 session 初始化 DBInitializer
        session = self._writer._get_session()
        self._db_initializer = DBInitializer(session, config)

    def run(self, trade_date: date) -> None:
        """完整任务编排。

        步骤：
        1. 记录任务开始日志（含时间戳）
        2. DBInitializer.ensure_initialized()
        3. DataFetcher.fetch_all_etf(trade_date)
        4. DataCleaner.clean(raw_df)
        5. DataTransformer.transform(clean_df, trade_date)
        6. DataWriter.write(std_df)
        7. 记录汇总日志（成功数、跳过数、丢弃数）
        8. 若成功率 < 90%，标记"数据质量警告"

        Args:
            trade_date: 当日交易日期。
        """
        start_time = time.time()
        self._run_logger.info(
            "任务开始：trade_date=%s, timestamp=%s",
            trade_date,
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)),
        )

        try:
            # 2. 初始化数据库（幂等）
            self._db_initializer.ensure_initialized()

            # 3. 采集全市场 ETF 数据
            raw_df = self._fetcher.fetch_all_etf(trade_date)

            # 4. 数据清洗
            clean_df, clean_summary = self._cleaner.clean(raw_df)

            # 5. 格式转换
            std_df = self._transformer.transform(clean_df, trade_date)

            # 6. 写入 DolphinDB
            write_result = self._writer.write(std_df)

        except EmptyDataError as exc:
            elapsed = time.time() - start_time
            self._error_logger.critical(
                "任务终止（EmptyDataError）：%s，耗时=%.2fs", exc, elapsed
            )
            self._run_logger.critical(
                "任务终止（EmptyDataError）：%s，耗时=%.2fs", exc, elapsed
            )
            return

        elapsed = time.time() - start_time
        success_count = write_result.success_count
        skip_count = self._fetcher.skip_count
        drop_count = clean_summary.drop_count
        total = success_count + skip_count + drop_count

        # 7. 记录汇总日志
        self._run_logger.info(
            "任务完成：成功写入=%d，跳过=%d，丢弃=%d，耗时=%.2fs",
            success_count,
            skip_count,
            drop_count,
            elapsed,
        )

        # 8. 成功率检查
        if total > 0:
            success_rate = success_count / total
            if success_rate < 0.9:
                self._run_logger.warning(
                    "数据质量警告：成功率=%.2f%%（成功=%d，跳过=%d，丢弃=%d）",
                    success_rate * 100,
                    success_count,
                    skip_count,
                    drop_count,
                )
