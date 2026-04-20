"""
数据写入模块。

DataWriter 负责将转换后的标准格式数据以 upsert 方式写入 DolphinDB，
连接失败时缓存到本地 Parquet 文件，启动时自动恢复缓存数据。
"""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

import dolphindb as ddb
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from etf_pipeline.config import PipelineConfig
from etf_pipeline.models import WriteResult

__all__ = ["DataWriter"]


class DataWriter:
    """将数据写入 DolphinDB 多表，支持 Parquet 缓存与自动恢复。"""

    _METADATA_TABLES = {"etf_metadata"}

    def __init__(self, config: PipelineConfig, error_logger: logging.Logger) -> None:
        self._config = config
        self._error_logger = error_logger
        self._session: ddb.Session | None = None

    def _get_db_path(self, table_name: str) -> str:
        if table_name in self._METADATA_TABLES:
            return self._config.db_path + "_meta"
        return self._config.db_path

    def write(self, df: pd.DataFrame, table_name: str, key_columns: list[str] | None = None) -> WriteResult:
        if key_columns is None:
            key_columns = ["date", "symbol"]

        if df.empty:
            return WriteResult(
                success_count=0, skip_count=0, drop_count=0,
                has_quality_warning=False, table_name=table_name,
            )

        trade_date = self._extract_trade_date(df)
        db_path = self._get_db_path(table_name)

        try:
            session = self._get_session()
            upserter = ddb.TableUpserter(
                db_path,
                table_name,
                session,
                key_cols=key_columns,
            )
            upserter.upsert(df)
            return WriteResult(
                success_count=len(df), skip_count=0, drop_count=0,
                has_quality_warning=False, table_name=table_name,
            )
        except Exception as exc:
            cache_path = self._cache_to_local(df, table_name, trade_date)
            self._error_logger.critical(
                "DolphinDB 写入失败，已缓存到本地：%s，表：%s，错误：%s",
                cache_path, table_name, exc,
            )
            return WriteResult(
                success_count=0, skip_count=0, drop_count=0,
                has_quality_warning=False, table_name=table_name,
            )

    def write_metadata(self, df: pd.DataFrame, table_name: str) -> WriteResult:
        return self.write(df, table_name, key_columns=["symbol"])

    def recover_from_cache(self) -> int:
        cache_dir = Path("cache")
        if not cache_dir.exists():
            return 0

        parquet_files = sorted(cache_dir.glob("*_cache.parquet"))
        if not parquet_files:
            return 0

        self._error_logger.info("发现 %d 个缓存文件，开始恢复...", len(parquet_files))
        total_recovered = 0

        for pf in parquet_files:
            try:
                df = pd.read_parquet(pf)
                if df.empty:
                    pf.unlink()
                    continue

                table_name = self._parse_table_name_from_cache(pf.name)
                key_columns = ["symbol"] if table_name == "etf_metadata" else ["date", "symbol"]
                db_path = self._get_db_path(table_name)

                session = self._get_session()
                upserter = ddb.TableUpserter(
                    db_path,
                    table_name,
                    session,
                    key_cols=key_columns,
                )
                upserter.upsert(df)
                total_recovered += len(df)
                pf.unlink()
                self._error_logger.info(
                    "缓存恢复成功：%s，%d 条记录", pf.name, len(df),
                )
            except Exception as exc:
                self._error_logger.error(
                    "缓存恢复失败：%s，错误：%s", pf.name, exc,
                )

        return total_recovered

    def _get_session(self) -> ddb.Session:
        """获取或重建 DolphinDB 连接。"""
        @retry(
            stop=stop_after_attempt(self._config.db_retry_times),
            wait=wait_fixed(self._config.db_retry_interval),
            reraise=True,
        )
        def _connect() -> ddb.Session:
            cfg = self._config.dolphindb
            session = ddb.Session()
            session.connect(cfg.host, cfg.port, cfg.username, cfg.password)
            return session

        self._session = _connect()
        return self._session

    def _cache_to_local(self, df: pd.DataFrame, table_name: str, trade_date: date) -> str:
        """将 DataFrame 序列化为 Parquet 文件缓存到本地。"""
        os.makedirs("cache", exist_ok=True)
        date_str = trade_date.strftime("%Y%m%d")
        path = f"cache/{date_str}_{table_name}_cache.parquet"
        df.to_parquet(path, index=False)
        return path

    @staticmethod
    def _extract_trade_date(df: pd.DataFrame) -> date:
        """从 DataFrame 的 date 列提取交易日期。"""
        if "date" not in df.columns or df.empty:
            return date.today()
        trade_date = df["date"].iloc[0]
        if hasattr(trade_date, "date"):
            return trade_date.date()
        if isinstance(trade_date, date):
            return trade_date
        return date.today()

    @staticmethod
    def _parse_table_name_from_cache(filename: str) -> str:
        """从缓存文件名解析表名。格式：{YYYYMMDD}_{table_name}_cache.parquet"""
        parts = filename.replace("_cache.parquet", "").split("_", 1)
        if len(parts) >= 2:
            return parts[1]
        return "etf_daily"
