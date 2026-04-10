"""
数据写入模块。

DataWriter 负责将转换后的标准格式数据以 upsert 方式写入 DolphinDB，
连接失败时缓存到本地 Parquet 文件。
"""

from __future__ import annotations

import logging
import os
from datetime import date

import dolphindb as ddb
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from etf_pipeline.config import PipelineConfig
from etf_pipeline.models import WriteResult

__all__ = ["DataWriter"]


class DataWriter:
    """将 ETF 数据写入 DolphinDB etf_daily 表。"""

    def __init__(self, config: PipelineConfig, error_logger: logging.Logger) -> None:
        self._config = config
        self._error_logger = error_logger
        self._session: ddb.Session | None = None

    def write(self, df: pd.DataFrame) -> WriteResult:
        """使用 TableUpserter 将 DataFrame upsert 到 etf_daily 表。

        连接失败时重试最多 db_retry_times 次，重试耗尽后缓存到本地 Parquet 文件。

        Args:
            df: 标准格式 DataFrame，列：date, symbol, open, high, low, close, volume, amount

        Returns:
            WriteResult 写入结果汇总。
        """
        # 从 date 列获取交易日期（用于缓存文件命名）
        trade_date: date = df["date"].iloc[0]
        if hasattr(trade_date, "date"):
            trade_date = trade_date.date()

        try:
            session = self._get_session()
            upserter = ddb.TableUpserter(
                self._config.db_path,
                self._config.table_name,
                session,
                keyColNames=["date", "symbol"],
            )
            upserter.upsert(df)
            return WriteResult(
                success_count=len(df),
                skip_count=0,
                drop_count=0,
                has_quality_warning=False,
            )
        except Exception as exc:
            cache_path = self._cache_to_local(df, trade_date)
            self._error_logger.critical(
                "DolphinDB 写入失败，已缓存到本地：%s，错误：%s",
                cache_path,
                exc,
            )
            return WriteResult(
                success_count=0,
                skip_count=0,
                drop_count=0,
                has_quality_warning=False,
            )

    def _get_session(self) -> ddb.Session:
        """获取或重建 DolphinDB 连接，含 tenacity 重试逻辑。

        Raises:
            Exception: 重试耗尽后仍无法连接时抛出。
        """
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

    def _cache_to_local(self, df: pd.DataFrame, trade_date: date) -> str:
        """将 DataFrame 序列化为 Parquet 文件缓存到本地。

        Args:
            df: 待缓存的 DataFrame。
            trade_date: 交易日期，用于文件命名。

        Returns:
            缓存文件路径，格式：cache/{YYYYMMDD}_etf_daily_cache.parquet
        """
        os.makedirs("cache", exist_ok=True)
        date_str = trade_date.strftime("%Y%m%d")
        path = f"cache/{date_str}_etf_daily_cache.parquet"
        df.to_parquet(path, index=False)
        return path
