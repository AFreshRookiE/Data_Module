"""
数据采集模块。

DataFetcher 负责通过 baostock 接口获取全市场 ETF 列表及逐只 OHLCV 数据，
内置 tenacity 重试逻辑。
"""

from __future__ import annotations

import logging
from datetime import date

import baostock as bs
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from etf_pipeline.config import PipelineConfig

__all__ = ["DataFetcher"]

_BAOSTOCK_COL_MAP = {
    "date": "日期",
    "open": "开盘",
    "high": "最高",
    "low": "最低",
    "close": "收盘",
    "volume": "成交量",
    "amount": "成交额",
}

_BAOSTOCK_FIELDS = "date,open,high,low,close,volume,amount"


class DataFetcher:
    """通过 baostock 采集 ETF 行情数据。"""

    def __init__(self, config: PipelineConfig, error_logger: logging.Logger) -> None:
        self._config = config
        self._error_logger = error_logger
        self.skip_count: int = 0
        self._logged_in = False

    def _ensure_login(self) -> None:
        if not self._logged_in:
            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"baostock 登录失败: {lg.error_msg}")
            self._logged_in = True

    def _logout(self) -> None:
        if self._logged_in:
            bs.logout()
            self._logged_in = False

    def fetch_etf_list(self) -> list[str]:
        """通过 baostock 获取全市场 ETF 代码列表。

        Returns:
            baostock 格式代码列表，如 ["sh.510300", "sz.159915", ...]
        """
        self._ensure_login()
        rs = bs.query_stock_basic(code_name="ETF")
        etf_codes: list[str] = []
        while rs.next():
            row = rs.get_row_data()
            code = row[0]
            if code.startswith("sh.51") or code.startswith("sz.15") or code.startswith("sz.16"):
                etf_codes.append(code)
        return etf_codes

    def fetch_single_etf(self, symbol: str, trade_date: date) -> pd.DataFrame | None:
        """通过 baostock 获取单只 ETF 当日数据。

        内置 tenacity 重试：最多 fetch_retry_times 次，间隔 fetch_retry_interval 秒。
        重试耗尽后记录错误日志，返回 None。

        Args:
            symbol: baostock 格式代码，如 "sh.510300"
            trade_date: 交易日期

        Returns:
            包含当日 OHLCV 数据的 DataFrame（中文列名，兼容下游 cleaner/transformer），
            失败时返回 None。
        """
        date_str = trade_date.strftime("%Y-%m-%d")

        @retry(
            stop=stop_after_attempt(self._config.fetch_retry_times),
            wait=wait_fixed(self._config.fetch_retry_interval),
            reraise=True,
        )
        def _fetch() -> pd.DataFrame:
            rs = bs.query_history_k_data_plus(
                symbol,
                _BAOSTOCK_FIELDS,
                start_date=date_str,
                end_date=date_str,
                frequency="d",
            )
            if rs.error_code != "0":
                raise RuntimeError(f"baostock 查询失败: {rs.error_msg}")

            rows = []
            while rs.next():
                rows.append(rs.get_row_data())

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows, columns=rs.fields)
            for col in ("open", "high", "low", "close", "volume", "amount"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            return df

        try:
            result = _fetch()
            if result.empty:
                return None
            result = result.rename(columns=_BAOSTOCK_COL_MAP)
            raw_code = symbol.split(".")[1]
            result["symbol"] = raw_code
            return result
        except Exception as exc:
            self._error_logger.error(
                "fetch_single_etf 失败: symbol=%s, date=%s, error=%s",
                symbol,
                date_str,
                exc,
            )
            return None

    def fetch_all_etf(self, trade_date: date) -> pd.DataFrame:
        """遍历全市场 ETF 列表，采集当日数据并合并为单个 DataFrame。

        跳过返回 None 的标的，累计到 self.skip_count。

        Args:
            trade_date: 交易日期

        Returns:
            合并后的 DataFrame，包含所有成功采集的 ETF 数据。
            列表为空或全部失败时返回空 DataFrame。
        """
        self.skip_count = 0
        self._ensure_login()

        try:
            symbols = self.fetch_etf_list()

            if not symbols:
                return pd.DataFrame()

            frames: list[pd.DataFrame] = []
            for symbol in symbols:
                df = self.fetch_single_etf(symbol, trade_date)
                if df is None:
                    self.skip_count += 1
                    continue
                frames.append(df)

            if not frames:
                return pd.DataFrame()

            return pd.concat(frames, ignore_index=True)
        finally:
            self._logout()
