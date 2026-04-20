"""
数据采集模块。

DataFetcher 通过腾讯财经 API 获取全市场 ETF 日线行情数据（增量模式），
IndexFetcher 通过腾讯财经 API 获取跟踪指数行情，
AdjustFactorFetcher 通过腾讯财经 API 获取复权因子（间接计算）。

优化策略：
1. 前收盘价缓存：从 DolphinDB 批量查询前一天收盘价，避免逐个 API 请求
2. ETF 列表缓存：内存缓存 + TTL，减少 DolphinDB/baostock 查询
3. 增量跳过：查询 DolphinDB 已有数据的 symbol，只处理缺失的
4. 失败重试：记录失败 symbol，支持只重试失败的
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, timedelta
from typing import ClassVar

import baostock as bs
import numpy as np
import pandas as pd
import requests

from etf_pipeline.config import PipelineConfig

__all__ = ["DataFetcher", "IndexFetcher", "AdjustFactorFetcher", "AmountFetcher"]

_TENCENT_FQ_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
_TENCENT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://gu.qq.com/",
}

_ETFLIST_CACHE_TTL = 3600


def _get_tencent_session() -> requests.Session:
    sess = requests.Session()
    sess.trust_env = False
    sess.proxies = {"http": None, "https": None}
    sess.headers.update(_TENCENT_HEADERS)
    return sess


def _code_to_tencent_symbol(code: str) -> str:
    if code.startswith("5") or code.startswith("6"):
        return f"sh{code}"
    return f"sz{code}"


def _raw_code_to_baostock(code: str) -> str:
    if code.startswith("5") or code.startswith("6"):
        return f"sh.{code}"
    return f"sz.{code}"


def _baostock_to_raw(code: str) -> str:
    return code.split(".")[1] if "." in code else code


def _get_prev_trading_day(d: date) -> date | None:
    import chinese_calendar
    cur = d
    for _ in range(10):
        cur = cur - timedelta(days=1)
        if chinese_calendar.is_workday(cur):
            return cur
    return None


class _EtfListCache:
    """ETF 列表内存缓存，带 TTL。"""

    _instance: ClassVar[_EtfListCache | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __new__(cls) -> _EtfListCache:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._symbols: list[str] = []
                cls._instance._raw_codes: list[str] = []
                cls._instance._timestamp: float = 0.0
            return cls._instance

    def get(self) -> tuple[list[str], list[str]] | None:
        if not self._symbols:
            return None
        if time.time() - self._timestamp > _ETFLIST_CACHE_TTL:
            return None
        return self._symbols, self._raw_codes

    def set(self, symbols: list[str], raw_codes: list[str]) -> None:
        self._symbols = symbols
        self._raw_codes = raw_codes
        self._timestamp = time.time()

    def invalidate(self) -> None:
        self._symbols = []
        self._raw_codes = []
        self._timestamp = 0.0


class _BaoStockSessionManager:
    """baostock 全局会话管理器（单例）。"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls) -> _BaoStockSessionManager:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._logged_in = False
            return cls._instance

    def ensure_login(self) -> None:
        if not self._logged_in:
            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"baostock 登录失败: {lg.error_msg}")
            self._logged_in = True


def _fetch_etf_list_from_dolphindb(config: PipelineConfig) -> tuple[list[str], list[str]] | None:
    try:
        import dolphindb as ddb
        cfg = config.dolphindb
        session = ddb.Session()
        session.connect(cfg.host, cfg.port, cfg.username, cfg.password)
        df = session.run(
            f'select distinct symbol from loadTable("{config.db_path}", "etf_daily")'
        )
        session.close()
        if df.empty:
            return None
        raw_codes = [s.split(".")[0] if "." in s else s for s in df["symbol"].tolist()]
        symbols = [_raw_code_to_baostock(c) for c in raw_codes]
        return symbols, raw_codes
    except Exception:
        return None


def _fetch_etf_list_from_baostock() -> list[str]:
    _BaoStockSessionManager().ensure_login()
    rs = bs.query_stock_basic(code_name="ETF")
    etf_codes: list[str] = []
    while rs.next():
        row = rs.get_row_data()
        code = row[0]
        if code.startswith("sh.51") or code.startswith("sz.15") or code.startswith("sz.16"):
            etf_codes.append(code)
    return etf_codes


def _fetch_prev_close_from_dolphindb(config: PipelineConfig, prev_date: date) -> dict[str, float]:
    try:
        import dolphindb as ddb
        cfg = config.dolphindb
        session = ddb.Session()
        session.connect(cfg.host, cfg.port, cfg.username, cfg.password)
        date_str = prev_date.strftime("%Y.%m.%d")
        df = session.run(
            f'select symbol, close from loadTable("{config.db_path}", "etf_daily") '
            f'where date = {date_str}'
        )
        session.close()
        if df.empty:
            return {}
        return {row["symbol"]: row["close"] for _, row in df.iterrows()}
    except Exception:
        return {}


def _fetch_existing_symbols_from_dolphindb(config: PipelineConfig, trade_date: date, table_name: str) -> set[str]:
    try:
        import dolphindb as ddb
        cfg = config.dolphindb
        session = ddb.Session()
        session.connect(cfg.host, cfg.port, cfg.username, cfg.password)
        date_str = trade_date.strftime("%Y.%m.%d")
        df = session.run(
            f'select distinct symbol from loadTable("{config.db_path}", "{table_name}") '
            f'where date = {date_str}'
        )
        session.close()
        if df.empty:
            return set()
        return set(df["symbol"].tolist())
    except Exception:
        return set()


class DataFetcher:
    """通过腾讯财经 API 采集 ETF 日线行情数据（增量模式，只拉当天）。

    优化：
    - 前收盘价从 DolphinDB 批量查询，避免逐个 API 请求
    - ETF 列表内存缓存，减少查询
    - 跳过 DolphinDB 中已有数据的 symbol
    - 记录失败 symbol，支持重试
    """

    def __init__(self, config: PipelineConfig, error_logger: logging.Logger) -> None:
        self._config = config
        self._error_logger = error_logger
        self.skip_count: int = 0
        self.failed_symbols: list[str] = []
        self._prev_close_cache: dict[str, float] = {}
        self._list_cache = _EtfListCache()

    def fetch_etf_list(self) -> list[str]:
        cached = self._list_cache.get()
        if cached is not None:
            return cached[0]

        result = _fetch_etf_list_from_dolphindb(self._config)
        if result is not None:
            symbols, raw_codes = result
            self._list_cache.set(symbols, raw_codes)
            return symbols

        symbols = _fetch_etf_list_from_baostock()
        raw_codes = [_baostock_to_raw(c) for c in symbols]
        self._list_cache.set(symbols, raw_codes)
        return symbols

    def fetch_raw_codes(self) -> list[str]:
        cached = self._list_cache.get()
        if cached is not None:
            return cached[1]
        self.fetch_etf_list()
        cached = self._list_cache.get()
        return cached[1] if cached is not None else []

    def _ensure_prev_close_cache(self, trade_date: date) -> None:
        if self._prev_close_cache:
            return
        prev_date = _get_prev_trading_day(trade_date)
        if prev_date is None:
            return
        self._prev_close_cache = _fetch_prev_close_from_dolphindb(self._config, prev_date)

    def fetch_single_etf(self, raw_code: str, trade_date: date) -> pd.DataFrame | None:
        date_str = trade_date.strftime("%Y-%m-%d")
        tencent_symbol = _code_to_tencent_symbol(raw_code)

        for attempt in range(3):
            try:
                sess = _get_tencent_session()
                param = f"{tencent_symbol},day,{date_str},{date_str},500,"
                r = sess.get(_TENCENT_FQ_URL, params={"param": param}, timeout=30)
                if r.status_code != 200:
                    continue
                data = r.json()
                raw_data = data.get("data")
                if not isinstance(raw_data, dict):
                    continue
                stock_data = raw_data.get(tencent_symbol, {})
                if not isinstance(stock_data, dict):
                    continue
                klines = stock_data.get("day", [])
                if not klines:
                    return None

                k = klines[0]
                close_price = float(k[2])
                df = pd.DataFrame([{
                    "日期": k[0],
                    "开盘": float(k[1]),
                    "收盘": close_price,
                    "最高": float(k[3]),
                    "最低": float(k[4]),
                    "成交量": int(float(k[5])),
                    "成交额": 0.0,
                    "是否ST": "",
                    "symbol": raw_code,
                }])

                symbol_key = f"{raw_code}.SH" if raw_code.startswith("5") or raw_code.startswith("6") else f"{raw_code}.SZ"
                prev_close = self._prev_close_cache.get(symbol_key)
                if prev_close is not None:
                    df["前收盘"] = prev_close
                    df["涨跌幅"] = round((close_price / prev_close - 1) * 100, 4)
                else:
                    prev_date = _get_prev_trading_day(trade_date)
                    if prev_date:
                        api_close = self._fetch_tencent_close(tencent_symbol, prev_date)
                        if api_close is not None:
                            df["前收盘"] = api_close
                            df["涨跌幅"] = round((close_price / api_close - 1) * 100, 4)
                        else:
                            df["前收盘"] = np.nan
                            df["涨跌幅"] = np.nan
                    else:
                        df["前收盘"] = np.nan
                        df["涨跌幅"] = np.nan

                return df
            except Exception as exc:
                if attempt < 2:
                    time.sleep(2)
                else:
                    self._error_logger.error(
                        "fetch_single_etf 失败: code=%s, date=%s, error=%s",
                        raw_code, date_str, exc,
                    )
                    return None
        return None

    def _fetch_tencent_close(self, tencent_symbol: str, prev_date: date) -> float | None:
        date_str = prev_date.strftime("%Y-%m-%d")
        try:
            sess = _get_tencent_session()
            param = f"{tencent_symbol},day,{date_str},{date_str},500,"
            r = sess.get(_TENCENT_FQ_URL, params={"param": param}, timeout=30)
            if r.status_code != 200:
                return None
            data = r.json()
            raw_data = data.get("data")
            if not isinstance(raw_data, dict):
                return None
            stock_data = raw_data.get(tencent_symbol, {})
            if not isinstance(stock_data, dict):
                return None
            klines = stock_data.get("day", [])
            if klines:
                return float(klines[0][2])
        except Exception:
            pass
        return None

    def fetch_all_etf(self, trade_date: date, retry_failed: bool = False) -> pd.DataFrame:
        self.skip_count = 0
        self.failed_symbols = []

        symbols = self.fetch_etf_list()
        if not symbols:
            return pd.DataFrame()

        self._ensure_prev_close_cache(trade_date)

        existing = _fetch_existing_symbols_from_dolphindb(
            self._config, trade_date, "etf_daily"
        )

        raw_codes = [_baostock_to_raw(s) for s in symbols]

        if retry_failed:
            to_process = [c for c in raw_codes]
        else:
            to_process = []
            for code in raw_codes:
                symbol_key = f"{code}.SH" if code.startswith("5") or code.startswith("6") else f"{code}.SZ"
                if symbol_key not in existing:
                    to_process.append(code)

        skipped_by_existing = len(raw_codes) - len(to_process)
        if skipped_by_existing > 0:
            print(f"  [ETF行情] 跳过已有 {skipped_by_existing} 只，待处理 {len(to_process)} 只")

        if not to_process:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        total = len(to_process)
        for i, raw_code in enumerate(to_process):
            if (i + 1) % 50 == 0 or i == total - 1:
                print(f"\r  [ETF行情] {i+1}/{total} ({(i+1)/total*100:.0f}%)...", end="", flush=True)
            df = self.fetch_single_etf(raw_code, trade_date)
            if df is None:
                self.skip_count += 1
                self.failed_symbols.append(raw_code)
                continue
            frames.append(df)
            time.sleep(0.1)

        print()
        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def retry_failed(self, trade_date: date) -> pd.DataFrame:
        if not self.failed_symbols:
            return pd.DataFrame()
        print(f"  [ETF行情] 重试 {len(self.failed_symbols)} 只失败的 ETF...")
        retry_codes = list(self.failed_symbols)
        self.failed_symbols = []
        self.skip_count = 0

        frames: list[pd.DataFrame] = []
        for i, raw_code in enumerate(retry_codes):
            print(f"\r  [ETF行情重试] {i+1}/{len(retry_codes)}...", end="", flush=True)
            df = self.fetch_single_etf(raw_code, trade_date)
            if df is None:
                self.skip_count += 1
                self.failed_symbols.append(raw_code)
                continue
            frames.append(df)
            time.sleep(0.1)

        print()
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


class AmountFetcher:
    """通过通达信(pytdx)获取 ETF 成交额数据。

    腾讯财经 API 不提供成交额，通达信行情接口支持成交额字段。
    pytdx 使用 TCP 协议直连通达信服务器，速度快(25ms/次)、无 IP 限制、
    数据与腾讯实时行情完全一致(交叉验证比值 1.0000)。
    """

    _TDX_SERVERS = [
        ("218.75.126.9", 7709),
        ("115.238.56.198", 7709),
        ("119.147.212.81", 7709),
        ("221.231.141.60", 7709),
    ]

    def __init__(self, config: PipelineConfig, error_logger: logging.Logger) -> None:
        self._config = config
        self._error_logger = error_logger
        self._api = None
        self._connected = False

    @staticmethod
    def _raw_code_to_market(raw_code: str) -> int:
        if raw_code.startswith("5") or raw_code.startswith("6"):
            return 1
        return 0

    def _ensure_connection(self) -> bool:
        if self._connected and self._api is not None:
            return True
        try:
            from pytdx.hq import TdxHq_API
            self._api = TdxHq_API()
            for ip, port in self._TDX_SERVERS:
                try:
                    if self._api.connect(ip, port):
                        self._connected = True
                        return True
                except Exception:
                    continue
            self._error_logger.warning("AmountFetcher: 无法连接通达信服务器")
            return False
        except ImportError:
            self._error_logger.error("AmountFetcher: pytdx 未安装")
            return False

    def disconnect(self) -> None:
        if self._connected and self._api is not None:
            try:
                self._api.disconnect()
            except Exception:
                pass
            self._connected = False
            self._api = None

    def fetch_amount_by_date(self, raw_codes: list[str], trade_date: date) -> pd.DataFrame:
        """从通达信获取指定日期 ETF 的成交额。

        Returns:
            DataFrame，列：symbol, amount
            amount 单位为元，与 DolphinDB etf_daily 表一致。
        """
        if not self._ensure_connection():
            return pd.DataFrame(columns=["symbol", "amount"])

        all_rows: list[dict] = []
        total = len(raw_codes)
        target_str = trade_date.strftime("%Y-%m-%d")

        for i, raw_code in enumerate(raw_codes):
            market = self._raw_code_to_market(raw_code)
            try:
                df = self._api.to_df(
                    self._api.get_security_bars(9, market, raw_code, 0, 5)
                )
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        dt_str = str(row["datetime"])[:10]
                        if dt_str == target_str:
                            symbol = (
                                f"{raw_code}.SH"
                                if raw_code.startswith("5") or raw_code.startswith("6")
                                else f"{raw_code}.SZ"
                            )
                            all_rows.append({"symbol": symbol, "amount": float(row["amount"])})
                            break
            except Exception as exc:
                self._error_logger.warning(
                    "AmountFetcher 查询失败: code=%s, error=%s", raw_code, exc
                )
                self._connected = False
                self._ensure_connection()

            if (i + 1) % 200 == 0 or i == total - 1:
                print(f"\r  [成交额] {min(i+1, total)}/{total}...", end="", flush=True)

        print()

        if not all_rows:
            return pd.DataFrame(columns=["symbol", "amount"])

        return pd.DataFrame(all_rows)

    def fetch_amount_history(self, raw_code: str, max_bars: int = 10000) -> pd.DataFrame | None:
        """从通达信获取 ETF 全量历史成交额（用于回填）。

        Returns:
            DataFrame，列：date, symbol, amount
            date 为 datetime.date，amount 单位为元。
        """
        if not self._ensure_connection():
            return None

        market = self._raw_code_to_market(raw_code)
        all_dfs: list[pd.DataFrame] = []

        for start_pos in range(0, max_bars, 800):
            try:
                batch = self._api.to_df(
                    self._api.get_security_bars(9, market, raw_code, start_pos, 800)
                )
                if batch is None or batch.empty:
                    break
                all_dfs.append(batch)
                if len(batch) < 800:
                    break
            except Exception as exc:
                self._error_logger.warning(
                    "AmountFetcher 历史查询失败: code=%s, pos=%d, error=%s",
                    raw_code, start_pos, exc,
                )
                self._connected = False
                if not self._ensure_connection():
                    break

        if not all_dfs:
            return None

        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df = full_df.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)

        symbol = (
            f"{raw_code}.SH"
            if raw_code.startswith("5") or raw_code.startswith("6")
            else f"{raw_code}.SZ"
        )
        full_df["date"] = pd.to_datetime(full_df["datetime"]).dt.date
        full_df["symbol"] = symbol
        return full_df[["date", "symbol", "amount"]]


class IndexFetcher:
    """通过腾讯财经 API 采集跟踪指数行情数据（增量模式）。"""

    def __init__(self, config: PipelineConfig, error_logger: logging.Logger) -> None:
        self._config = config
        self._error_logger = error_logger
        self.skip_count: int = 0
        self.failed_symbols: list[str] = []

    def fetch_single_index(self, symbol: str, trade_date: date) -> pd.DataFrame | None:
        date_str = trade_date.strftime("%Y-%m-%d")
        tencent_symbol = symbol.replace(".", "")

        for attempt in range(3):
            try:
                sess = _get_tencent_session()
                param = f"{tencent_symbol},day,{date_str},{date_str},500,"
                r = sess.get(_TENCENT_FQ_URL, params={"param": param}, timeout=30)
                if r.status_code != 200:
                    continue
                data = r.json()
                raw_data = data.get("data")
                if not isinstance(raw_data, dict):
                    continue
                stock_data = raw_data.get(tencent_symbol, {})
                if not isinstance(stock_data, dict):
                    continue
                klines = stock_data.get("day", [])
                if not klines:
                    return None

                k = klines[0]
                raw_code = symbol.split(".")[1]
                df = pd.DataFrame([{
                    "open": float(k[1]),
                    "close": float(k[2]),
                    "high": float(k[3]),
                    "low": float(k[4]),
                    "volume": int(float(k[5])),
                    "amount": 0.0,
                    "symbol": raw_code,
                }])
                return df
            except Exception as exc:
                if attempt < 2:
                    time.sleep(2)
                else:
                    self._error_logger.error(
                        "fetch_single_index 失败: symbol=%s, date=%s, error=%s",
                        symbol, date_str, exc,
                    )
                    return None
        self.failed_symbols.append(symbol)
        return None

    def fetch_all_indices(self, trade_date: date) -> pd.DataFrame:
        self.skip_count = 0
        self.failed_symbols = []

        existing = _fetch_existing_symbols_from_dolphindb(
            self._config, trade_date, "index_daily"
        )

        to_process = []
        for symbol in self._config.tracking_indices:
            raw_code = symbol.split(".")[1]
            if raw_code not in existing:
                to_process.append(symbol)

        skipped = len(self._config.tracking_indices) - len(to_process)
        if skipped > 0:
            print(f"  [指数行情] 跳过已有 {skipped} 只，待处理 {len(to_process)} 只")

        if not to_process:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        total = len(to_process)
        for i, symbol in enumerate(to_process):
            print(f"\r  [指数行情] {i+1}/{total} {symbol}...", end="", flush=True)
            df = self.fetch_single_index(symbol, trade_date)
            if df is None:
                self.skip_count += 1
                continue
            frames.append(df)
            time.sleep(0.1)

        print()
        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def retry_failed(self, trade_date: date) -> pd.DataFrame:
        if not self.failed_symbols:
            return pd.DataFrame()
        print(f"  [指数行情] 重试 {len(self.failed_symbols)} 只失败的指数...")
        retry_list = list(self.failed_symbols)
        self.failed_symbols = []
        self.skip_count = 0

        frames: list[pd.DataFrame] = []
        for i, symbol in enumerate(retry_list):
            print(f"\r  [指数行情重试] {i+1}/{len(retry_list)}...", end="", flush=True)
            df = self.fetch_single_index(symbol, trade_date)
            if df is None:
                self.skip_count += 1
                self.failed_symbols.append(symbol)
                continue
            frames.append(df)
            time.sleep(0.1)

        print()
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


class AdjustFactorFetcher:
    """通过腾讯财经 API 采集 ETF 复权因子（增量模式，间接计算）。

    优化：
    - 复用 DataFetcher 的 ETF 列表缓存
    - 跳过 DolphinDB 中已有数据的 symbol
    - 记录失败 symbol，支持重试
    """

    def __init__(self, config: PipelineConfig, error_logger: logging.Logger) -> None:
        self._config = config
        self._error_logger = error_logger
        self.skip_count: int = 0
        self.failed_symbols: list[str] = []
        self._list_cache = _EtfListCache()

    def fetch_single_factor(self, raw_code: str, trade_date: date) -> pd.DataFrame | None:
        date_str = trade_date.strftime("%Y-%m-%d")
        tencent_symbol = _code_to_tencent_symbol(raw_code)

        for attempt in range(3):
            try:
                sess = _get_tencent_session()

                raw_close = None
                qfq_close = None
                hfq_close = None

                param_raw = f"{tencent_symbol},day,{date_str},{date_str},500,"
                r = sess.get(_TENCENT_FQ_URL, params={"param": param_raw}, timeout=30)
                if r.status_code == 200:
                    data = r.json().get("data")
                    if isinstance(data, dict):
                        sd = data.get(tencent_symbol, {})
                        if isinstance(sd, dict):
                            klines = sd.get("day", [])
                            if klines:
                                raw_close = float(klines[0][2])

                param_qfq = f"{tencent_symbol},day,{date_str},{date_str},500,qfq"
                r = sess.get(_TENCENT_FQ_URL, params={"param": param_qfq}, timeout=30)
                if r.status_code == 200:
                    data = r.json().get("data")
                    if isinstance(data, dict):
                        sd = data.get(tencent_symbol, {})
                        if isinstance(sd, dict):
                            klines = sd.get("qfqday", sd.get("day", []))
                            if klines:
                                qfq_close = float(klines[0][2])

                param_hfq = f"{tencent_symbol},day,{date_str},{date_str},500,hfq"
                r = sess.get(_TENCENT_FQ_URL, params={"param": param_hfq}, timeout=30)
                if r.status_code == 200:
                    data = r.json().get("data")
                    if isinstance(data, dict):
                        sd = data.get(tencent_symbol, {})
                        if isinstance(sd, dict):
                            klines = sd.get("hfqday", sd.get("day", []))
                            if klines:
                                hfq_close = float(klines[0][2])

                if raw_close is None or raw_close == 0:
                    return None

                fore_adjust = round(qfq_close / raw_close, 6) if qfq_close is not None else 1.0
                back_adjust = round(hfq_close / raw_close, 6) if hfq_close is not None else 1.0

                df = pd.DataFrame([{
                    "date": date_str,
                    "symbol": raw_code,
                    "fore_adjust": fore_adjust,
                    "back_adjust": back_adjust,
                }])
                return df
            except Exception as exc:
                if attempt < 2:
                    time.sleep(2)
                else:
                    self._error_logger.error(
                        "fetch_single_factor 失败: code=%s, date=%s, error=%s",
                        raw_code, date_str, exc,
                    )
                    return None
        self.failed_symbols.append(raw_code)
        return None

    def fetch_all_factors(self, trade_date: date) -> pd.DataFrame:
        self.skip_count = 0
        self.failed_symbols = []

        cached = self._list_cache.get()
        if cached is not None:
            _, raw_codes = cached
        else:
            result = _fetch_etf_list_from_dolphindb(self._config)
            if result is not None:
                _, raw_codes = result
                self._list_cache.set(result[0], raw_codes)
            else:
                symbols = _fetch_etf_list_from_baostock()
                raw_codes = [_baostock_to_raw(c) for c in symbols]
                self._list_cache.set(symbols, raw_codes)

        if not raw_codes:
            return pd.DataFrame()

        existing = _fetch_existing_symbols_from_dolphindb(
            self._config, trade_date, "adjust_factor"
        )

        to_process = []
        for code in raw_codes:
            symbol_key = f"{code}.SH" if code.startswith("5") or code.startswith("6") else f"{code}.SZ"
            if symbol_key not in existing:
                to_process.append(code)

        skipped = len(raw_codes) - len(to_process)
        if skipped > 0:
            print(f"  [复权因子] 跳过已有 {skipped} 只，待处理 {len(to_process)} 只")

        if not to_process:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        total = len(to_process)
        for i, raw_code in enumerate(to_process):
            if (i + 1) % 50 == 0 or i == total - 1:
                print(f"\r  [复权因子] {i+1}/{total} ({(i+1)/total*100:.0f}%)...", end="", flush=True)
            df = self.fetch_single_factor(raw_code, trade_date)
            if df is None:
                self.skip_count += 1
                continue
            frames.append(df)
            time.sleep(0.1)

        print()
        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def retry_failed(self, trade_date: date) -> pd.DataFrame:
        if not self.failed_symbols:
            return pd.DataFrame()
        print(f"  [复权因子] 重试 {len(self.failed_symbols)} 只失败的 ETF...")
        retry_codes = list(self.failed_symbols)
        self.failed_symbols = []
        self.skip_count = 0

        frames: list[pd.DataFrame] = []
        for i, raw_code in enumerate(retry_codes):
            print(f"\r  [复权因子重试] {i+1}/{len(retry_codes)}...", end="", flush=True)
            df = self.fetch_single_factor(raw_code, trade_date)
            if df is None:
                self.skip_count += 1
                self.failed_symbols.append(raw_code)
                continue
            frames.append(df)
            time.sleep(0.1)

        print()
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
