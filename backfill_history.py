"""
ETF 数据仓全量历史回填脚本。

按顺序回填全部 6 张表的历史数据：
  1. etf_daily    — ETF 日线行情（腾讯财经 API，baostock 仅3个月）
  2. index_daily  — 跟踪指数日线行情（腾讯财经 API）
  3. adjust_factor — 复权因子（腾讯财经 API 前复权/后复权间接计算）
  4. etf_nav      — ETF 净值（东方财富 API）
  5. etf_premium  — 折溢价率（由 etf_daily + etf_nav 计算）
  6. etf_metadata — ETF 元数据（baostock + YAML 分类）

特性：
- 每张表独立进度文件，支持断点续传
- 分批写入 DolphinDB
- 实时进度显示（支持 GUI 回调）
- baostock 编码错误自动重试 + 重新登录
- 自适应限速：出错时自动降速，恢复后自动加速
- 连续错误冷却：连续出错时主动暂停，让服务器恢复
- 指数退避重试：重试延迟逐步增加
- 支持取消：通过 _cancel_check 回调检查取消标志
"""

from __future__ import annotations

import os

import json
import re
import time
from datetime import date
from pathlib import Path
from typing import Callable

import dolphindb as ddb
import numpy as np
import pandas as pd
import requests

from etf_pipeline.cleaner import DataCleaner
from etf_pipeline.config import load_config
from etf_pipeline.db_initializer import DBInitializer
from etf_pipeline.fetcher import _BaoStockSessionManager
from etf_pipeline.fetcher_metadata import MetadataFetcher
from etf_pipeline.logger import get_error_logger, get_run_logger
from etf_pipeline.transformer import DataTransformer

START_DATE = "2016-01-01"
END_DATE = date.today().strftime("%Y-%m-%d")
BATCH_SIZE = 50
PROGRESS_DIR = "backfill_progress"

REQUEST_INTERVAL = 0.5
MAX_RETRIES = 5
RETRY_DELAYS = [3, 6, 12, 30, 60]
RELOGIN_DELAY = 5
CONSECUTIVE_ERROR_THRESHOLD = 3
COOLDOWN_SECONDS = 30
ADAPTIVE_INTERVAL_MAX = 3.0

_TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
_EM_NAV_URL = "https://fund.eastmoney.com/f10/F10DataApi.aspx"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

_bs_mgr = _BaoStockSessionManager()

_progress_hook: Callable[[str], None] | None = None
_cancel_check: Callable[[], bool] | None = None


def _output(msg: str) -> None:
    try:
        print(msg, flush=True)
    except Exception:
        pass
    if _progress_hook is not None:
        try:
            _progress_hook(msg)
        except Exception:
            pass


def _should_cancel() -> bool:
    if _cancel_check is not None:
        try:
            return _cancel_check()
        except Exception:
            pass
    return False


def _code_to_baostock(code: str) -> str:
    if code.startswith("5") or code.startswith("6"):
        return f"sh.{code}"
    return f"sz.{code}"


def _code_to_tencent_symbol(code: str) -> str:
    if code.startswith("5") or code.startswith("6"):
        return f"sh{code}"
    return f"sz{code}"


def _is_baostock_garbled_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in [
        "utf-8", "codec", "decode", "decompress", "invalid",
        "index out of range", "接收数据异常",
        "10060", "10054", "timeout", "timed out",
        "winerror", "connectionreset", "brokenpipe",
        "connectionaborted", "networkerror",
    ])


def _relogin_baostock() -> None:
    import baostock as bs
    try:
        bs.logout()
    except Exception:
        pass
    _bs_mgr._logged_in = False
    time.sleep(RELOGIN_DELAY)
    _bs_mgr.ensure_login()


def get_etf_list() -> list[str]:
    import baostock as bs
    for attempt in range(MAX_RETRIES):
        try:
            _bs_mgr.ensure_login()
            rs = bs.query_stock_basic(code_name="ETF")
            etf_codes: list[str] = []
            while rs.next():
                row = rs.get_row_data()
                code = row[0]
                raw = code.split(".")[1] if "." in code else code
                if raw.startswith("51") or raw.startswith("15") or raw.startswith("16"):
                    etf_codes.append(raw)
            return etf_codes
        except Exception as exc:
            if _is_baostock_garbled_error(exc) and attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                _output(f"  [重试] ETF列表获取失败({attempt+1}/{MAX_RETRIES})，{delay}s 后重新登录...")
                _relogin_baostock()
                time.sleep(delay)
            else:
                raise


def load_progress(table_name: str) -> set[str]:
    path = os.path.join(PROGRESS_DIR, f"{table_name}.txt")
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def save_progress(table_name: str, items: list[str]) -> None:
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    path = os.path.join(PROGRESS_DIR, f"{table_name}.txt")
    with open(path, "a", encoding="utf-8") as f:
        for s in items:
            f.write(s + "\n")


def write_to_dolphindb(session: ddb.Session, db_path: str, table_name: str,
                       df: pd.DataFrame, key_cols: list[str] | None = None) -> int:
    if df.empty:
        return 0
    if key_cols is None:
        key_cols = ["date", "symbol"]
    upserter = ddb.TableUpserter(db_path, table_name, session, key_cols=key_cols)
    upserter.upsert(df)
    return len(df)


def fetch_etf_daily_baostock(code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    import baostock as bs
    symbol = _code_to_baostock(code)
    fields = "date,open,high,low,close,volume,amount,preclose,pctChg,isST"

    for attempt in range(MAX_RETRIES):
        try:
            _bs_mgr.ensure_login()
            rs = bs.query_history_k_data_plus(
                symbol, fields,
                start_date=start_date, end_date=end_date,
                frequency="d",
            )
            if rs.error_code != "0":
                return None
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=rs.fields)
            for col in ("open", "high", "low", "close", "volume", "amount", "preclose", "pctChg"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            rename_map = {
                "date": "日期", "open": "开盘", "high": "最高", "low": "最低",
                "close": "收盘", "volume": "成交量", "amount": "成交额",
                "preclose": "前收盘", "pctChg": "涨跌幅", "isST": "是否ST",
            }
            df = df.rename(columns=rename_map)
            df["symbol"] = code
            return df
        except Exception as exc:
            if _is_baostock_garbled_error(exc) and attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                _output(f"  [重试] {code} 编码异常({attempt+1}/{MAX_RETRIES})，重新登录后 {delay}s 再试...")
                _relogin_baostock()
                time.sleep(delay)
            else:
                return None
    return None


def fetch_index_daily_baostock(symbol: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    import baostock as bs
    fields = "date,open,high,low,close,volume,amount"

    for attempt in range(MAX_RETRIES):
        try:
            _bs_mgr.ensure_login()
            rs = bs.query_history_k_data_plus(
                symbol, fields,
                start_date=start_date, end_date=end_date,
                frequency="d",
            )
            if rs.error_code != "0":
                return None
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=rs.fields)
            for col in ("open", "high", "low", "close", "volume", "amount"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            raw = symbol.split(".")[1]
            df["symbol"] = raw
            return df
        except Exception as exc:
            if _is_baostock_garbled_error(exc) and attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                _output(f"  [重试] {symbol} 编码异常({attempt+1}/{MAX_RETRIES})，重新登录后 {delay}s 再试...")
                _relogin_baostock()
                time.sleep(delay)
            else:
                return None
    return None


def fetch_adjust_factor_baostock(symbol: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    import baostock as bs

    for attempt in range(MAX_RETRIES):
        try:
            _bs_mgr.ensure_login()
            rs = bs.query_adjust_factor(symbol, start_date=start_date, end_date=end_date)
            if rs.error_code != "0":
                return None
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            if not rows:
                return None
            df = pd.DataFrame(rows, columns=rs.fields)
            raw = symbol.split(".")[1]
            df["symbol"] = raw
            return df
        except Exception as exc:
            if _is_baostock_garbled_error(exc) and attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                _output(f"  [重试] {symbol} 复权因子编码异常({attempt+1}/{MAX_RETRIES})，重新登录后 {delay}s 再试...")
                _relogin_baostock()
                time.sleep(delay)
            else:
                return None
    return None


def _fetch_nav_range(code: str, sdate: str, edate: str) -> list[list[str]]:
    rows: list[list[str]] = []
    page = 1
    total_pages = 1
    pattern = re.compile(
        r"<tr>\s*"
        r"<td>(\d{4}-\d{2}-\d{2})</td>\s*"
        r"<td[^>]*>([\d.]+)</td>\s*"
        r"<td[^>]*>([\d.]+)</td>"
    )
    pages_pattern = re.compile(r"pages:(\d+)")

    while page <= total_pages:
        params = {
            "type": "lsjz",
            "code": code,
            "page": page,
            "sdate": sdate,
            "edate": edate,
            "per": 40,
        }
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(_EM_NAV_URL, params=params, headers=_HEADERS, timeout=30)
                if r.status_code != 200:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)])
                        continue
                    return rows
                html = r.text
                for match in pattern.finditer(html):
                    rows.append([match.group(1), match.group(2), match.group(3)])
                pages_match = pages_pattern.search(html)
                if pages_match:
                    total_pages = int(pages_match.group(1))
                break
            except Exception:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)])
                else:
                    return rows
        page += 1
    return rows


def fetch_nav_history(code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    from datetime import datetime
    start_year = datetime.strptime(start_date, "%Y-%m-%d").year
    end_year = datetime.strptime(end_date, "%Y-%m-%d").year
    all_rows: list[list[str]] = []

    for yr in range(start_year, end_year + 1):
        sdate = f"{yr}-01-01" if yr > start_year else start_date
        edate = f"{yr}-12-31" if yr < end_year else end_date
        rows = _fetch_nav_range(code, sdate, edate)
        all_rows.extend(rows)

    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows, columns=["日期", "单位净值", "累计净值"])
    df["单位净值"] = pd.to_numeric(df["单位净值"], errors="coerce")
    df["累计净值"] = pd.to_numeric(df["累计净值"], errors="coerce")
    df["symbol"] = code
    return df


class CancelledError(Exception):
    pass


def backfill_etf_daily(session: ddb.Session, config, cleaner: DataCleaner,
                       transformer: DataTransformer, error_logger) -> int:
    _output("=" * 60)
    _output("  [1/6] ETF 日线行情 (腾讯财经 API)")
    _output("=" * 60)

    symbols = get_etf_list()
    _output(f"共发现 {len(symbols)} 只 ETF")

    completed = load_progress("etf_daily")
    remaining = [s for s in symbols if s not in completed]
    _output(f"已完成 {len(completed)} 只，剩余 {len(remaining)} 只")

    if not remaining:
        _output("etf_daily 已全部回填，跳过。")
        return 0

    total_written = 0
    total_failed = 0
    batch_df = pd.DataFrame()
    batch_symbols: list[str] = []
    start_time = time.time()
    consecutive_errors = 0
    current_interval = REQUEST_INTERVAL
    failed_codes: list[str] = []

    for i, code in enumerate(remaining):
        if _should_cancel():
            raise CancelledError("用户取消")

        _output(f"  [{i+1}/{len(remaining)}] {code}...")

        try:
            raw_df = _fetch_tencent_etf_daily(code, START_DATE, END_DATE)
            if raw_df is None:
                total_failed += 1
                consecutive_errors += 1
                failed_codes.append(code)

                if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                    _output(f"  ⚠ 连续 {consecutive_errors} 只失败，冷却 {COOLDOWN_SECONDS}s...")
                    time.sleep(COOLDOWN_SECONDS)
                    current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                    consecutive_errors = 0
                continue

            if raw_df.empty:
                batch_symbols.append(code)
                continue

            consecutive_errors = 0
            current_interval = max(current_interval * 0.9, REQUEST_INTERVAL)

            clean_df, _ = cleaner.clean(raw_df, "etf_daily")
            std_df = transformer.transform_etf_daily(clean_df, date.today())
            batch_df = pd.concat([batch_df, std_df], ignore_index=True)
            batch_symbols.append(code)

            if len(batch_symbols) >= BATCH_SIZE:
                try:
                    written = write_to_dolphindb(session, config.db_path, config.tables.etf_daily, batch_df)
                    total_written += written
                    save_progress("etf_daily", batch_symbols)
                except Exception as exc:
                    error_logger.error("etf_daily 批量写入失败: %s", exc)
                    total_failed += len(batch_symbols)
                    failed_codes.extend(batch_symbols)
                batch_df = pd.DataFrame()
                batch_symbols = []

            time.sleep(current_interval)

        except CancelledError:
            raise
        except Exception as exc:
            error_logger.error("etf_daily 回填失败: code=%s, error=%s", code, exc)
            total_failed += 1
            consecutive_errors += 1
            failed_codes.append(code)

            if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                _output(f"  ⚠ 连续 {consecutive_errors} 只失败，冷却 {COOLDOWN_SECONDS}s...")
                time.sleep(COOLDOWN_SECONDS)
                current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                consecutive_errors = 0

    if not batch_df.empty:
        try:
            written = write_to_dolphindb(session, config.db_path, config.tables.etf_daily, batch_df)
            total_written += written
            save_progress("etf_daily", batch_symbols)
        except Exception as exc:
            error_logger.error("etf_daily 最后批次写入失败: %s", exc)
            failed_codes.extend(batch_symbols)

    elapsed = time.time() - start_time
    _output(f"  完成！写入 {total_written} 条，失败 {total_failed} 只，耗时 {elapsed:.0f}s")
    if failed_codes:
        _output(f"  失败代码: {failed_codes[:20]}{'...' if len(failed_codes) > 20 else ''}")
    return total_written


def _fetch_tencent_index_daily(symbol: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    sess = _get_tencent_session()
    tencent_symbol = symbol.replace(".", "")
    all_rows: list[dict] = []
    sy = int(start_date[:4])
    ey = int(end_date[:4])
    year_starts = []
    for y in range(sy, ey + 1):
        ds = f"{y}-01-01" if y > sy else start_date
        de = f"{y}-12-31" if y < ey else end_date
        year_starts.append((ds, de))

    for ds, de in year_starts:
        param = f"{tencent_symbol},day,{ds},{de},500,"
        for attempt in range(3):
            try:
                r = sess.get(_TENCENT_FQ_URL, params={"param": param}, timeout=30)
                if r.status_code != 200:
                    continue
                data = r.json()
                raw_data = data.get("data")
                if isinstance(raw_data, dict):
                    stock_data = raw_data.get(tencent_symbol, {})
                    if not isinstance(stock_data, dict):
                        continue
                    klines = stock_data.get("day", [])
                    for k in klines:
                        all_rows.append({
                            "date": k[0],
                            "open": float(k[1]),
                            "close": float(k[2]),
                            "high": float(k[3]),
                            "low": float(k[4]),
                            "volume": int(float(k[5])),
                            "amount": 0.0,
                        })
                break
            except Exception as exc:
                if attempt < 2:
                    time.sleep(2)
                else:
                    _output(f"  [腾讯] {symbol} 指数 {ds}~{de} 失败: {exc}")
                    return None

    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    df = df.sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    raw = symbol.split(".")[1]
    df["symbol"] = raw
    return df


def backfill_index_daily(session: ddb.Session, config, cleaner: DataCleaner,
                         transformer: DataTransformer, error_logger) -> int:
    _output("=" * 60)
    _output("  [2/6] 跟踪指数日线行情 (腾讯财经 API)")
    _output("=" * 60)

    indices = config.tracking_indices
    _output(f"共 {len(indices)} 个指数")

    total_written = 0
    start_time = time.time()
    consecutive_errors = 0
    current_interval = REQUEST_INTERVAL

    for i, symbol in enumerate(indices):
        if _should_cancel():
            raise CancelledError("用户取消")

        _output(f"  [指数行情] {i+1}/{len(indices)} {symbol}...")

        try:
            raw_df = _fetch_tencent_index_daily(symbol, START_DATE, END_DATE)
            if raw_df is None:
                consecutive_errors += 1
                if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                    _output(f"  ⚠ 连续 {consecutive_errors} 次失败，冷却 {COOLDOWN_SECONDS}s...")
                    time.sleep(COOLDOWN_SECONDS)
                    current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                    consecutive_errors = 0
                continue

            if raw_df.empty:
                continue

            consecutive_errors = 0
            current_interval = max(current_interval * 0.9, REQUEST_INTERVAL)

            clean_df, _ = cleaner.clean(raw_df, "index_daily")
            std_df = transformer.transform_index_daily(clean_df, date.today())
            written = write_to_dolphindb(session, config.db_path, config.tables.index_daily, std_df)
            total_written += written

        except CancelledError:
            raise
        except Exception as exc:
            error_logger.error("index_daily 回填失败: symbol=%s, error=%s", symbol, exc)
            consecutive_errors += 1

            if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                _output(f"  ⚠ 连续 {consecutive_errors} 次失败，冷却 {COOLDOWN_SECONDS}s...")
                time.sleep(COOLDOWN_SECONDS)
                current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                consecutive_errors = 0

        time.sleep(current_interval)

    elapsed = time.time() - start_time
    _output(f"  完成！写入 {total_written} 条，耗时 {elapsed:.0f}s")
    return total_written


_TENCENT_FQ_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
_TENCENT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}
_TENCENT_SESSION = None


def _get_tencent_session():
    import requests as _req
    sess = _req.Session()
    sess.trust_env = False
    sess.proxies = {"http": None, "https": None}
    sess.headers.update(_TENCENT_HEADERS)
    return sess


def _fetch_tencent_kline(code: str, start_date: str, end_date: str,
                         fq_type: str) -> pd.DataFrame | None:
    sess = _get_tencent_session()
    symbol = _code_to_tencent_symbol(code)
    all_rows: list[dict] = []
    year_starts = []
    sy = int(start_date[:4])
    ey = int(end_date[:4])
    for y in range(sy, ey + 1):
        ds = f"{y}-01-01" if y > sy else start_date
        de = f"{y}-12-31" if y < ey else end_date
        year_starts.append((ds, de))

    for ds, de in year_starts:
        param = f"{symbol},day,{ds},{de},500,{fq_type}"
        for attempt in range(3):
            try:
                r = sess.get(_TENCENT_FQ_URL, params={"param": param}, timeout=30)
                if r.status_code != 200:
                    continue
                data = r.json()
                raw_data = data.get("data")
                if isinstance(raw_data, dict):
                    stock_data = raw_data.get(symbol, {})
                    if not isinstance(stock_data, dict):
                        continue
                    if fq_type == "":
                        klines = stock_data.get("day", [])
                    elif fq_type == "qfq":
                        klines = stock_data.get("qfqday", [])
                        if not klines:
                            klines = stock_data.get("day", [])
                    elif fq_type == "hfq":
                        klines = stock_data.get("hfqday", [])
                        if not klines:
                            klines = stock_data.get("day", [])
                    else:
                        klines = []
                    for k in klines:
                        all_rows.append({"date": k[0], "close": float(k[2])})
                break
            except Exception as exc:
                if attempt < 2:
                    time.sleep(2)
                else:
                    _output(f"  [腾讯] {code} fq={fq_type} {ds}~{de} 失败: {exc}")
                    return None

    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = code
    return df


def _fetch_tencent_etf_daily(code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    sess = _get_tencent_session()
    symbol = _code_to_tencent_symbol(code)
    all_rows: list[dict] = []
    sy = int(start_date[:4])
    ey = int(end_date[:4])
    year_starts = []
    for y in range(sy, ey + 1):
        ds = f"{y}-01-01" if y > sy else start_date
        de = f"{y}-12-31" if y < ey else end_date
        year_starts.append((ds, de))

    for ds, de in year_starts:
        param = f"{symbol},day,{ds},{de},500,"
        for attempt in range(3):
            try:
                r = sess.get(_TENCENT_FQ_URL, params={"param": param}, timeout=30)
                if r.status_code != 200:
                    continue
                data = r.json()
                raw_data = data.get("data")
                if isinstance(raw_data, dict):
                    stock_data = raw_data.get(symbol, {})
                    if not isinstance(stock_data, dict):
                        continue
                    klines = stock_data.get("day", [])
                    for k in klines:
                        all_rows.append({
                            "日期": k[0],
                            "开盘": float(k[1]),
                            "收盘": float(k[2]),
                            "最高": float(k[3]),
                            "最低": float(k[4]),
                            "成交量": int(float(k[5])),
                        })
                break
            except Exception as exc:
                if attempt < 2:
                    time.sleep(2)
                else:
                    _output(f"  [腾讯] {code} 日线 {ds}~{de} 失败: {exc}")
                    return None

    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    df = df.sort_values("日期").reset_index(drop=True)
    df["前收盘"] = df["收盘"].shift(1)
    df.loc[df.index[0], "前收盘"] = np.nan
    df["涨跌幅"] = ((df["收盘"] / df["前收盘"] - 1) * 100).round(4)
    df["成交额"] = 0.0
    df["是否ST"] = ""
    df["symbol"] = code
    return df


def backfill_adjust_factor(session: ddb.Session, config, cleaner: DataCleaner,
                           transformer: DataTransformer, error_logger) -> int:
    _output("=" * 60)
    _output("  [3/6] 复权因子 (腾讯财经 前复权/后复权行情间接计算)")
    _output("=" * 60)

    symbols = get_etf_list()
    _output(f"共发现 {len(symbols)} 只 ETF")

    completed = load_progress("adjust_factor")
    remaining = [s for s in symbols if s not in completed]
    _output(f"已完成 {len(completed)} 只，剩余 {len(remaining)} 只")

    if not remaining:
        _output("adjust_factor 已全部回填，跳过。")
        return 0

    total_written = 0
    total_failed = 0
    batch_df = pd.DataFrame()
    batch_symbols: list[str] = []
    start_time = time.time()
    consecutive_errors = 0
    current_interval = REQUEST_INTERVAL
    failed_codes: list[str] = []

    for i, code in enumerate(remaining):
        if _should_cancel():
            raise CancelledError("用户取消")

        _output(f"  [复权因子] {i+1}/{len(remaining)} {code}...")

        try:
            raw_df = _fetch_tencent_kline(code, START_DATE, END_DATE, fq_type="")
            if raw_df is None or raw_df.empty:
                if raw_df is None:
                    total_failed += 1
                    consecutive_errors += 1
                    failed_codes.append(code)
                if raw_df is not None:
                    batch_symbols.append(code)

                if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                    _output(f"  ⚠ 连续 {consecutive_errors} 只失败，冷却 {COOLDOWN_SECONDS}s...")
                    time.sleep(COOLDOWN_SECONDS)
                    current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                    consecutive_errors = 0
                continue

            fore_df = _fetch_tencent_kline(code, START_DATE, END_DATE, fq_type="qfq")
            if fore_df is None or fore_df.empty:
                if fore_df is None:
                    total_failed += 1
                    consecutive_errors += 1
                    failed_codes.append(code)
                if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                    _output(f"  ⚠ 连续 {consecutive_errors} 只失败，冷却 {COOLDOWN_SECONDS}s...")
                    time.sleep(COOLDOWN_SECONDS)
                    current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                    consecutive_errors = 0
                continue

            back_df = _fetch_tencent_kline(code, START_DATE, END_DATE, fq_type="hfq")
            if back_df is None or back_df.empty:
                if back_df is None:
                    total_failed += 1
                    consecutive_errors += 1
                    failed_codes.append(code)
                if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                    _output(f"  ⚠ 连续 {consecutive_errors} 只失败，冷却 {COOLDOWN_SECONDS}s...")
                    time.sleep(COOLDOWN_SECONDS)
                    current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                    consecutive_errors = 0
                continue

            raw_close = raw_df.rename(columns={"close": "raw_close"})[["date", "raw_close"]]
            fore_close = fore_df.rename(columns={"close": "fore_close"})[["date", "fore_close"]]
            back_close = back_df.rename(columns={"close": "back_close"})[["date", "back_close"]]

            merged = raw_close.merge(fore_close, on="date").merge(back_close, on="date")

            merged["fore_adjust"] = (merged["fore_close"] / merged["raw_close"]).round(6)
            merged["back_adjust"] = (merged["back_close"] / merged["raw_close"]).round(6)

            merged = merged.replace([np.inf, -np.inf], np.nan).dropna(subset=["fore_adjust", "back_adjust"])

            if merged.empty:
                batch_symbols.append(code)
                continue

            merged["symbol"] = code
            result = merged[["date", "symbol", "fore_adjust", "back_adjust"]].copy()
            result["symbol"] = result["symbol"].apply(
                lambda c: f"{c}.SH" if c.startswith("51") or c.startswith("60") else f"{c}.SZ"
            )

            batch_df = pd.concat([batch_df, result], ignore_index=True)
            batch_symbols.append(code)
            consecutive_errors = 0
            current_interval = max(current_interval * 0.9, REQUEST_INTERVAL)

            if len(batch_symbols) >= BATCH_SIZE:
                try:
                    written = write_to_dolphindb(
                        session, config.db_path, config.tables.adjust_factor, batch_df
                    )
                    total_written += written
                    save_progress("adjust_factor", batch_symbols)
                except Exception as exc:
                    error_logger.error("adjust_factor 批量写入失败: %s", exc)
                    total_failed += len(batch_symbols)
                    failed_codes.extend(batch_symbols)
                batch_df = pd.DataFrame()
                batch_symbols = []

            time.sleep(current_interval)

        except CancelledError:
            raise
        except Exception as exc:
            error_logger.error("adjust_factor 回填失败: code=%s, error=%s", code, exc)
            total_failed += 1
            consecutive_errors += 1
            failed_codes.append(code)

            if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                _output(f"  ⚠ 连续 {consecutive_errors} 只失败，冷却 {COOLDOWN_SECONDS}s...")
                time.sleep(COOLDOWN_SECONDS)
                current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                consecutive_errors = 0

    if not batch_df.empty:
        try:
            written = write_to_dolphindb(
                session, config.db_path, config.tables.adjust_factor, batch_df
            )
            total_written += written
            save_progress("adjust_factor", batch_symbols)
        except Exception as exc:
            error_logger.error("adjust_factor 最后批次写入失败: %s", exc)
            failed_codes.extend(batch_symbols)

    elapsed = time.time() - start_time
    _output(f"  完成！写入 {total_written} 条，失败 {total_failed} 只，耗时 {elapsed:.0f}s")
    if failed_codes:
        _output(f"  失败代码: {failed_codes[:20]}{'...' if len(failed_codes) > 20 else ''}")
    return total_written


def backfill_etf_nav(session: ddb.Session, config, cleaner: DataCleaner,
                     transformer: DataTransformer, error_logger) -> int:
    _output("=" * 60)
    _output("  [4/6] ETF 净值 (东方财富)")
    _output("=" * 60)

    codes = get_etf_list()
    _output(f"共 {len(codes)} 只 ETF")

    completed = load_progress("etf_nav")
    remaining = [c for c in codes if c not in completed]
    _output(f"已完成 {len(completed)} 只，剩余 {len(remaining)} 只")

    if not remaining:
        _output("etf_nav 已全部回填，跳过。")
        return 0

    total_written = 0
    total_failed = 0
    batch_df = pd.DataFrame()
    batch_codes: list[str] = []
    start_time = time.time()
    consecutive_errors = 0
    current_interval = REQUEST_INTERVAL
    failed_codes: list[str] = []

    for i, code in enumerate(remaining):
        if _should_cancel():
            raise CancelledError("用户取消")

        _output(f"  [净值] {i+1}/{len(remaining)} {code}...")

        try:
            raw_df = fetch_nav_history(code, START_DATE, END_DATE)
            if raw_df is None:
                total_failed += 1
                consecutive_errors += 1
                failed_codes.append(code)

                if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                    _output(f"  ⚠ 连续 {consecutive_errors} 只失败，冷却 {COOLDOWN_SECONDS}s...")
                    time.sleep(COOLDOWN_SECONDS)
                    current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                    consecutive_errors = 0
                continue

            if raw_df.empty:
                continue

            consecutive_errors = 0
            current_interval = max(current_interval * 0.9, REQUEST_INTERVAL)

            clean_df, _ = cleaner.clean(raw_df, "etf_nav")
            std_df = transformer.transform_nav(clean_df, date.today())
            batch_df = pd.concat([batch_df, std_df], ignore_index=True)
            batch_codes.append(code)

            if len(batch_codes) >= BATCH_SIZE:
                try:
                    written = write_to_dolphindb(session, config.db_path, config.tables.etf_nav, batch_df)
                    total_written += written
                    save_progress("etf_nav", batch_codes)
                except Exception as exc:
                    error_logger.error("etf_nav 批量写入失败: %s", exc)
                    total_failed += len(batch_codes)
                    failed_codes.extend(batch_codes)
                batch_df = pd.DataFrame()
                batch_codes = []

            time.sleep(current_interval)

        except CancelledError:
            raise
        except Exception as exc:
            error_logger.error("etf_nav 回填失败: code=%s, error=%s", code, exc)
            total_failed += 1
            consecutive_errors += 1
            failed_codes.append(code)

            if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                _output(f"  ⚠ 连续 {consecutive_errors} 只失败，冷却 {COOLDOWN_SECONDS}s...")
                time.sleep(COOLDOWN_SECONDS)
                current_interval = min(current_interval * 2, ADAPTIVE_INTERVAL_MAX)
                consecutive_errors = 0

    if not batch_df.empty:
        try:
            written = write_to_dolphindb(session, config.db_path, config.tables.etf_nav, batch_df)
            total_written += written
            save_progress("etf_nav", batch_codes)
        except Exception as exc:
            error_logger.error("etf_nav 最后批次写入失败: %s", exc)
            failed_codes.extend(batch_codes)

    elapsed = time.time() - start_time
    _output(f"  完成！写入 {total_written} 条，失败 {total_failed} 只，耗时 {elapsed:.0f}s")
    if failed_codes:
        _output(f"  失败代码: {failed_codes[:20]}{'...' if len(failed_codes) > 20 else ''}")
    return total_written


def backfill_etf_premium(session: ddb.Session, config, transformer: DataTransformer,
                         error_logger) -> int:
    _output("=" * 60)
    _output("  [5/6] 折溢价率 (由 etf_daily + etf_nav 计算)")
    _output("=" * 60)

    if _should_cancel():
        raise CancelledError("用户取消")

    try:
        etf_daily_df = session.run(
            f'select date, symbol, close from loadTable("{config.db_path}", "etf_daily")'
        )
        nav_df = session.run(
            f'select date, symbol, nav from loadTable("{config.db_path}", "etf_nav")'
        )
    except Exception as exc:
        error_logger.error("etf_premium 读取源数据失败: %s", exc)
        _output("  跳过：无法读取 etf_daily 或 etf_nav")
        return 0

    if etf_daily_df.empty or nav_df.empty:
        _output("  跳过：etf_daily 或 etf_nav 为空")
        return 0

    _output(f"  etf_daily: {len(etf_daily_df)} 条, etf_nav: {len(nav_df)} 条")

    premium_df = transformer.compute_premium(etf_daily_df, nav_df)
    if premium_df.empty:
        _output("  折溢价率计算结果为空")
        return 0

    try:
        written = write_to_dolphindb(session, config.db_path, config.tables.etf_premium, premium_df)
        _output(f"  完成！写入 {written} 条")
        return written
    except Exception as exc:
        error_logger.error("etf_premium 写入失败: %s", exc)
        _output(f"  写入失败: {exc}")
        return 0


def backfill_etf_metadata(session: ddb.Session, config, error_logger) -> int:
    _output("=" * 60)
    _output("  [6/6] ETF 元数据 (baostock + YAML 分类)")
    _output("=" * 60)

    if _should_cancel():
        raise CancelledError("用户取消")

    _output("  重新登录 baostock（确保连接存活）...")
    _relogin_baostock()

    for attempt in range(MAX_RETRIES):
        try:
            meta_fetcher = MetadataFetcher(config, error_logger)
            df = meta_fetcher.fetch_metadata()
            break
        except Exception as exc:
            if _is_baostock_garbled_error(exc) and attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                _output(f"  [重试] 元数据获取异常({attempt+1}/{MAX_RETRIES})，{delay}s 后重新登录...")
                _relogin_baostock()
                time.sleep(delay)
            else:
                _output(f"  元数据获取失败: {exc}")
                return 0

    if df.empty:
        _output("  元数据为空，跳过")
        return 0

    db_path = config.db_path + "_meta"
    try:
        written = write_to_dolphindb(session, db_path, config.tables.etf_metadata, df,
                                     key_cols=["symbol"])
        _output(f"  完成！写入 {written} 条")
        return written
    except Exception as exc:
        error_logger.error("etf_metadata 写入失败: %s", exc)
        _output(f"  写入失败: {exc}")
        return 0


def main() -> None:
    _output("=" * 60)
    _output("  ETF 数据仓 — 全量历史回填")
    _output(f"  数据范围: {START_DATE} ~ {END_DATE}")
    _output("=" * 60)

    config = load_config("config.yaml")
    run_logger = get_run_logger(config.run_log_dir, config.log_retention_days)
    error_logger = get_error_logger(config.error_log_dir, config.log_retention_days)

    session = ddb.Session()
    cfg = config.dolphindb
    session.connect(cfg.host, cfg.port, cfg.username, cfg.password)

    DBInitializer(session, config).ensure_initialized()
    _output("数据库初始化完成")

    cleaner = DataCleaner(error_logger)
    transformer = DataTransformer()

    total_start = time.time()
    results = {}

    results["etf_daily"] = backfill_etf_daily(session, config, cleaner, transformer, error_logger)
    results["index_daily"] = backfill_index_daily(session, config, cleaner, transformer, error_logger)
    results["adjust_factor"] = backfill_adjust_factor(session, config, cleaner, transformer, error_logger)
    results["etf_nav"] = backfill_etf_nav(session, config, cleaner, transformer, error_logger)
    results["etf_premium"] = backfill_etf_premium(session, config, transformer, error_logger)
    results["etf_metadata"] = backfill_etf_metadata(session, config, error_logger)

    session.close()

    total_elapsed = time.time() - total_start
    _output("=" * 60)
    _output("  全量回填完成！")
    _output("=" * 60)
    for name, count in results.items():
        _output(f"  {name:20s}: {count:>10,} 条")
    _output(f"  {'总计耗时':20s}: {total_elapsed:.0f}s ({total_elapsed/60:.1f}min)")

    run_logger.info(
        "全量回填完成: etf_daily=%d, index_daily=%d, adjust_factor=%d, "
        "etf_nav=%d, etf_premium=%d, etf_metadata=%d, 耗时=%.0fs",
        results["etf_daily"], results["index_daily"], results["adjust_factor"],
        results["etf_nav"], results["etf_premium"], results["etf_metadata"],
        total_elapsed,
    )


if __name__ == "__main__":
    main()
