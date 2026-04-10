"""
历史数据回填脚本。

通过腾讯财经接口获取全市场 ETF 自 2016-01-01 至今的日线 OHLCV 数据，
经清洗、转换后写入 DolphinDB。

特性：
- 支持断点续传（记录已完成 ETF 到 backfill_progress.txt）
- 分批写入 DolphinDB（每 BATCH_SIZE 只 ETF 写入一次）
- 实时显示进度
- 腾讯财经接口稳定可靠，支持历史数据回溯
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date

import baostock as bs
import dolphindb as ddb
import pandas as pd
import requests

from etf_pipeline.cleaner import DataCleaner
from etf_pipeline.config import load_config
from etf_pipeline.logger import get_error_logger, get_run_logger
from etf_pipeline.transformer import DataTransformer

START_DATE = "2016-01-01"
END_DATE = date.today().strftime("%Y-%m-%d")
BATCH_SIZE = 50
PROGRESS_FILE = "backfill_progress.txt"

_TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def _code_to_tencent_symbol(code: str) -> str:
    if code.startswith("5") or code.startswith("6"):
        return f"sh{code}"
    return f"sz{code}"


def get_etf_list() -> list[str]:
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {lg.error_msg}")
    rs = bs.query_stock_basic(code_name="ETF")
    etf_codes: list[str] = []
    while rs.next():
        row = rs.get_row_data()
        code = row[0]
        raw = code.split(".")[1] if "." in code else code
        if raw.startswith("51") or raw.startswith("15") or raw.startswith("16"):
            etf_codes.append(raw)
    bs.logout()
    return etf_codes


def fetch_etf_history_tencent(code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    symbol = _code_to_tencent_symbol(code)
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    all_klines: list[list] = []
    for year in range(start_year, end_year + 1):
        y_start = f"{year}-01-01"
        y_end = f"{year}-12-31"
        params = {
            "_var": "kline_dayqfq",
            "param": f"{symbol},day,{y_start},{y_end},800,qfq",
        }
        try:
            r = requests.get(_TENCENT_KLINE_URL, params=params, headers=_HEADERS, timeout=30)
            if r.status_code != 200:
                continue
            json_str = re.sub(r"^[a-zA-Z_]+=", "", r.text)
            data = json.loads(json_str)
            if data.get("code") != 0:
                continue
            if not isinstance(data.get("data"), dict):
                continue
            stock_data = data["data"].get(symbol, {})
            klines = stock_data.get("qfqday", [])
            if not klines:
                klines = stock_data.get("day", [])
            all_klines.extend(klines)
        except Exception:
            continue
    if not all_klines:
        return None
    df = pd.DataFrame(all_klines, columns=["日期", "开盘", "收盘", "最高", "最低", "成交量"])
    for col in ("开盘", "收盘", "最高", "最低", "成交量"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["成交额"] = 0.0
    df["symbol"] = code
    df = df.drop_duplicates(subset=["日期"], keep="last")
    return df


def load_progress() -> set[str]:
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def save_progress(symbols: list[str]) -> None:
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        for s in symbols:
            f.write(s + "\n")


def write_to_dolphindb(session: ddb.Session, db_path: str, table_name: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    upserter = ddb.TableUpserter(
        db_path,
        table_name,
        session,
        key_cols=["date", "symbol"],
    )
    upserter.upsert(df)
    return len(df)


def main() -> None:
    print("=" * 60)
    print("  ETF 历史数据回填工具 (腾讯财经数据源)")
    print(f"  数据范围: {START_DATE} ~ {END_DATE}")
    print("=" * 60)

    config = load_config("config.yaml")
    run_logger = get_run_logger(config.run_log_dir, config.log_retention_days)
    error_logger = get_error_logger(config.error_log_dir, config.log_retention_days)

    symbols = get_etf_list()
    print(f"\n共发现 {len(symbols)} 只 ETF")

    completed = load_progress()
    remaining = [s for s in symbols if s not in completed]
    print(f"已完成 {len(completed)} 只，剩余 {len(remaining)} 只\n")

    if not remaining:
        print("所有 ETF 已回填完成，无需操作。")
        return

    session = ddb.Session()
    cfg = config.dolphindb
    session.connect(cfg.host, cfg.port, cfg.username, cfg.password)

    cleaner = DataCleaner(error_logger)
    transformer = DataTransformer()

    total_written = 0
    total_failed = 0
    batch_df = pd.DataFrame()
    batch_symbols: list[str] = []
    start_time = time.time()

    for i, code in enumerate(remaining):
        progress_pct = (i + 1) / len(remaining) * 100
        print(f"\r[{progress_pct:5.1f}%] 正在处理 {code} ({i+1}/{len(remaining)})...", end="", flush=True)

        try:
            raw_df = fetch_etf_history_tencent(code, START_DATE, END_DATE)
            if raw_df is None or raw_df.empty:
                total_failed += 1
                batch_symbols.append(code)
                continue

            clean_df, _ = cleaner.clean(raw_df)
            std_df = transformer.transform(clean_df, date.today())
            batch_df = pd.concat([batch_df, std_df], ignore_index=True)
            batch_symbols.append(code)

            if len(batch_symbols) >= BATCH_SIZE:
                try:
                    written = write_to_dolphindb(session, config.db_path, config.table_name, batch_df)
                    total_written += written
                except Exception as exc:
                    error_logger.error("批量写入失败: %s", exc)
                    total_failed += len(batch_symbols)
                save_progress(batch_symbols)
                batch_df = pd.DataFrame()
                batch_symbols = []

            time.sleep(0.3)

        except Exception as exc:
            error_logger.error("回填失败: code=%s, error=%s", code, exc)
            total_failed += 1
            batch_symbols.append(code)

    if not batch_df.empty:
        try:
            written = write_to_dolphindb(session, config.db_path, config.table_name, batch_df)
            total_written += written
        except Exception as exc:
            error_logger.error("最后批次写入失败: %s", exc)
        save_progress(batch_symbols)

    session.close()

    elapsed = time.time() - start_time
    print(f"\n\n回填完成！")
    print(f"  写入记录数: {total_written}")
    print(f"  失败 ETF 数: {total_failed}")
    print(f"  耗时: {elapsed:.1f}s")

    run_logger.info(
        "历史回填完成: 写入=%d, 失败=%d, 耗时=%.1fs, 范围=%s~%s",
        total_written, total_failed, elapsed, START_DATE, END_DATE,
    )


if __name__ == "__main__":
    main()
