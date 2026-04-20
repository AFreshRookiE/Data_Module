"""
历史成交额回填脚本（通达信pytdx → 保存Parquet → 按月upsert更新）。

DolphinDB etf_daily 表中 amount 字段全部为 0（腾讯API不提供成交额），
本脚本从通达信行情接口获取全量 amount 数据并更新到 DolphinDB。

通达信(pytdx) 优势：
- TCP 协议直连，速度快(25ms/次)，无 HTTP 开销
- 无 IP 限制/封禁风险
- 数据与腾讯实时行情完全一致（交叉验证比值 1.0000）
- amount 单位为元，与 DolphinDB 表一致
- 全量历史覆盖（最早可到 2005 年）

策略：
1. 按 ETF 代码从通达信获取全量历史 amount，保存到本地 Parquet
2. 按月读取 DolphinDB 数据 → 合并 amount → upsert 覆盖写入
"""

import os
import time
from datetime import date

import dolphindb as ddb
import pandas as pd

from etf_pipeline.config import load_config
from etf_pipeline.fetcher import AmountFetcher

PROGRESS_DIR = "backfill_progress"
AMOUNT_CACHE_DIR = "cache/amount_history"


def _load_fetch_progress() -> set[str]:
    path = os.path.join(PROGRESS_DIR, "amount_fetch.txt")
    if not os.path.exists(path):
        return set()
    with open(path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def _save_fetch_progress(codes: list[str]) -> None:
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    path = os.path.join(PROGRESS_DIR, "amount_fetch.txt")
    with open(path, "a", encoding="utf-8") as f:
        for code in codes:
            f.write(f"{code}\n")


def _load_upsert_progress() -> set[str]:
    path = os.path.join(PROGRESS_DIR, "amount_upsert.txt")
    if not os.path.exists(path):
        return set()
    with open(path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def _save_upsert_progress(month_keys: list[str]) -> None:
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    path = os.path.join(PROGRESS_DIR, "amount_upsert.txt")
    with open(path, "a", encoding="utf-8") as f:
        for key in month_keys:
            f.write(f"{key}\n")


def _upsert_month_amount(session, config, month_amount: pd.DataFrame, yr: int, mnth: int) -> int:
    if month_amount.empty:
        return 0

    if mnth == 12:
        month_end = f"{yr + 1}.01.01"
    else:
        month_end = f"{yr}.{mnth + 1:02d}.01"

    month_data = session.run(
        f'select * from loadTable("{config.db_path}", "{config.tables.etf_daily}") '
        f'where date >= {yr}.{mnth:02d}.01 and date < {month_end}'
    )

    if month_data.empty:
        return 0

    amt_map = {}
    for _, row in month_amount.iterrows():
        d = row["date"]
        key_date = d.date() if hasattr(d, "date") else d
        key = (key_date, row["symbol"])
        amt_map[key] = row["amount"]

    updated = 0
    for idx, row in month_data.iterrows():
        d = row["date"]
        key_date = d.date() if hasattr(d, "date") else d
        key = (key_date, row["symbol"])
        if key in amt_map and amt_map[key] > 0 and row["amount"] == 0.0:
            month_data.at[idx, "amount"] = amt_map[key]
            updated += 1

    if updated > 0:
        upserter = ddb.TableUpserter(
            config.db_path,
            config.tables.etf_daily,
            session,
            key_cols=["date", "symbol"],
        )
        upserter.upsert(month_data)

    return updated


def phase1_fetch() -> bool:
    print("\n--- 阶段1：从通达信(pytdx)获取 amount 数据 ---")

    config = load_config("config.yaml")
    session = ddb.Session()
    session.connect(config.dolphindb.host, config.dolphindb.port, config.dolphindb.username, config.dolphindb.password)

    symbols_df = session.run(
        f'select distinct symbol from loadTable("{config.db_path}", "etf_daily")'
    )
    all_codes = [s.split(".")[0] if "." in s else s for s in symbols_df["symbol"].tolist()]
    session.close()

    completed = _load_fetch_progress()
    remaining = [c for c in all_codes if c not in completed]
    print(f"共 {len(all_codes)} 只 ETF，已完成 {len(completed)} 只，剩余 {len(remaining)} 只")

    if not remaining:
        print("阶段1 已完成，跳过。")
        return True

    os.makedirs(AMOUNT_CACHE_DIR, exist_ok=True)

    import logging
    error_logger = logging.getLogger("backfill_amount")
    fetcher = AmountFetcher(config, error_logger)

    fetch_start = time.time()
    fetch_failed = 0
    fetch_empty = 0
    batch_codes: list[str] = []

    for i, code in enumerate(remaining):
        df = fetcher.fetch_amount_history(code)
        if df is not None and not df.empty:
            cache_path = os.path.join(AMOUNT_CACHE_DIR, f"{code}.parquet")
            df.to_parquet(cache_path, index=False)
            batch_codes.append(code)
        elif df is not None and df.empty:
            fetch_empty += 1
            batch_codes.append(code)
        else:
            fetch_failed += 1

        if len(batch_codes) >= 50:
            _save_fetch_progress(batch_codes)
            batch_codes = []

        if (i + 1) % 100 == 0 or i == len(remaining) - 1:
            elapsed = time.time() - fetch_start
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(remaining) - i - 1) / rate / 60 if rate > 0 else 0
            print(
                f"  获取: {i+1}/{len(remaining)} ({(i+1)/len(remaining)*100:.0f}%) "
                f"成功={i+1-fetch_failed-fetch_empty} 空={fetch_empty} 失败={fetch_failed} "
                f"速率={rate:.1f}只/s ETA={eta:.0f}min"
            )

    if batch_codes:
        _save_fetch_progress(batch_codes)

    fetcher.disconnect()

    fetch_elapsed = time.time() - fetch_start
    print(f"\n获取完成: {len(remaining)-fetch_failed} 只成功, {fetch_failed} 只失败, 耗时 {fetch_elapsed:.0f}s")
    return True


def phase2_upsert() -> None:
    print("\n--- 阶段2：按月 upsert 更新 ---")

    config = load_config("config.yaml")
    session = ddb.Session()
    session.connect(config.dolphindb.host, config.dolphindb.port, config.dolphindb.username, config.dolphindb.password)
    print("DolphinDB 连接成功")

    all_frames: list[pd.DataFrame] = []
    if os.path.exists(AMOUNT_CACHE_DIR):
        for fname in os.listdir(AMOUNT_CACHE_DIR):
            if fname.endswith(".parquet"):
                fpath = os.path.join(AMOUNT_CACHE_DIR, fname)
                try:
                    df = pd.read_parquet(fpath)
                    if not df.empty:
                        all_frames.append(df)
                except Exception:
                    pass

    if not all_frames:
        print("无缓存数据可写入。")
        session.close()
        return

    all_df = pd.concat(all_frames, ignore_index=True)
    all_df["ym"] = all_df["date"].apply(lambda d: (d.year, d.month) if hasattr(d, "year") else (d.year, d.month))

    months = sorted(all_df["ym"].unique())
    print(f"共 {len(months)} 个月份需要更新，总记录 {len(all_df)} 条")

    upsert_done = _load_upsert_progress()
    total_updated = 0
    write_start = time.time()
    done_months: list[str] = []

    for mi, (yr, mnth) in enumerate(months):
        month_key = f"{yr}-{mnth:02d}"
        if month_key in upsert_done:
            continue

        month_amount = all_df[all_df["ym"] == (yr, mnth)].drop(columns=["ym"])

        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                updated = _upsert_month_amount(session, config, month_amount, yr, mnth)
                total_updated += updated
                done_months.append(month_key)
                break
            except Exception as exc:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"\n  写入失败 {month_key} (重试 {retry_count}): {exc}")
                    time.sleep(5)
                    try:
                        session.close()
                    except Exception:
                        pass
                    session = ddb.Session()
                    session.connect(config.dolphindb.host, config.dolphindb.port, config.dolphindb.username, config.dolphindb.password)
                else:
                    print(f"\n  写入失败 {month_key} (已重试{max_retries}次): {exc}")

        if len(done_months) >= 5:
            _save_upsert_progress(done_months)
            done_months = []

        if (mi + 1) % 10 == 0 or mi == len(months) - 1:
            elapsed = time.time() - write_start
            print(
                f"  写入: {mi+1}/{len(months)} ({(mi+1)/len(months)*100:.0f}%) "
                f"更新={total_updated} 耗时={elapsed:.0f}s"
            )

    if done_months:
        _save_upsert_progress(done_months)

    write_elapsed = time.time() - write_start

    print(f"\n{'=' * 60}")
    print(f"  阶段2 完成！")
    print(f"  更新: {total_updated} 条, 耗时 {write_elapsed:.0f}s ({write_elapsed/60:.1f}min)")

    try:
        remaining_zero = session.run(
            f'select count(*) as cnt from loadTable("{config.db_path}", "etf_daily") where amount = 0.0'
        )
        print(f"  剩余 amount=0 的记录: {remaining_zero['cnt'].iloc[0]}")
    except Exception:
        pass

    session.close()


def main() -> None:
    print("=" * 60)
    print("  ETF 成交额历史回填（通达信pytdx）")
    print("=" * 60)

    phase1_fetch()
    phase2_upsert()

    print(f"\n{'=' * 60}")
    print("  全部完成！")


if __name__ == "__main__":
    main()
