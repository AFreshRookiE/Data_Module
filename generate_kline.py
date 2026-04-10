"""
ETF 日线 K 线图批量生成脚本。

从 DolphinDB 读取每只 ETF 的日线数据，生成 K 线图并保存到桌面文件夹。
文件命名格式：ETF名称-设立日期-结束日期（或最后交易日期）
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import baostock as bs
import dolphindb as ddb
import matplotlib
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd

from etf_pipeline.config import load_config

matplotlib.use("Agg")
plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei"]
plt.rcParams["axes.unicode_minus"] = False

DESKTOP = Path(os.path.expanduser("~/Desktop"))
OUTPUT_DIR = DESKTOP / "ETF_K线图"

mc = mpf.make_marketcolors(
    up="red",
    down="green",
    edge="inherit",
    wick="inherit",
    volume="in",
)
_style = mpf.make_mpf_style(
    marketcolors=mc,
    figcolor="white",
    gridstyle="--",
    gridcolor="#e0e0e0",
    rc={"font.sans-serif": ["SimHei", "Microsoft YaHei"], "axes.unicode_minus": False},
)


def get_etf_metadata() -> dict[str, dict]:
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {lg.error_msg}")
    rs = bs.query_stock_basic(code_name="ETF")
    meta = {}
    while rs.next():
        row = rs.get_row_data()
        code = row[0]
        raw = code.split(".")[1] if "." in code else code
        if raw.startswith("51") or raw.startswith("15") or raw.startswith("16"):
            meta[raw] = {
                "name": row[1],
                "ipo_date": row[2] if row[2] else "未知",
                "out_date": row[3] if row[3] else "",
            }
    bs.logout()
    return meta


def fetch_etf_data(session: ddb.Session, db_path: str, table: str, symbol: str) -> pd.DataFrame | None:
    try:
        df = session.run(
            f'select * from loadTable("{db_path}", "{table}") where symbol="{symbol}" order by date asc'
        )
        if df is None or df.empty:
            return None
        df = df.rename(columns={"date": "Date", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date")
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df[["Open", "High", "Low", "Close", "Volume"]]
        return df
    except Exception:
        return None


def sanitize_filename(name: str) -> str:
    invalid = '<>:"/\\|?*'
    for ch in invalid:
        name = name.replace(ch, "_")
    return name.strip()


def generate_kline(df: pd.DataFrame, etf_name: str, output_path: str) -> bool:
    try:
        fig, _ = mpf.plot(
            df,
            type="candle",
            style=_style,
            volume=True,
            title=f"\n{etf_name}",
            figsize=(16, 9),
            returnfig=True,
            warn_too_much_data=len(df) + 1,
        )
        fig.savefig(output_path, dpi=100, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        return True
    except Exception:
        plt.close("all")
        return False


def main() -> None:
    print("=" * 60)
    print("  ETF 日线 K 线图批量生成工具")
    print("=" * 60)

    config = load_config("config.yaml")
    cfg = config.dolphindb

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"输出目录: {OUTPUT_DIR}")

    meta = get_etf_metadata()
    print(f"共发现 {len(meta)} 只 ETF 元数据")

    session = ddb.Session()
    session.connect(cfg.host, cfg.port, cfg.username, cfg.password)

    symbols = list(meta.keys())
    total = len(symbols)
    success = 0
    failed = 0
    start_time = time.time()

    for i, symbol in enumerate(symbols):
        pct = (i + 1) / total * 100
        info = meta[symbol]
        etf_name = info["name"]
        ipo_date = info["ipo_date"]
        out_date = info["out_date"]

        print(f"\r[{pct:5.1f}%] {symbol} {etf_name}...", end="", flush=True)

        full_symbol = f"{symbol}.SH" if symbol.startswith("5") else f"{symbol}.SZ"
        df = fetch_etf_data(session, config.db_path, config.table_name, full_symbol)
        if df is None or df.empty:
            failed += 1
            continue

        last_date = out_date if out_date else str(df.index[-1].date())
        filename = sanitize_filename(f"{etf_name}-{ipo_date}-{last_date}")
        output_path = str(OUTPUT_DIR / f"{filename}.png")

        if generate_kline(df, etf_name, output_path):
            success += 1
        else:
            failed += 1

    session.close()

    elapsed = time.time() - start_time
    print(f"\n\n生成完成！")
    print(f"  成功: {success}")
    print(f"  失败: {failed}")
    print(f"  耗时: {elapsed:.1f}s")
    print(f"  保存位置: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
