"""
一键更新 DolphinDB ETF 数据。

功能：
1. 自动启动 DolphinDB 服务（如未运行）
2. 自动检测最近交易日
3. 执行增量更新
4. 输出结果汇总
"""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import date, timedelta

import chinese_calendar


def ensure_dolphindb_running(dolphindb_path: str, host: str = "localhost", port: int = 8848) -> bool:
    try:
        import dolphindb as ddb
        s = ddb.Session()
        s.connect(host, port, "admin", "123456")
        s.run("1+1")
        s.close()
        print("  DolphinDB 已在运行")
        return True
    except Exception:
        pass

    print(f"  正在启动 DolphinDB: {dolphindb_path}")
    import os
    work_dir = os.path.dirname(dolphindb_path)
    subprocess.Popen(
        [dolphindb_path],
        cwd=work_dir,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )

    for i in range(30):
        time.sleep(2)
        try:
            import dolphindb as ddb
            s = ddb.Session()
            s.connect(host, port, "admin", "123456")
            s.run("1+1")
            s.close()
            print(f"  DolphinDB 启动成功 (等待 {2*(i+1)}s)")
            return True
        except Exception:
            pass

    print("  DolphinDB 启动超时!")
    return False


def get_latest_trading_day() -> date:
    today = date.today()
    d = today
    for _ in range(10):
        if chinese_calendar.is_workday(d):
            return d
        d -= timedelta(days=1)
    return today


def main() -> None:
    print("=" * 60)
    print("  一键更新 DolphinDB ETF 数据")
    print("=" * 60)

    trade_date = get_latest_trading_day()
    print(f"\n  最近交易日: {trade_date}")

    from etf_pipeline.config import load_config
    config = load_config("config.yaml")

    if not ensure_dolphindb_running(config.dolphindb_path):
        print("\n  [错误] DolphinDB 无法启动，更新中止。")
        input("\n按回车键退出...")
        return

    from etf_pipeline.task_runner import TaskRunner
    from etf_pipeline.logger import get_error_logger

    error_logger = get_error_logger(config.error_log_dir, config.log_retention_days)
    runner = TaskRunner(config)

    print(f"\n  开始更新 (trade_date={trade_date})...\n")
    start = time.time()

    results = runner.run(trade_date)

    elapsed = time.time() - start

    print(f"\n{'=' * 60}")
    print(f"  更新完成! 耗时 {elapsed:.1f}s ({elapsed/60:.1f}min)")
    print(f"{'=' * 60}")

    success = sum(1 for r in results if r.success)
    fail = sum(1 for r in results if not r.success)
    total_records = sum(r.record_count for r in results)

    for r in results:
        status = "OK" if r.success else f"FAIL({r.error_message})"
        print(f"  {r.pipeline_name:20s} {status:15s} {r.record_count:>6} 条  {r.elapsed_seconds:.1f}s")

    print(f"\n  汇总: 成功 {success}/{len(results)}, 失败 {fail}, 总记录 {total_records}")

    input("\n按回车键退出...")


if __name__ == "__main__":
    main()
