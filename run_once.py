"""
手动触发入口：立即执行一次 ETF 数据拉取并写入 DolphinDB，完成后退出。
双击 exe 或在命令行运行均可。
"""

import sys
from datetime import date

from etf_pipeline.config import ConfigError, load_config
from etf_pipeline.scheduler import is_trading_day
from etf_pipeline.task_runner import TaskRunner


def main() -> None:
    print("=" * 50)
    print("  ETF 数据管道 - 手动触发模式")
    print("=" * 50)

    # 1. 加载配置
    try:
        config = load_config("config.yaml")
    except ConfigError as exc:
        print(f"\n[错误] 配置加载失败：{exc}")
        print("\n请检查 config.yaml 文件是否存在且填写了必填字段。")
        input("\n按 Enter 键退出...")
        sys.exit(1)

    today = date.today()
    print(f"\n当前日期：{today}")

    # 2. 交易日检查（可跳过）
    if not is_trading_day(today):
        print(f"\n[提示] 今日（{today}）为非交易日。")
        choice = input("是否仍要强制拉取数据？(y/N): ").strip().lower()
        if choice != "y":
            print("已取消。")
            input("\n按 Enter 键退出...")
            sys.exit(0)

    # 3. 执行数据拉取
    print(f"\n开始拉取 {today} 的全市场 ETF 数据...\n")
    try:
        runner = TaskRunner(config)
        runner.run(today)
        print("\n[完成] 数据拉取并写入 DolphinDB 成功。")
    except Exception as exc:
        print(f"\n[错误] 任务执行失败：{exc}")
        print("请查看 logs/error/ 目录下的错误日志获取详情。")

    input("\n按 Enter 键退出...")


if __name__ == "__main__":
    main()
