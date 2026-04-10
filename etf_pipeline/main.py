"""
程序入口模块。

负责加载配置、初始化 TaskRunner 并启动 ETFScheduler。
配置文件缺失或必填项为空时打印明确错误信息并退出。
"""

from __future__ import annotations

import sys

from etf_pipeline.config import ConfigError, load_config
from etf_pipeline.scheduler import ETFScheduler
from etf_pipeline.task_runner import TaskRunner

__all__ = ["main"]


def main() -> None:
    """启动 ETF 数据管道服务。"""
    # 1. 加载配置
    try:
        config = load_config("config.yaml")
    except ConfigError as exc:
        print(f"[错误] 配置加载失败：{exc}", file=sys.stderr)
        sys.exit(1)

    # 2. 初始化 TaskRunner
    task_runner = TaskRunner(config)

    # 3. 初始化调度器
    scheduler = ETFScheduler(config, task_runner.run)

    # 4. 启动调度器（阻塞主线程）
    try:
        print("ETF 数据管道已启动，按 Ctrl+C 退出。")
        scheduler.start()
    except KeyboardInterrupt:
        print("\nETF 数据管道已停止。")


if __name__ == "__main__":
    main()
