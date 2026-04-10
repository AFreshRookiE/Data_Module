"""
调度器模块。

封装 APScheduler BlockingScheduler，实现每日 Cron 触发、交易日判断与防重入锁。
"""

from __future__ import annotations

import threading
from datetime import date
from typing import Callable

import chinese_calendar
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from etf_pipeline.config import PipelineConfig
from etf_pipeline.logger import get_run_logger

__all__ = ["is_trading_day", "ETFScheduler"]


def is_trading_day(d: date) -> bool:
    """
    判断给定日期是否为 A 股交易日。

    使用 chinesecalendar 库检查是否为工作日（排除周末和法定节假日含调休）。
    """
    return chinese_calendar.is_workday(d)


class ETFScheduler:
    """APScheduler 封装，每日 Cron 触发，含交易日检查与防重入锁。"""

    def __init__(self, config: PipelineConfig, task_fn: Callable) -> None:
        self._config = config
        self._task_fn = task_fn
        self._lock = threading.Lock()
        self._logger = get_run_logger(
            log_dir=config.run_log_dir,
            retention_days=config.log_retention_days,
        )

        hour, minute = config.schedule_time.split(":")
        self._scheduler = BlockingScheduler()
        self._scheduler.add_job(
            self._trigger_job,
            trigger=CronTrigger(hour=int(hour), minute=int(minute)),
        )

    def start(self) -> None:
        """启动 BlockingScheduler，阻塞主线程。"""
        self._scheduler.start()

    def _trigger_job(self) -> None:
        """
        Cron 触发回调：
        1. 检查交易日，非交易日记录 INFO 日志并返回
        2. 检查防重入锁，已锁定则记录 WARNING 日志并返回
        3. 调用 task_fn(today)
        """
        today = date.today()

        if not is_trading_day(today):
            self._logger.info("今日（%s）为非交易日，跳过任务。", today)
            return

        acquired = self._lock.acquire(blocking=False)
        if not acquired:
            self._logger.warning(
                "上一次任务（%s）仍在执行中，跳过本次触发。", today
            )
            return

        try:
            self._task_fn(today)
        finally:
            self._lock.release()
