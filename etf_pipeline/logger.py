"""
日志工厂模块。

提供运行日志和错误日志的创建函数，使用 TimedRotatingFileHandler 按日滚动。
运行日志与错误日志存放在不同路径，互相独立。
"""

from __future__ import annotations

import logging
import os
from logging.handlers import TimedRotatingFileHandler

__all__ = ["get_run_logger", "get_error_logger"]

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"


def _build_logger(
    name: str,
    log_dir: str,
    filename: str,
    level: int,
    retention_days: int,
) -> logging.Logger:
    """通用 logger 构建函数，避免重复添加 handler。"""
    logger = logging.getLogger(name)

    # 避免重复添加 handler
    if logger.handlers:
        return logger

    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, filename)
    handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",
        backupCount=retention_days,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    handler.setLevel(level)

    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False

    return logger


def get_run_logger(log_dir: str, retention_days: int) -> logging.Logger:
    """创建运行日志 logger，按日滚动，保留 retention_days 天。

    Args:
        log_dir: 运行日志存放目录，不存在时自动创建。
        retention_days: 日志文件保留天数。

    Returns:
        配置好的 logging.Logger 实例（INFO 级别）。
    """
    return _build_logger(
        name="etf_pipeline.run",
        log_dir=log_dir,
        filename="etf_pipeline_run.log",
        level=logging.INFO,
        retention_days=retention_days,
    )


def get_error_logger(log_dir: str, retention_days: int) -> logging.Logger:
    """创建错误日志 logger，按日滚动，保留 retention_days 天，独立路径。

    Args:
        log_dir: 错误日志存放目录（应与运行日志目录不同），不存在时自动创建。
        retention_days: 日志文件保留天数。

    Returns:
        配置好的 logging.Logger 实例（WARNING 级别）。
    """
    return _build_logger(
        name="etf_pipeline.error",
        log_dir=log_dir,
        filename="etf_pipeline_error.log",
        level=logging.WARNING,
        retention_days=retention_days,
    )
