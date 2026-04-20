"""
ETF 数据仓管道包。

提供每日定时采集全市场 ETF 多维数据并写入 DolphinDB 的完整流程。
支持 ETF 行情、净值、跟踪指数、复权因子、折溢价率、元数据等多表管理。
"""

__version__ = "0.2.0"

__all__ = [
    "config",
    "scheduler",
    "task_runner",
    "fetcher",
    "fetcher_nav",
    "fetcher_metadata",
    "cleaner",
    "transformer",
    "writer",
    "db_initializer",
    "exporter",
    "logger",
    "models",
]
