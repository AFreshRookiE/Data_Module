"""
数据库初始化模块。

DBInitializer 负责幂等地检查并创建 DolphinDB 数据库和 etf_daily 表结构。
首次运行时自动建库建表，后续运行跳过，不修改现有结构。
"""

from __future__ import annotations

import logging

import dolphindb as ddb

from etf_pipeline.config import PipelineConfig
from etf_pipeline.logger import get_run_logger

__all__ = ["DBInitializer"]

# DolphinDB 建库建表脚本（含幂等保护）
_CREATE_SCRIPT = """
if (!existsDatabase("{db_path}")) {{
    db = database(
        "{db_path}",
        VALUE,
        2016.01M..2030.12M,
        engine="TSDB"
    )
}}
if (!existsTable("{db_path}", "{table_name}")) {{
    db = database("{db_path}")
    schema = table(
        1:0,
        `date`symbol`open`high`low`close`volume`amount,
        [DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE]
    )
    db.createPartitionedTable(
        schema, "{table_name}",
        partitionColumns=`date,
        sortColumns=`symbol`date,
        keepDuplicates=LAST
    )
}}
"""


class DBInitializer:
    """幂等地初始化 DolphinDB 数据库和表结构。"""

    def __init__(self, session: ddb.Session, config: PipelineConfig) -> None:
        self._session = session
        self._config = config
        self._logger: logging.Logger = get_run_logger(
            config.run_log_dir, config.log_retention_days
        )

    def ensure_initialized(self) -> None:
        """幂等操作：检查数据库和表是否存在，不存在则创建，已存在则跳过。

        不修改现有表结构。
        """
        db_path = self._config.db_path
        table_name = self._config.table_name

        if self._db_exists():
            self._logger.info("数据库已存在，跳过创建：%s", db_path)
        else:
            self._logger.info("数据库不存在，开始创建：%s", db_path)
            self._create_db_and_table()
            self._logger.info("数据库和表创建完成：%s / %s", db_path, table_name)
            return

        if self._table_exists():
            self._logger.info("表已存在，跳过创建：%s / %s", db_path, table_name)
        else:
            self._logger.info("表不存在，开始创建：%s / %s", db_path, table_name)
            self._create_db_and_table()
            self._logger.info("表创建完成：%s / %s", db_path, table_name)

    def _db_exists(self) -> bool:
        """检查目标数据库是否存在。"""
        result = self._session.run(f"existsDatabase('{self._config.db_path}')")
        return bool(result)

    def _table_exists(self) -> bool:
        """检查 etf_daily 表是否存在。"""
        result = self._session.run(
            f"existsTable('{self._config.db_path}', '{self._config.table_name}')"
        )
        return bool(result)

    def _create_db_and_table(self) -> None:
        """执行建库建表 DolphinDB 脚本（脚本内含幂等保护）。"""
        script = _CREATE_SCRIPT.format(
            db_path=self._config.db_path,
            table_name=self._config.table_name,
        )
        self._session.run(script)
