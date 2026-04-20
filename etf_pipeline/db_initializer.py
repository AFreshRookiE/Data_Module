"""
数据库初始化模块。

DBInitializer 负责幂等地检查并创建 DolphinDB 数据库和所有表结构。
首次运行时自动建库建表，后续运行跳过，不修改现有结构。

注意：
- 时序数据表（etf_daily/etf_nav/index_daily/adjust_factor/etf_premium）使用按月分区
- etf_metadata 是维度表（不分区），存放在独立数据库中
"""

from __future__ import annotations

import logging

import dolphindb as ddb

from etf_pipeline.config import PipelineConfig
from etf_pipeline.logger import get_run_logger

__all__ = ["DBInitializer"]

_TABLE_SCHEMAS = {
    "etf_daily": {
        "columns": "`date`symbol`open`high`low`close`volume`amount`preclose`pct_chg`is_st`limit_up`limit_down",
        "types": "[DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE, DOUBLE, BOOL, DOUBLE, DOUBLE]",
        "partition_columns": "`date",
        "sort_columns": "`symbol`date",
        "dimension": False,
    },
    "etf_nav": {
        "columns": "`date`symbol`nav`acc_nav",
        "types": "[DATE, SYMBOL, DOUBLE, DOUBLE]",
        "partition_columns": "`date",
        "sort_columns": "`symbol`date",
        "dimension": False,
    },
    "index_daily": {
        "columns": "`date`symbol`open`high`low`close`volume`amount",
        "types": "[DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE]",
        "partition_columns": "`date",
        "sort_columns": "`symbol`date",
        "dimension": False,
    },
    "adjust_factor": {
        "columns": "`date`symbol`fore_adjust`back_adjust",
        "types": "[DATE, SYMBOL, DOUBLE, DOUBLE]",
        "partition_columns": "`date",
        "sort_columns": "`symbol`date",
        "dimension": False,
    },
    "etf_metadata": {
        "columns": "`symbol`name`etf_type`tracking_index`list_date`delist_date",
        "types": "[SYMBOL, STRING, SYMBOL, SYMBOL, DATE, DATE]",
        "partition_columns": "`symbol",
        "sort_columns": "`symbol",
        "dimension": True,
    },
    "etf_premium": {
        "columns": "`date`symbol`close`nav`premium_rate",
        "types": "[DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE]",
        "partition_columns": "`date",
        "sort_columns": "`symbol`date",
        "dimension": False,
    },
}


class DBInitializer:
    """幂等地初始化 DolphinDB 数据库和所有表结构。"""

    def __init__(self, session: ddb.Session, config: PipelineConfig) -> None:
        self._session = session
        self._config = config
        self._logger: logging.Logger = get_run_logger(
            config.run_log_dir, config.log_retention_days
        )

    @property
    def _metadata_db_path(self) -> str:
        return self._config.db_path + "_meta"

    def ensure_initialized(self) -> None:
        """幂等操作：检查数据库和所有表是否存在，不存在则创建。"""
        db_path = self._config.db_path

        if not self._db_exists(db_path):
            self._logger.info("数据库不存在，开始创建：%s", db_path)
            self._create_tsdb_database(db_path)
            self._logger.info("数据库创建完成：%s", db_path)
        else:
            self._logger.info("数据库已存在：%s", db_path)

        if not self._db_exists(self._metadata_db_path):
            self._logger.info("元数据库不存在，开始创建：%s", self._metadata_db_path)
            self._create_metadata_database()
            self._logger.info("元数据库创建完成：%s", self._metadata_db_path)
        else:
            self._logger.info("元数据库已存在：%s", self._metadata_db_path)

        for table_key, schema in _TABLE_SCHEMAS.items():
            table_name = getattr(self._config.tables, table_key, table_key)
            target_db = self._metadata_db_path if schema["dimension"] else db_path

            if not self._table_exists(target_db, table_name):
                self._logger.info("表不存在，开始创建：%s / %s", target_db, table_name)
                self._create_table(target_db, table_name, schema)
                self._logger.info("表创建完成：%s / %s", target_db, table_name)
            else:
                self._logger.info("表已存在：%s / %s", target_db, table_name)

    def _db_exists(self, db_path: str) -> bool:
        result = self._session.run(f"existsDatabase('{db_path}')")
        return bool(result)

    def _table_exists(self, db_path: str, table_name: str) -> bool:
        result = self._session.run(
            f"existsTable('{db_path}', '{table_name}')"
        )
        return bool(result)

    def _create_tsdb_database(self, db_path: str) -> None:
        self._session.run(f"""
            if (!existsDatabase("{db_path}")) {{
                db = database(
                    "{db_path}",
                    VALUE,
                    2016.01M..2035.12M,
                    engine="TSDB"
                )
            }}
        """)

    def _create_metadata_database(self) -> None:
        self._session.run(f"""
            if (!existsDatabase("{self._metadata_db_path}")) {{
                db = database(
                    "{self._metadata_db_path}",
                    HASH,
                    [SYMBOL, 10],
                    engine="TSDB"
                )
            }}
        """)

    def _create_table(self, db_path: str, table_name: str, schema: dict) -> None:
        keep_dup = "ALL" if schema["dimension"] else "LAST"
        script = f"""
            db = database("{db_path}")
            schema = table(
                1:0,
                {schema["columns"]},
                {schema["types"]}
            )
            db.createPartitionedTable(
                schema, "{table_name}",
                partitionColumns={schema["partition_columns"]},
                sortColumns={schema["sort_columns"]},
                keepDuplicates={keep_dup}
            )
        """
        self._session.run(script)
