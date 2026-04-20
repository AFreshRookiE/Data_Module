"""
Parquet 导出模块。

Exporter 负责将 DolphinDB 中的数据导出为 Parquet 文件，
按 ETF 代码分区存储，并生成 manifest.json 元信息清单。
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import dolphindb as ddb
import pandas as pd

from etf_pipeline.config import PipelineConfig
from etf_pipeline.models import ExportResult, TableInfo

__all__ = ["Exporter"]

_TABLE_EXPORT_CONFIG = {
    "etf_daily": {"subdir": "etf_daily", "partition_by": "symbol"},
    "etf_nav": {"subdir": "etf_nav", "partition_by": "symbol"},
    "index_daily": {"subdir": "index_daily", "partition_by": "symbol"},
    "adjust_factor": {"subdir": "adjust_factor", "partition_by": "symbol"},
    "etf_metadata": {"subdir": "metadata", "partition_by": None},
    "etf_premium": {"subdir": "etf_premium", "partition_by": "symbol"},
}


class Exporter:
    """将 DolphinDB 数据导出为 Parquet 文件，按 symbol 分区。"""

    def __init__(self, config: PipelineConfig, session: ddb.Session, error_logger: logging.Logger) -> None:
        self._config = config
        self._session = session
        self._error_logger = error_logger

    def export_all(self) -> list[ExportResult]:
        """导出所有配置的表。"""
        if not self._config.export.enabled:
            return []

        results: list[ExportResult] = []
        for table_key, export_cfg in _TABLE_EXPORT_CONFIG.items():
            table_name = getattr(self._config.tables, table_key, table_key)
            result = self.export_table(table_name, export_cfg)
            results.append(result)

        self._write_manifest(results)
        return results

    def export_table(self, table_name: str, export_cfg: dict) -> ExportResult:
        """导出单张表到 Parquet。"""
        output_dir = os.path.join(self._config.export.output_dir, export_cfg["subdir"])
        os.makedirs(output_dir, exist_ok=True)

        try:
            df = self._read_table(table_name)
            if df is None or df.empty:
                return ExportResult(
                    success=True, table_name=table_name,
                    file_count=0, total_records=0, output_dir=output_dir,
                )

            partition_by = export_cfg.get("partition_by")

            if partition_by and partition_by in df.columns:
                file_count = self._export_partitioned(df, output_dir, partition_by)
            else:
                file_path = os.path.join(output_dir, f"{table_name}.parquet")
                df.to_parquet(file_path, index=False)
                file_count = 1

            return ExportResult(
                success=True,
                table_name=table_name,
                file_count=file_count,
                total_records=len(df),
                output_dir=output_dir,
            )

        except Exception as exc:
            self._error_logger.error("导出失败：%s，%s", table_name, exc)
            return ExportResult(
                success=False, table_name=table_name,
                error_message=str(exc),
            )

    _METADATA_TABLES = {"etf_metadata"}

    def _get_db_path(self, table_name: str) -> str:
        if table_name in self._METADATA_TABLES:
            return self._config.db_path + "_meta"
        return self._config.db_path

    def _read_table(self, table_name: str) -> pd.DataFrame | None:
        try:
            db_path = self._get_db_path(table_name)
            df = self._session.run(
                f'select * from loadTable("{db_path}", "{table_name}")'
            )
            return df
        except Exception as exc:
            self._error_logger.error("读取 DolphinDB 表失败：%s，%s", table_name, exc)
            return None

    @staticmethod
    def _export_partitioned(df: pd.DataFrame, output_dir: str, partition_by: str) -> int:
        """按指定列分区导出，每个值一个 Parquet 文件。"""
        file_count = 0
        for symbol, group in df.groupby(partition_by):
            safe_name = str(symbol).replace(".", "_").replace(":", "_")
            file_path = os.path.join(output_dir, f"{safe_name}.parquet")
            group.to_parquet(file_path, index=False)
            file_count += 1
        return file_count

    def _write_manifest(self, results: list[ExportResult]) -> None:
        """生成 manifest.json 元信息清单。"""
        manifest = {
            "generated_at": datetime.now().isoformat(),
            "db_path": self._config.db_path,
            "tables": {},
        }

        for result in results:
            if not result.success:
                continue

            table_info = {
                "record_count": result.total_records,
                "file_count": result.file_count,
                "output_dir": result.output_dir,
                "last_updated": datetime.now().isoformat(),
            }
            manifest["tables"][result.table_name] = table_info

        manifest_path = os.path.join(self._config.export.output_dir, "manifest.json")
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)

        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
