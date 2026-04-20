"""
数据模型模块。

定义管道内部使用的数据传输对象（dataclass）。
"""

__all__ = [
    "CleanSummary",
    "ValidationSummary",
    "WriteResult",
    "PipelineResult",
    "ExportResult",
    "TableInfo",
]

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class CleanSummary:
    """数据清洗汇总信息。"""

    input_count: int
    pass_count: int
    drop_count: int
    rule_counts: dict[str, int] = field(
        default_factory=lambda: {"C1": 0, "C2": 0, "C3": 0, "C4": 0, "C5": 0, "C6": 0, "C7": 0}
    )
    warning_counts: dict[str, int] = field(
        default_factory=lambda: {"W1": 0, "W2": 0}
    )


@dataclass
class ValidationSummary:
    """业务级校验汇总信息（post-transform）。"""

    input_count: int
    pass_count: int
    drop_count: int
    rule_counts: dict[str, int] = field(
        default_factory=lambda: {"V1": 0}
    )


@dataclass
class WriteResult:
    """数据写入结果汇总。"""

    success_count: int
    skip_count: int
    drop_count: int
    has_quality_warning: bool
    table_name: str = ""


@dataclass
class PipelineResult:
    """单条管道执行结果。"""

    pipeline_name: str
    table_name: str
    success: bool
    record_count: int = 0
    skip_count: int = 0
    drop_count: int = 0
    error_message: str = ""
    elapsed_seconds: float = 0.0


@dataclass
class ExportResult:
    """Parquet 导出结果。"""

    success: bool
    table_name: str
    file_count: int = 0
    total_records: int = 0
    output_dir: str = ""
    manifest_path: str = ""
    error_message: str = ""


@dataclass
class TableInfo:
    """DolphinDB 表元信息，用于 manifest.json。"""

    table_name: str
    record_count: int = 0
    symbol_count: int = 0
    date_range_start: str = ""
    date_range_end: str = ""
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    columns: list[str] = field(default_factory=list)
