"""
配置加载模块。

基于 pydantic-settings 实现配置加载，支持从 config.yaml 读取配置，
并支持通过环境变量（如 DOLPHINDB__PASSWORD）覆盖对应字段。
必填项（host、username、password）缺失或配置文件不存在时抛出 ConfigError。
"""

from __future__ import annotations

import os
from typing import Any

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["DolphinDBConfig", "TableNames", "ExportConfig", "PipelineConfig", "load_config", "ConfigError"]


class ConfigError(Exception):
    """配置错误：配置文件缺失、格式无效或必填项为空时抛出。"""
    pass


class DolphinDBConfig(BaseModel):
    """DolphinDB 连接配置。"""

    host: str
    port: int = 8848
    username: str
    password: str


class TableNames(BaseModel):
    """DolphinDB 各表名称配置。"""

    etf_daily: str = "etf_daily"
    etf_nav: str = "etf_nav"
    index_daily: str = "index_daily"
    adjust_factor: str = "adjust_factor"
    etf_metadata: str = "etf_metadata"
    etf_premium: str = "etf_premium"


class ExportConfig(BaseModel):
    """Parquet 导出配置。"""

    enabled: bool = True
    output_dir: str = "data/export"
    format: str = "parquet"


class PipelineConfig(BaseSettings):
    """Pipeline 全局配置，支持 .env 文件与环境变量覆盖。"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
    )

    dolphindb: DolphinDBConfig
    tables: TableNames = TableNames()
    dolphindb_path: str = ""
    tracking_indices: list[str] = [
        "sh.000300", "sh.000905", "sh.000852", "sh.000016",
        "sz.399006", "sz.399673", "sh.000688",
    ]
    schedule_time: str = "18:00"
    fetch_retry_times: int = 3
    fetch_retry_interval: int = 10
    db_retry_times: int = 3
    db_retry_interval: int = 15
    run_log_dir: str = "logs/run"
    error_log_dir: str = "logs/error"
    log_retention_days: int = 30
    db_path: str = "dfs://etf_db"
    etf_classification_file: str = "etf_classification.yaml"
    export: ExportConfig = ExportConfig()
    cache_recovery_on_startup: bool = True

    @property
    def table_name(self) -> str:
        """向后兼容：返回 etf_daily 表名。"""
        return self.tables.etf_daily


def _apply_env_overrides(data: dict[str, Any]) -> dict[str, Any]:
    """将环境变量中的嵌套覆盖值合并到配置字典中。"""
    prefix = "DOLPHINDB__"
    for key, value in os.environ.items():
        if key.upper().startswith(prefix):
            field = key[len(prefix):].lower()
            if "dolphindb" not in data:
                data["dolphindb"] = {}
            data["dolphindb"][field] = value
    return data


def load_config(config_path: str = "config.yaml") -> PipelineConfig:
    """从 YAML 文件加载配置，缺失必填项时抛出 ConfigError。"""
    if not os.path.exists(config_path):
        raise ConfigError(
            f"配置文件不存在：{config_path}。"
            "请创建配置文件并填写必填字段（dolphindb.host、dolphindb.username、dolphindb.password）。"
        )

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw: dict[str, Any] = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"配置文件格式无效（YAML 解析失败）：{exc}") from exc

    raw = _apply_env_overrides(raw)

    dolphindb_cfg: dict[str, Any] = raw.get("dolphindb") or {}
    missing: list[str] = []
    for field in ("host", "username", "password"):
        value = dolphindb_cfg.get(field)
        if not value or (isinstance(value, str) and not value.strip()):
            missing.append(f"dolphindb.{field}")

    if missing:
        raise ConfigError(
            f"配置文件缺少必填字段：{', '.join(missing)}。"
            "请在 config.yaml 或对应环境变量中提供这些字段的值。"
        )

    try:
        config = PipelineConfig.model_validate(raw)
    except Exception as exc:
        raise ConfigError(f"配置校验失败：{exc}") from exc

    return config
