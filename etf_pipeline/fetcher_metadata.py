"""
ETF 元数据采集模块。

MetadataFetcher 通过 baostock 获取 ETF 基本信息，
并从 etf_classification.yaml 加载分类和跟踪指数映射。
使用全局 _BaoStockSessionManager 管理 baostock 连接生命周期。
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

import baostock as bs
import pandas as pd
import yaml

from etf_pipeline.config import PipelineConfig
from etf_pipeline.fetcher import _BaoStockSessionManager

__all__ = ["MetadataFetcher"]


class MetadataFetcher:
    """采集 ETF 元数据（名称、分类、跟踪指数、上市日期等）。"""

    def __init__(self, config: PipelineConfig, error_logger: logging.Logger) -> None:
        self._config = config
        self._error_logger = error_logger
        self._session_mgr = _BaoStockSessionManager()
        self._classification_map = self._load_classification()

    def fetch_metadata(self) -> pd.DataFrame:
        """获取全市场 ETF 元数据，合并 baostock 基本信息 + YAML 分类。"""
        self._session_mgr.ensure_login()
        rs = bs.query_stock_basic(code_name="ETF")
        rows: list[dict] = []
        while rs.next():
            row = rs.get_row_data()
            code = row[0]
            raw = code.split(".")[1] if "." in code else code
            if not (raw.startswith("51") or raw.startswith("15") or raw.startswith("16")):
                continue

            suffix = "SH" if raw.startswith("51") else "SZ"
            symbol = f"{raw}.{suffix}"

            classification = self._classification_map.get(symbol, {})

            def _parse_date(s):
                if not s:
                    return None
                try:
                    return datetime.strptime(s, "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    return None

            rows.append({
                "symbol": symbol,
                "name": row[1] if len(row) > 1 else "",
                "etf_type": classification.get("etf_type", "未分类"),
                "tracking_index": classification.get("tracking_index", ""),
                "list_date": _parse_date(row[2] if len(row) > 2 and row[2] else ""),
                "delist_date": _parse_date(row[3] if len(row) > 3 and row[3] else ""),
            })

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)

    def _load_classification(self) -> dict[str, dict]:
        yaml_path = self._config.etf_classification_file
        if not os.path.exists(yaml_path):
            self._error_logger.warning("ETF 分类文件不存在：%s，所有 ETF 将标记为未分类", yaml_path)
            return {}

        try:
            with open(yaml_path, "r", encoding="utf-8") as f:
                data: dict = yaml.safe_load(f) or {}
        except Exception as exc:
            self._error_logger.error("ETF 分类文件加载失败：%s，%s", yaml_path, exc)
            return {}

        result: dict[str, dict] = {}
        for etf_type, category_data in data.items():
            if not isinstance(category_data, dict):
                continue
            mapping_list = category_data.get("mapping", [])
            if not isinstance(mapping_list, list):
                continue
            for item in mapping_list:
                if not isinstance(item, dict):
                    continue
                code = item.get("code", "")
                if code:
                    result[code] = {
                        "etf_type": etf_type,
                        "tracking_index": item.get("tracking_index", ""),
                    }
        return result
