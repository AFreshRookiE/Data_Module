"""
数据清洗模块。

DataCleaner 按 C1→C2→C3→C4 规则依次过滤无效记录，输出清洗汇总信息。
"""

from __future__ import annotations

import logging
import re

import pandas as pd

from etf_pipeline.models import CleanSummary

__all__ = ["DataCleaner", "EmptyDataError"]

# AKShare 原始中文列名 → 标准英文列名映射
_COLUMN_MAP: dict[str, str] = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
}

_PRICE_COLS = ["open", "high", "low", "close"]
_SYMBOL_RE = re.compile(r"^\d{6}$")


class EmptyDataError(Exception):
    """清洗后记录数为零时抛出。"""
    pass


class DataCleaner:
    """对原始 ETF 数据执行清洗规则，过滤无效记录。"""

    def __init__(self, error_logger: logging.Logger) -> None:
        self._log = error_logger

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, CleanSummary]:
        """按 C1→C2→C3→C4 顺序依次过滤。

        每条被丢弃的记录写入错误日志（symbol, date, rule, raw_values）。
        若清洗后记录数为 0，写入错误日志并抛出 EmptyDataError。

        Args:
            df: 原始 DataFrame，支持中文或英文列名。

        Returns:
            (clean_df, CleanSummary) 元组。

        Raises:
            EmptyDataError: 清洗后记录数为 0 时抛出。
        """
        df = self._normalize_columns(df.copy())

        input_count = len(df)
        rule_counts: dict[str, int] = {"C1": 0, "C2": 0, "C3": 0, "C4": 0}

        df, rule_counts["C1"] = self._check_c1(df)
        df, rule_counts["C2"] = self._check_c2(df)
        df, rule_counts["C3"] = self._check_c3(df)
        df, rule_counts["C4"] = self._check_c4(df)

        drop_count = sum(rule_counts.values())
        pass_count = input_count - drop_count

        if len(df) == 0:
            self._log.critical(
                "清洗后记录数为 0，任务终止。input_count=%d, rule_counts=%s",
                input_count,
                rule_counts,
            )
            raise EmptyDataError("清洗后记录数为 0")

        summary = CleanSummary(
            input_count=input_count,
            pass_count=pass_count,
            drop_count=drop_count,
            rule_counts=rule_counts,
        )
        return df.reset_index(drop=True), summary

    def _check_c1(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """C1：任意价格字段（open/high/low/close）为空或非正数则丢弃。"""
        available = [c for c in _PRICE_COLS if c in df.columns]
        if not available:
            return df, 0

        mask_bad = pd.Series(False, index=df.index)
        for col in available:
            mask_bad |= df[col].isna() | (df[col] <= 0)

        self._log_dropped(df[mask_bad], "C1", available)
        return df[~mask_bad], int(mask_bad.sum())

    def _check_c2(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """C2：high < low 则丢弃。"""
        if "high" not in df.columns or "low" not in df.columns:
            return df, 0

        mask_bad = df["high"] < df["low"]
        self._log_dropped(df[mask_bad], "C2", ["high", "low"])
        return df[~mask_bad], int(mask_bad.sum())

    def _check_c3(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """C3：volume 或 amount 为负数则丢弃。"""
        mask_bad = pd.Series(False, index=df.index)
        logged_cols: list[str] = []

        if "volume" in df.columns:
            mask_bad |= df["volume"] < 0
            logged_cols.append("volume")
        if "amount" in df.columns:
            mask_bad |= df["amount"] < 0
            logged_cols.append("amount")

        self._log_dropped(df[mask_bad], "C3", logged_cols)
        return df[~mask_bad], int(mask_bad.sum())

    def _check_c4(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """C4：symbol 为空或格式无效（非 6 位纯数字）则丢弃。"""
        if "symbol" not in df.columns:
            return df, 0

        def _is_invalid(val: object) -> bool:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return True
            return not bool(_SYMBOL_RE.match(str(val)))

        mask_bad = df["symbol"].apply(_is_invalid)
        self._log_dropped(df[mask_bad], "C4", ["symbol"])
        return df[~mask_bad], int(mask_bad.sum())

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """将中文列名重命名为英文列名（已是英文的列保持不变）。"""
        rename_map = {k: v for k, v in _COLUMN_MAP.items() if k in df.columns}
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _log_dropped(
        self,
        bad_rows: pd.DataFrame,
        rule: str,
        relevant_cols: list[str],
    ) -> None:
        """将每条被丢弃的记录以 WARNING 级别写入错误日志。"""
        if bad_rows.empty:
            return

        sym_col = "symbol" if "symbol" in bad_rows.columns else None
        date_col = "date" if "date" in bad_rows.columns else None

        for _, row in bad_rows.iterrows():
            symbol = row[sym_col] if sym_col else "N/A"
            date_val = row[date_col] if date_col else "N/A"
            raw_values = {col: row[col] for col in relevant_cols if col in row.index}
            self._log.warning(
                "丢弃记录 rule=%s symbol=%s date=%s raw_values=%s",
                rule,
                symbol,
                date_val,
                raw_values,
            )
