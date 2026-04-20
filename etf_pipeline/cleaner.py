"""
数据清洗模块。

DataCleaner 按 C1→C2→C3→C4→C5→C6→C7 规则依次过滤无效记录，
W1/W2 为仅警告不丢弃的软规则，输出清洗汇总信息。
支持 ETF 行情、指数行情、净值等不同数据类型的清洗。

硬规则（丢弃记录）：
  C1: 价格字段为空或非正
  C2: high < low
  C3: volume/amount 为负 / nav 为空或非正
  C4: symbol 格式无效
  C5: 涨跌幅超限（基于代码前缀粗判板块涨跌停幅度）
  C6: 涨跌幅一致性（pct_chg 与 (close-preclose)/preclose 偏差过大）
  C7: OHLC 逻辑完整性（open/close 必须在 [low, high] 范围内）

软规则（仅警告）：
  W1: 成交量/成交额矛盾（一方为零另一方非零）
  W2: 零成交量

业务级校验（post-transform）：
  V1: 收盘价超出涨跌停范围
"""

from __future__ import annotations

import logging
import re

import numpy as np
import pandas as pd

from etf_pipeline.models import CleanSummary, ValidationSummary

__all__ = ["DataCleaner", "EmptyDataError"]

_COLUMN_MAP: dict[str, str] = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
    "前收盘": "preclose",
    "涨跌幅": "pct_chg",
    "是否ST": "is_st",
    "单位净值": "nav",
    "累计净值": "acc_nav",
}

_PRICE_COLS = ["open", "high", "low", "close"]
_SYMBOL_RE = re.compile(r"^\d{6}$")


class EmptyDataError(Exception):
    pass


class DataCleaner:
    """对原始数据执行清洗规则，过滤无效记录。"""

    def __init__(self, error_logger: logging.Logger) -> None:
        self._log = error_logger

    def clean(self, df: pd.DataFrame, data_type: str = "etf_daily") -> tuple[pd.DataFrame, CleanSummary]:
        """按 C1→C2→C3→C4→C5→C6→C7 顺序依次过滤，W1/W2 仅警告。"""
        df = self._normalize_columns(df.copy())

        input_count = len(df)
        rule_counts: dict[str, int] = {"C1": 0, "C2": 0, "C3": 0, "C4": 0, "C5": 0, "C6": 0, "C7": 0}

        df, rule_counts["C1"] = self._check_c1(df)
        df, rule_counts["C2"] = self._check_c2(df)
        df, rule_counts["C3"] = self._check_c3(df, data_type)
        df, rule_counts["C4"] = self._check_c4(df)
        df, rule_counts["C5"] = self._check_c5(df, data_type)
        df, rule_counts["C6"] = self._check_c6(df, data_type)
        df, rule_counts["C7"] = self._check_c7(df, data_type)

        warning_counts: dict[str, int] = {
            "W1": self._check_w1(df, data_type),
            "W2": self._check_w2(df, data_type),
        }

        drop_count = sum(rule_counts.values())
        pass_count = input_count - drop_count

        if len(df) == 0:
            self._log.critical(
                "清洗后记录数为 0，任务终止。input_count=%d, rule_counts=%s, data_type=%s",
                input_count, rule_counts, data_type,
            )
            raise EmptyDataError(f"清洗后记录数为 0 (data_type={data_type})")

        summary = CleanSummary(
            input_count=input_count,
            pass_count=pass_count,
            drop_count=drop_count,
            rule_counts=rule_counts,
            warning_counts=warning_counts,
        )
        return df.reset_index(drop=True), summary

    def validate_etf_daily(self, df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationSummary]:
        """对已转换的 ETF 日线数据执行业务级校验（post-transform）。

        V1: 收盘价超出涨跌停范围。
        """
        input_count = len(df)
        rule_counts: dict[str, int] = {"V1": 0}

        if not df.empty and all(c in df.columns for c in ("close", "limit_up", "limit_down")):
            df, rule_counts["V1"] = self._check_v1(df)

        drop_count = sum(rule_counts.values())
        pass_count = input_count - drop_count

        summary = ValidationSummary(
            input_count=input_count,
            pass_count=pass_count,
            drop_count=drop_count,
            rule_counts=rule_counts,
        )
        return df.reset_index(drop=True), summary

    def _check_c1(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """C1：任意价格字段为空或非正数则丢弃。"""
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

    def _check_c3(self, df: pd.DataFrame, data_type: str) -> tuple[pd.DataFrame, int]:
        """C3：volume/amount 为负数则丢弃；NAV 数据检查 nav 为正。"""
        mask_bad = pd.Series(False, index=df.index)
        logged_cols: list[str] = []

        if "volume" in df.columns:
            mask_bad |= df["volume"] < 0
            logged_cols.append("volume")
        if "amount" in df.columns:
            mask_bad |= df["amount"] < 0
            logged_cols.append("amount")
        if "nav" in df.columns:
            mask_bad |= df["nav"].isna() | (df["nav"] <= 0)
            logged_cols.append("nav")

        self._log_dropped(df[mask_bad], "C3", logged_cols)
        return df[~mask_bad], int(mask_bad.sum())

    def _check_c4(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """C4：symbol 为空或格式无效则丢弃。"""
        if "symbol" not in df.columns:
            return df, 0

        def _is_invalid(val: object) -> bool:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return True
            s = str(val)
            if "." in s:
                s = s.split(".")[0]
            return not bool(_SYMBOL_RE.match(s))

        mask_bad = df["symbol"].apply(_is_invalid)
        self._log_dropped(df[mask_bad], "C4", ["symbol"])
        return df[~mask_bad], int(mask_bad.sum())

    def _check_c5(self, df: pd.DataFrame, data_type: str) -> tuple[pd.DataFrame, int]:
        """C5：涨跌幅超限检查（基于代码前缀粗判板块涨跌停幅度）。

        创业板(15xxxx)/科创板(58xxxx): ±20% + 0.5% 容差
        其他 ETF: ±10% + 0.5% 容差
        ST 标识在此阶段无法精确判断，由 V1 精确校验。
        """
        if data_type != "etf_daily" or "pct_chg" not in df.columns or "symbol" not in df.columns:
            return df, 0

        mask_bad = pd.Series(False, index=df.index)
        sym = df["symbol"].astype(str)
        pct_abs = df["pct_chg"].abs()

        mask_20 = sym.str.startswith("15") | sym.str.startswith("58")
        mask_bad |= mask_20 & (pct_abs > 20.5)

        mask_normal = ~mask_20
        mask_bad |= mask_normal & (pct_abs > 10.5)

        self._log_dropped(df[mask_bad], "C5", ["pct_chg", "symbol"])
        return df[~mask_bad], int(mask_bad.sum())

    def _check_c6(self, df: pd.DataFrame, data_type: str) -> tuple[pd.DataFrame, int]:
        """C6：涨跌幅一致性检查。

        比较 pct_chg 与 (close - preclose) / preclose * 100 的偏差，
        偏差超过 1.0% 视为数据不一致。
        """
        if data_type != "etf_daily":
            return df, 0
        if not all(c in df.columns for c in ("pct_chg", "close", "preclose")):
            return df, 0

        valid = df["preclose"].notna() & (df["preclose"] > 0) & df["close"].notna() & df["pct_chg"].notna()
        if not valid.any():
            return df, 0

        calc_pct = pd.Series(np.nan, index=df.index)
        calc_pct[valid] = ((df.loc[valid, "close"] - df.loc[valid, "preclose"]) / df.loc[valid, "preclose"] * 100)

        diff = (df["pct_chg"] - calc_pct).abs()
        mask_bad = valid & (diff > 1.0)

        self._log_dropped(df[mask_bad], "C6", ["pct_chg", "close", "preclose"])
        return df[~mask_bad], int(mask_bad.sum())

    def _check_c7(self, df: pd.DataFrame, data_type: str) -> tuple[pd.DataFrame, int]:
        """C7：OHLC 逻辑完整性检查。

        open 必须在 [low, high] 范围内，
        close 必须在 [low, high] 范围内。
        """
        if data_type not in ("etf_daily", "index_daily"):
            return df, 0

        mask_bad = pd.Series(False, index=df.index)
        logged_cols: list[str] = []

        if all(c in df.columns for c in ("open", "high", "low")):
            mask_bad |= (df["open"] > df["high"]) | (df["open"] < df["low"])
            logged_cols.extend(["open", "high", "low"])

        if all(c in df.columns for c in ("close", "high", "low")):
            mask_bad |= (df["close"] > df["high"]) | (df["close"] < df["low"])
            if "close" not in logged_cols:
                logged_cols.extend(["close", "high", "low"])

        if not logged_cols:
            return df, 0

        self._log_dropped(df[mask_bad], "C7", list(set(logged_cols)))
        return df[~mask_bad], int(mask_bad.sum())

    def _check_w1(self, df: pd.DataFrame, data_type: str) -> int:
        """W1：成交量/成交额矛盾（仅警告，不丢弃）。

        volume=0 但 amount>0，或 volume>0 但 amount=0。
        当 amount 列全为 0 时跳过（数据源不提供成交额）。
        """
        if data_type not in ("etf_daily", "index_daily"):
            return 0
        if "volume" not in df.columns or "amount" not in df.columns:
            return 0

        if df["amount"].eq(0).all():
            return 0

        mask_contradict = (
            ((df["volume"] == 0) & (df["amount"] > 0))
            | ((df["volume"] > 0) & (df["amount"] == 0))
        )
        count = int(mask_contradict.sum())
        if count > 0:
            self._log.warning(
                "W1: 成交量/成交额矛盾 %d 条 (volume=0&amount>0 或 volume>0&amount=0)",
                count,
            )
        return count

    def _check_w2(self, df: pd.DataFrame, data_type: str) -> int:
        """W2：零成交量（仅警告，不丢弃）。"""
        if data_type not in ("etf_daily", "index_daily"):
            return 0
        if "volume" not in df.columns:
            return 0

        mask_zero = df["volume"] == 0
        count = int(mask_zero.sum())
        if count > 0:
            self._log.warning("W2: 零成交量 %d 条", count)
        return count

    def _check_v1(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """V1：收盘价超出涨跌停范围。

        使用 transformer 计算的 limit_up/limit_down 进行精确校验，
        允许 0.01 元容差（浮点精度）。
        """
        valid_limits = df["limit_up"].notna() & df["limit_down"].notna()
        if not valid_limits.any():
            return df, 0

        tolerance = 0.01
        mask_over = valid_limits & (
            (df["close"] > df["limit_up"] + tolerance)
            | (df["close"] < df["limit_down"] - tolerance)
        )
        count = int(mask_over.sum())
        if count > 0:
            self._log.warning("V1: 收盘价超出涨跌停范围 %d 条", count)
            for _, row in df[mask_over].iterrows():
                self._log.warning(
                    "V1 detail: symbol=%s date=%s close=%.3f limit_up=%.3f limit_down=%.3f",
                    row.get("symbol", "N/A"), row.get("date", "N/A"),
                    row.get("close", 0), row.get("limit_up", 0), row.get("limit_down", 0),
                )
        return df[~mask_over], count

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """将中文列名重命名为英文列名。"""
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
                rule, symbol, date_val, raw_values,
            )
