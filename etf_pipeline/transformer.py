"""
数据转换模块。

DataTransformer 负责将清洗后的原始数据转换为标准格式，
包括列重命名、symbol 格式化、价格精度处理、类型转换，
以及涨跌停价计算、折溢价率计算等衍生指标。
"""

from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd

__all__ = ["DataTransformer", "COLUMN_MAP"]

COLUMN_MAP = {
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
    "前复权因子": "fore_adjust",
    "后复权因子": "back_adjust",
}

_PRICE_COLS = ["open", "high", "low", "close"]

_OUTPUT_COLS_ETF_DAILY = [
    "date", "symbol", "open", "high", "low", "close",
    "volume", "amount", "preclose", "pct_chg", "is_st",
    "limit_up", "limit_down",
]

_OUTPUT_COLS_INDEX_DAILY = [
    "date", "symbol", "open", "high", "low", "close", "volume", "amount",
]

_OUTPUT_COLS_NAV = ["date", "symbol", "nav", "acc_nav"]

_OUTPUT_COLS_ADJUST = ["date", "symbol", "fore_adjust", "back_adjust"]

_OUTPUT_COLS_PREMIUM = ["date", "symbol", "close", "nav", "premium_rate"]


class DataTransformer:
    """将清洗后的数据转换为标准格式，并计算衍生指标。"""

    def transform_etf_daily(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """转换 ETF 日线数据，含涨跌停价计算。"""
        df = df.copy()

        rename_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
        if rename_map:
            df = df.rename(columns=rename_map)

        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).apply(self.format_symbol)

        for col in _PRICE_COLS:
            if col in df.columns:
                df[col] = df[col].round(4)

        if "preclose" in df.columns:
            df["preclose"] = df["preclose"].round(4)

        if "pct_chg" in df.columns:
            df["pct_chg"] = df["pct_chg"].round(4)

        if "volume" in df.columns:
            df["volume"] = df["volume"].fillna(0).astype("int64")

        if "is_st" in df.columns:
            df["is_st"] = df["is_st"].apply(self._parse_is_st).astype("boolean")
        else:
            df["is_st"] = pd.array([False] * len(df), dtype="boolean")

        df = self._calc_limit_prices(df)

        if "date" in df.columns:
            df["date"] = df["date"].apply(_to_date)
        else:
            df["date"] = trade_date

        available_cols = [c for c in _OUTPUT_COLS_ETF_DAILY if c in df.columns]
        return df[available_cols].reset_index(drop=True)

    def transform_index_daily(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """转换指数日线数据。"""
        df = df.copy()

        rename_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
        if rename_map:
            df = df.rename(columns=rename_map)

        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).apply(self.format_index_symbol)

        for col in _PRICE_COLS:
            if col in df.columns:
                df[col] = df[col].round(4)

        if "volume" in df.columns:
            df["volume"] = df["volume"].astype("int64")

        if "date" in df.columns:
            df["date"] = df["date"].apply(_to_date)
        else:
            df["date"] = trade_date

        available_cols = [c for c in _OUTPUT_COLS_INDEX_DAILY if c in df.columns]
        return df[available_cols].reset_index(drop=True)

    def transform_nav(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """转换 ETF 净值数据。"""
        df = df.copy()

        rename_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
        if rename_map:
            df = df.rename(columns=rename_map)

        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).apply(self.format_symbol)

        for col in ("nav", "acc_nav"):
            if col in df.columns:
                df[col] = df[col].round(6)

        if "date" in df.columns:
            df["date"] = df["date"].apply(_to_date)
        else:
            df["date"] = trade_date

        available_cols = [c for c in _OUTPUT_COLS_NAV if c in df.columns]
        return df[available_cols].reset_index(drop=True)

    def transform_adjust_factor(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """转换复权因子数据。"""
        df = df.copy()

        rename_map = {
            "foreAdjustFactor": "fore_adjust",
            "backAdjustFactor": "back_adjust",
        }
        for old, new in rename_map.items():
            if old in df.columns:
                df = df.rename(columns={old: new})

        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).apply(self.format_symbol)

        for col in ("fore_adjust", "back_adjust"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").round(6)

        if "date" in df.columns:
            df["date"] = df["date"].apply(_to_date)
        else:
            df["date"] = trade_date

        available_cols = [c for c in _OUTPUT_COLS_ADJUST if c in df.columns]
        return df[available_cols].reset_index(drop=True)

    def compute_premium(self, etf_daily_df: pd.DataFrame, nav_df: pd.DataFrame) -> pd.DataFrame:
        """从 ETF 行情和净值数据计算折溢价率。

        Args:
            etf_daily_df: 标准 ETF 日线数据，需含 date, symbol, close
            nav_df: 标准 ETF 净值数据，需含 date, symbol, nav

        Returns:
            折溢价率 DataFrame，列：date, symbol, close, nav, premium_rate
            过滤条件：|premium_rate| > 50% 视为数据异常丢弃
        """
        if etf_daily_df.empty or nav_df.empty:
            return pd.DataFrame(columns=_OUTPUT_COLS_PREMIUM)

        merged = pd.merge(
            etf_daily_df[["date", "symbol", "close"]],
            nav_df[["date", "symbol", "nav"]],
            on=["date", "symbol"],
            how="inner",
        )

        if merged.empty:
            return pd.DataFrame(columns=_OUTPUT_COLS_PREMIUM)

        merged = merged.dropna(subset=["close", "nav"])
        merged = merged[merged["nav"] > 0]

        if merged.empty:
            return pd.DataFrame(columns=_OUTPUT_COLS_PREMIUM)

        merged["premium_rate"] = ((merged["close"] / merged["nav"] - 1) * 100).round(4)
        merged = merged.replace([np.inf, -np.inf], np.nan).dropna(subset=["premium_rate"])
        merged = merged[merged["premium_rate"].abs() <= 50]

        return merged[_OUTPUT_COLS_PREMIUM].reset_index(drop=True)

    @staticmethod
    def _calc_limit_prices(df: pd.DataFrame) -> pd.DataFrame:
        """根据前收盘价和 ST 标识计算涨跌停价。

        规则：
        - ST 股/ETF：涨跌停幅度 ±5%
        - 普通 ETF：涨跌停幅度 ±10%
        - 创业板/科创板 ETF：涨跌停幅度 ±20%
        """
        if "preclose" not in df.columns:
            df["limit_up"] = np.nan
            df["limit_down"] = np.nan
            return df

        df["limit_up"] = np.nan
        df["limit_down"] = np.nan

        mask_st = df["is_st"].fillna(False) == True if "is_st" in df.columns else pd.Series(False, index=df.index)
        mask_cyb = df["symbol"].str.startswith("15").fillna(False) if "symbol" in df.columns else pd.Series(False, index=df.index)
        mask_kcb = df["symbol"].str.startswith("58").fillna(False) if "symbol" in df.columns else pd.Series(False, index=df.index)
        mask_normal = ~mask_st & ~mask_cyb & ~mask_kcb

        preclose = df["preclose"]

        df.loc[mask_st, "limit_up"] = (preclose[mask_st] * 1.05).round(3)
        df.loc[mask_st, "limit_down"] = (preclose[mask_st] * 0.95).round(3)

        df.loc[mask_cyb, "limit_up"] = (preclose[mask_cyb] * 1.20).round(3)
        df.loc[mask_cyb, "limit_down"] = (preclose[mask_cyb] * 0.80).round(3)

        df.loc[mask_kcb, "limit_up"] = (preclose[mask_kcb] * 1.20).round(3)
        df.loc[mask_kcb, "limit_down"] = (preclose[mask_kcb] * 0.80).round(3)

        df.loc[mask_normal, "limit_up"] = (preclose[mask_normal] * 1.10).round(3)
        df.loc[mask_normal, "limit_down"] = (preclose[mask_normal] * 0.90).round(3)

        return df

    @staticmethod
    def _parse_is_st(val: object) -> bool:
        """将 isST 字段转为布尔值。"""
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return bool(val)
        if isinstance(val, str):
            return val.strip().upper() in ("1", "TRUE", "Y", "YES")
        return False

    @staticmethod
    def format_symbol(raw_code: str) -> str:
        """将 6 位数字代码格式化为 {code}.{SH|SZ}。"""
        code = raw_code.strip()
        if "." in code:
            return code

        prefix2 = code[:2] if len(code) >= 2 else ""
        if prefix2 in ("60", "51", "58"):
            suffix = "SH"
        elif prefix2 in ("15", "16"):
            suffix = "SZ"
        else:
            first = code[0] if code else ""
            if first in ("0", "3"):
                suffix = "SZ"
            elif first in ("6", "5"):
                suffix = "SH"
            else:
                suffix = "SH"

        return f"{code}.{suffix}"

    @staticmethod
    def format_index_symbol(raw_code: str) -> str:
        """将指数代码格式化为 {code}.{SH|SZ}。"""
        code = raw_code.strip()
        if "." in code:
            return code

        prefix = code[:3] if len(code) >= 3 else ""
        if prefix in ("000", "880"):
            suffix = "SH"
        elif prefix in ("399"):
            suffix = "SZ"
        else:
            suffix = "SH"

        return f"{code}.{suffix}"


def _to_date(val: object) -> date:
    """将各种日期表示统一转为 datetime.date。"""
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        val = val.strip()
        if len(val) == 8 and val.isdigit():
            return datetime.strptime(val, "%Y%m%d").date()
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    if hasattr(val, "date"):
        return val.date()
    raise ValueError(f"无法转换为 date 类型：{val!r}")
