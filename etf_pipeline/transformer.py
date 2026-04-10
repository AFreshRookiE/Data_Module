"""
数据转换模块。

DataTransformer 负责将清洗后的原始数据转换为标准格式，
包括列重命名、symbol 格式化、价格精度处理和类型转换。
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

__all__ = ["DataTransformer", "COLUMN_MAP"]

# AKShare 原始列名 → 标准列名映射
COLUMN_MAP = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
}

_PRICE_COLS = ["open", "high", "low", "close"]
_OUTPUT_COLS = ["date", "symbol", "open", "high", "low", "close", "volume", "amount"]


class DataTransformer:
    """将清洗后的 ETF 数据转换为标准格式。"""

    def transform(self, df: pd.DataFrame, trade_date: date) -> pd.DataFrame:
        """将清洗后的 DataFrame 转换为量化回测标准格式。

        处理步骤：
        1. 重命名中文列名（若存在）
        2. 将 symbol 列（6位数字）格式化为 {6位}.{SH|SZ}
        3. 价格字段 round(4)，volume 转 int64
        4. date 列统一转为 datetime.date

        Args:
            df: 经 DataCleaner 清洗后的 DataFrame。
            trade_date: 当日交易日期（用于 date 列缺失时填充）。

        Returns:
            标准格式 DataFrame，列顺序：date, symbol, open, high, low, close, volume, amount
        """
        df = df.copy()

        # 1. 重命名中文列名（若存在）
        rename_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
        if rename_map:
            df = df.rename(columns=rename_map)

        # 2. 格式化 symbol 列
        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).apply(self.format_symbol)

        # 3. 价格字段保留 4 位小数
        for col in _PRICE_COLS:
            if col in df.columns:
                df[col] = df[col].round(4)

        # volume 转 int64
        if "volume" in df.columns:
            df["volume"] = df["volume"].astype("int64")

        # 4. date 列统一转为 datetime.date
        if "date" in df.columns:
            df["date"] = df["date"].apply(_to_date)
        else:
            df["date"] = trade_date

        # 5. 按指定列顺序输出
        return df[_OUTPUT_COLS].reset_index(drop=True)

    @staticmethod
    def format_symbol(raw_code: str) -> str:
        """将 6 位数字代码格式化为 {code}.{SH|SZ}。

        规则：
        - 60xxxx / 51xxxx → .SH（上交所）
        - 15xxxx / 16xxxx → .SZ（深交所）
        - 其他：首位 0/3 → .SZ，首位 6/5 → .SH，无法判断默认 .SH

        Args:
            raw_code: 6 位 ETF 数字代码，如 "510300"。

        Returns:
            带交易所后缀的代码，如 "510300.SH"。
        """
        code = raw_code.strip()

        # 已带后缀则直接返回
        if "." in code:
            return code

        prefix2 = code[:2] if len(code) >= 2 else ""
        if prefix2 in ("60", "51"):
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
                suffix = "SH"  # 默认上交所

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
