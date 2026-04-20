"""
ETF 净值数据采集模块。

NavFetcher 通过东方财富基金 API 获取 ETF 历史净值数据（单位净值、累计净值）。
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date

import pandas as pd
import requests

from etf_pipeline.config import PipelineConfig

__all__ = ["NavFetcher"]

_EM_NAV_URL = "https://fund.eastmoney.com/f10/F10DataApi.aspx"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://fund.eastmoney.com/",
}


class NavFetcher:
    """通过东方财富 API 采集 ETF 净值数据。"""

    def __init__(self, config: PipelineConfig, error_logger: logging.Logger) -> None:
        self._config = config
        self._error_logger = error_logger
        self.skip_count: int = 0

    def fetch_single_nav(self, code: str, trade_date: date) -> pd.DataFrame | None:
        """获取单只 ETF 当日净值。

        Args:
            code: 6位数字代码，如 "510300"
            trade_date: 交易日期
        """
        date_str = trade_date.strftime("%Y-%m-%d")
        params = {
            "type": "lsjz",
            "code": code,
            "page": 1,
            "sdate": date_str,
            "edate": date_str,
            "per": 1,
        }

        try:
            resp = requests.get(_EM_NAV_URL, params=params, headers=_HEADERS, timeout=15)
            if resp.status_code != 200:
                self._error_logger.warning(
                    "NavFetcher HTTP %d: code=%s, date=%s",
                    resp.status_code, code, date_str,
                )
                return None

            html = resp.text
            rows = self._parse_nav_html(html)
            if not rows:
                return None

            df = pd.DataFrame(rows, columns=["日期", "单位净值", "累计净值"])
            df["单位净值"] = pd.to_numeric(df["单位净值"], errors="coerce")
            df["累计净值"] = pd.to_numeric(df["累计净值"], errors="coerce")
            df["symbol"] = code
            return df

        except Exception as exc:
            self._error_logger.error(
                "NavFetcher 失败: code=%s, date=%s, error=%s",
                code, date_str, exc,
            )
            return None

    def fetch_all_nav(self, etf_codes: list[str], trade_date: date) -> pd.DataFrame:
        """批量采集 ETF 净值数据。

        Args:
            etf_codes: 6位数字代码列表
            trade_date: 交易日期
        """
        self.skip_count = 0
        frames: list[pd.DataFrame] = []
        total = len(etf_codes)

        for i, code in enumerate(etf_codes):
            if (i + 1) % 50 == 0 or i == total - 1:
                print(f"\r  [ETF净值] {i+1}/{total} ({(i+1)/total*100:.0f}%)...", end="", flush=True)
            df = self.fetch_single_nav(code, trade_date)
            if df is None:
                self.skip_count += 1
                continue
            frames.append(df)
            time.sleep(0.1)

        print()
        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _parse_nav_html(html: str) -> list[list[str]]:
        """解析东方财富净值页面 HTML，提取净值数据行。"""
        rows: list[list[str]] = []
        pattern = re.compile(
            r"<tr>\s*"
            r"<td>(\d{4}-\d{2}-\d{2})</td>\s*"
            r"<td[^>]*>([\d.]+)</td>\s*"
            r"<td[^>]*>([\d.]+)</td>"
        )
        for match in pattern.finditer(html):
            rows.append([match.group(1), match.group(2), match.group(3)])
        return rows
