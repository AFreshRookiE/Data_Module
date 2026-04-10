"""
数据模型模块。

定义管道内部使用的数据传输对象（dataclass）。
"""

__all__ = ["CleanSummary", "WriteResult"]

from dataclasses import dataclass, field


@dataclass
class CleanSummary:
    """数据清洗汇总信息。

    记录 DataCleaner.clean() 执行后的统计结果，
    用于 TaskRunner 写入运行日志和质量评估。

    Attributes:
        input_count: 清洗前输入的记录总数。
        pass_count:  通过全部清洗规则的记录数。
        drop_count:  被任意规则丢弃的记录数。
                     恒满足：input_count == pass_count + drop_count。
        rule_counts: 各规则触发次数，键固定为 C1–C4。
                     同一条记录只被最先匹配的规则计数一次，
                     因此各值之和 <= drop_count。
                     - C1: 价格字段为空或非正值
                     - C2: high < low（最高价低于最低价）
                     - C3: 成交量或成交额为负
                     - C4: symbol 格式无效
    """

    input_count: int
    pass_count: int
    drop_count: int
    rule_counts: dict[str, int] = field(
        default_factory=lambda: {"C1": 0, "C2": 0, "C3": 0, "C4": 0}
    )


@dataclass
class WriteResult:
    """数据写入结果汇总。

    记录 DataWriter.write() 执行后的统计结果，
    用于 TaskRunner 写入运行日志和触发质量警告。

    Attributes:
        success_count:       成功 upsert 到 DolphinDB 的记录数。
        skip_count:          DataFetcher 阶段因接口失败而跳过的 ETF 数量。
        drop_count:          DataCleaner 阶段被清洗规则丢弃的记录数。
        has_quality_warning: 当成功率 < 90% 时为 True，表示本次运行存在数据质量问题。
                             成功率 = success_count / (success_count + drop_count)，
                             skip_count 不计入分母。
    """

    success_count: int
    skip_count: int
    drop_count: int
    has_quality_warning: bool
