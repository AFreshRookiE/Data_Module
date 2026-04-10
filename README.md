# ETF 数据管道系统

全市场 ETF 日线行情数据的自动采集、清洗、存储与可视化工具。

---

## 功能概览

| 功能 | 说明 |
|------|------|
| 每日数据采集 | 每个交易日 18:00 自动从 baostock 拉取全市场 ETF 日线数据 |
| 历史数据回填 | 通过腾讯财经接口回填 2016 年至今的 ETF 历史数据 |
| 数据清洗 | 按 C1-C4 规则自动过滤无效记录 |
| 数据存储 | 写入 DolphinDB 时序数据库（TSDB 引擎，按月分区） |
| K 线图生成 | 批量生成 1035 只 ETF 的日线 K 线图，保存到桌面 |

---

## 环境要求

- Python 3.10+
- DolphinDB 3.00+（已启动，监听 localhost:8848）
- Windows 操作系统

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 DolphinDB 连接

编辑 `config.yaml`，确认数据库连接信息：

```yaml
dolphindb:
  host: localhost
  port: 8848
  username: admin
  password: "123456"
```

### 3. 初始化数据库

首次使用时，运行管道会自动创建数据库和表。也可以手动初始化：

```bash
python -c "from etf_pipeline.db_initializer import DBInitializer; from etf_pipeline.config import load_config; DBInitializer(load_config()).initialize()"
```

### 4. 回填历史数据

将 2016 年至今的 ETF 日线数据写入 DolphinDB：

```bash
python backfill_history.py
```

- 支持断点续传，中断后重新运行会自动跳过已完成的 ETF
- 进度记录在 `backfill_progress.txt`，如需重新回填请先删除此文件
- 预计耗时约 30-40 分钟

### 5. 每日数据拉取

**方式一：双击运行**

直接双击 `ETF数据拉取.bat`，会立即执行一次数据拉取。

**方式二：命令行运行**

```bash
python run_once.py
```

**方式三：定时调度模式**

```bash
python -m etf_pipeline.main
```

程序会在每个交易日 18:00 自动拉取数据，持续运行。

### 6. 生成 K 线图

批量生成所有 ETF 的日线 K 线图，保存到桌面的 `ETF_K线图` 文件夹：

```bash
python generate_kline.py
```

- 文件命名格式：`ETF名称-设立日期-最后交易日期.png`
- 红色阳线、绿色阴线（A 股惯例）
- 包含成交量柱状图
- 预计耗时约 15 分钟

---

## 项目结构

```
AKShare_Module/
├── config.yaml              # 配置文件（DolphinDB 连接、调度时间等）
├── requirements.txt         # Python 依赖
├── run_once.py              # 手动触发入口（双击运行）
├── backfill_history.py      # 历史数据回填脚本
├── generate_kline.py        # K 线图批量生成脚本
├── build_exe.py             # PyInstaller 打包脚本
├── ETF数据拉取.bat            # 一键运行批处理
├── ETF数据拉取.spec           # PyInstaller 打包配置
│
├── etf_pipeline/            # 核心管道模块
│   ├── main.py              # 定时调度入口
│   ├── config.py            # 配置加载（pydantic-settings）
│   ├── scheduler.py         # 定时调度器（APScheduler）
│   ├── task_runner.py       # 任务编排（串联各模块）
│   ├── fetcher.py           # 数据采集（baostock）
│   ├── cleaner.py           # 数据清洗（C1-C4 规则）
│   ├── transformer.py       # 格式转换（列名、symbol 标准化）
│   ├── writer.py            # 数据写入（DolphinDB upsert）
│   ├── db_initializer.py    # 数据库初始化（建库建表）
│   ├── logger.py            # 日志管理
│   └── models.py            # 数据模型（CleanSummary）
│
└── logs/                    # 运行日志（自动创建）
    ├── run/                 # 运行日志（按日滚动）
    └── error/               # 错误日志（按日滚动）
```

---

## 配置说明

`config.yaml` 完整配置项：

```yaml
dolphindb:
  host: localhost            # DolphinDB 地址
  port: 8848                 # DolphinDB 端口
  username: admin            # 用户名
  password: "123456"         # 密码

schedule_time: "18:00"       # 每日调度时间
fetch_retry_times: 3         # 数据采集重试次数
fetch_retry_interval: 10     # 数据采集重试间隔（秒）
db_retry_times: 3            # 数据库写入重试次数
db_retry_interval: 15        # 数据库写入重试间隔（秒）
run_log_dir: logs/run        # 运行日志目录
error_log_dir: logs/error    # 错误日志目录
log_retention_days: 30       # 日志保留天数
db_path: dfs://etf_db        # DolphinDB 数据库路径
table_name: etf_daily        # DolphinDB 表名
```

---

## 数据库表结构

DolphinDB 中的 `etf_daily` 表：

| 字段 | 类型 | 说明 |
|------|------|------|
| date | DATE | 交易日期 |
| symbol | SYMBOL | ETF 代码（如 510300.SH） |
| open | DOUBLE | 开盘价 |
| high | DOUBLE | 最高价 |
| low | DOUBLE | 最低价 |
| close | DOUBLE | 收盘价 |
| volume | LONG | 成交量 |
| amount | DOUBLE | 成交额 |

- 数据库引擎：TSDB
- 分区方式：按月分区（2016.01M ~ 2030.12M）
- 排序列：symbol + date
- 写入方式：upsert（按 date + symbol 去重）

---

## 数据清洗规则

| 规则 | 说明 |
|------|------|
| C1 | 任意价格字段（open/high/low/close）为空或非正数 → 丢弃 |
| C2 | 最高价 < 最低价 → 丢弃 |
| C3 | 成交量或成交额为负数 → 丢弃 |
| C4 | symbol 为空或格式无效（非 6 位纯数字）→ 丢弃 |

被丢弃的记录会写入错误日志，包含 symbol、日期、规则编号和原始值。

---

## 数据源说明

| 用途 | 数据源 | 说明 |
|------|--------|------|
| 每日增量采集 | baostock | 稳定可靠，免费，无需注册 |
| 历史数据回填 | 腾讯财经 | 支持 2016 年至今的完整 OHLCV 数据，前复权 |
| ETF 元数据 | baostock | ETF 名称、设立日期、结束日期 |

> 注意：腾讯财经接口返回的是前复权数据，成交额字段为 0（接口不提供）。

---

## 常见问题

### 1. 运行提示"baostock 登录失败"

baostock 需要访问互联网，请检查网络连接。如果网络正常但仍失败，可能是 baostock 服务器临时不可用，稍后重试即可。

### 2. 运行提示"DolphinDB 连接失败"

请确认：
- DolphinDB 服务已启动
- 端口号正确（默认 8848）
- `config.yaml` 中的用户名和密码正确

### 3. 历史回填中断了怎么办？

直接重新运行 `python backfill_history.py`，脚本会自动跳过已完成的 ETF，从中断处继续。

如需完全重新回填，先删除 `backfill_progress.txt`，再重建数据库：

```bash
python -c "
import dolphindb as ddb
s = ddb.Session()
s.connect('localhost', 8848, 'admin', '123456')
if s.run(\"existsDatabase('dfs://etf_db')\"):
    s.run(\"dropDatabase('dfs://etf_db')\")
s.run('''
db = database(\"dfs://etf_db\", VALUE, 2016.01M..2030.12M, engine=\"TSDB\")
schema = table(1:0, \`date\`symbol\`open\`high\`low\`close\`volume\`amount, [DATE,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,LONG,DOUBLE])
db.createPartitionedTable(schema, \"etf_daily\", partitionColumns=\`date, sortColumns=\`symbol\`date, keepDuplicates=LAST)
''')
print('数据库重建完成')
s.close()
"
```

### 4. K 线图中文显示为方框

脚本已配置 SimHei（黑体）字体。如果仍显示异常，说明系统缺少该字体，可安装微软雅黑或其他中文字体后修改 `generate_kline.py` 中的字体配置。

### 5. 如何打包为 exe？

```bash
python build_exe.py
```

打包后的文件在 `dist/` 目录下。

---

## 技术栈

| 组件 | 技术 |
|------|------|
| 数据采集 | baostock、腾讯财经 API |
| 数据清洗 | pandas |
| 数据存储 | DolphinDB（TSDB 引擎） |
| 定时调度 | APScheduler |
| 配置管理 | pydantic-settings + YAML |
| 重试机制 | tenacity |
| K 线图 | mplfinance + matplotlib |
| 交易日判断 | chinesecalendar |
| 打包 | PyInstaller |
