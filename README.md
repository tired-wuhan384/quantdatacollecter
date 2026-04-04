# Crypto Orderflow Data Collector / 加密货币订单流数据采集器

> **Production-grade orderflow data pipeline — from raw tick data to ML-ready datasets in one click.**
>
> **生产级订单流数据管道 — 从原始逐笔数据到机器学习数据集，一键完成。**

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Latest-orange.svg)](https://clickhouse.com/)
[![Docker](https://img.shields.io/badge/Docker-Ready-green.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 🚀 Why This Project / 为什么选择这个项目

### English

Most crypto data projects give you raw CSV files and say "good luck." This project is different.

**We've built the entire data engineering pipeline so you can focus on what matters — alpha research.**

- 📊 **50+ quantitative features** pre-computed: VPIN, Kyle's Lambda, Amihud illiquidity, fractional differentiation, microprice bias, order book imbalance, and more
- 🏗️ **Production-grade layered architecture**: `raw → sample → feature → label → dataset` — the same pipeline used by professional quant funds
- ⚡ **Real-time collection** from Binance Futures with ClickHouse batch writes — handles millions of events per hour
- 🧠 **ML-ready datasets** with Triple Barrier labels, forward returns, and training-ready joins — export to CSV and start modeling immediately
- 🐳 **One-command Docker deployment** — no infrastructure headaches

**Stop building data pipelines. Start finding alpha.**

### 中文

大多数加密货币数据项目只给你原始 CSV 文件然后说"祝你好运"。这个项目完全不同。

**我们构建了完整的数据工程管道，让你专注于最重要的事情 — Alpha 研究。**

- 📊 **50+ 量化特征**已预计算：VPIN、Kyle's Lambda、Amihud 非流动性、分数阶差分、微观价格偏差、订单簿不平衡等
- 🏗️ **生产级分层架构**：`raw → sample → feature → label → dataset` — 专业量化基金使用的同一套管道
- ⚡ **实时采集** Binance Futures 数据，ClickHouse 批量写入 — 每小时处理数百万事件
- 🧠 **可直接用于机器学习的数据集**：Triple Barrier 标签、前向收益率、训练级关联 — 导出 CSV 即可开始建模
- 🐳 **一条命令 Docker 部署** — 无需基础设施烦恼

**别再造数据管道的轮子了，开始寻找 Alpha 吧。**

---

## 📊 Features & Data Architecture / 功能与数据架构

### Data Collection / 数据采集

| Data Type | 数据类型 | Description / 说明 |
|-----------|---------|-------------------|
| TRADES | 逐笔成交 | Real-time trade executions with price, quantity, side |
| L2_BOOK | L2 订单簿 | Top-K order book snapshots with depth metrics |
| LIQUIDATIONS | 爆仓/清算 | Forced liquidation events |
| FUNDING | 资金费率 | Funding rate history |
| OPEN_INTEREST | 持仓量 | Open interest over time |

### Pre-computed Features / 预计算特征 (50+)

> **These features are ready to use for research — no additional processing needed.**
>
> **这些特征可直接用于研究 — 无需额外处理。**

| Category / 类别 | Features / 特征 |
|----------------|----------------|
| **Price / 价格** | log_return, co_return, hl_range, close_position, gap_return, price_acceleration |
| **Volatility / 波动率** | realized_vol_5/20/60, vol_ratio_5_20, frac_diff_0.4/0.6 |
| **Volume / 成交量** | volume_zscore_20, volume_ratio_5_20, buy_ratio, delta_volume, avg_trade_size |
| **Order Flow / 订单流** | VPIN_5/10/20, signed_sqrt_dollar_volume, kyle_lambda_approx, amihud_illiquidity |
| **Order Book / 订单簿** | spread_bps, imbalance_top1/5/10, microprice_bias, depth_ratio_5, ob_quality |
| **Open Interest / 持仓量** | oi_change, oi_change_pct, oi_zscore_20 |
| **Liquidation / 爆仓** | liq_imbalance, liq_intensity, liq_volume_pct |
| **Time / 时间** | hour_of_day, day_of_week |
| **Autocorrelation / 自相关** | autocorrelation_5/20 |

### Labels for ML / 机器学习标签

| Label / 标签 | Description / 说明 |
|-------------|-------------------|
| fwd_return_1/5/10/20/60 | Forward returns at multiple horizons |
| fwd_label_3class_5/10 | 3-class classification (up/down/sideways) |
| triple_barrier_signal | Triple Barrier Method labels |
| mae_5/10, mfe_5/10 | Maximum Adverse/Favorable Excursion |
| label_threshold_5/10 | Volatility-adaptive thresholds |

### Data Pipeline Architecture / 数据管道架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Binance Futures                              │
│                   (Real-time WebSocket Feed)                        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  RAW Layer / 原始层                                                  │
│  trades | book_topk_raw | liquidations | funding | open_interest    │
│  ← Millions of events per hour, partitioned by day                  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SAMPLE Layer / 采样层                                               │
│  bars_time (60s, 300s, 900s, 1800s, 3600s, 14400s, 86400s)         │
│  ← OHLCV + order book stats + OI + funding + liquidations per bar   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  FEATURE Layer / 特征层                                              │
│  features_time_bar — 50+ quantitative features                       │
│  ← Rolling windows, fractional diff, VPIN, Kyle's Lambda, etc.      │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LABEL Layer / 标签层                                                │
│  labels_time_bar — Forward returns, 3-class, Triple Barrier         │
│  ← Volatility-adaptive thresholds, MAE/MFE                         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DATASET Layer / 数据集层                                            │
│  dataset_time_bar — JOIN of features + labels, training-ready       │
│  ← Export to CSV → Feed into your ML model                          │
└─────────────────────────────────────────────────────────────────────┘



```

---

## ⚡ Quick Start / 快速开始

### Option 1: Docker (Recommended) / 方式一：Docker（推荐）

```bash
# 1. Clone / 克隆
git clone <your-repo-url>
cd quantdatacollecter

# 2. Configure / 配置
cp .env.example .env
# Edit .env with your ClickHouse password / 编辑 .env 设置密码

# 3. Start / 启动
docker-compose up -d

# 4. Initialize DB / 初始化数据库
docker-compose exec collector python init_db.py

# 5. View logs / 查看日志
docker-compose logs -f collector
```

### Option 2: Local / 方式二：本地部署

```bash
# 1. Install deps / 安装依赖
pip install -r requirements.txt

# 2. Install ClickHouse / 安装 ClickHouse
# See: https://clickhouse.com/docs/en/install

# 3. Configure / 配置
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USERNAME=default
export CLICKHOUSE_PASSWORD=your_password
export COLLECTOR_SYMBOLS=BTC-USDT-PERP,ETH-USDT-PERP

# 4. Initialize DB / 初始化数据库
python init_db.py

# 5. Start collector / 启动采集器
python collect.py
```

### Run Pipeline / 运行数据处理管道

```bash
# Backfill historical data / 回补历史数据
python -m pipelines.backfill_time_bar_pipeline \
  --intervals 60,300,900 \
  --start "2024-01-01" \
  --end "2024-12-31"

# Incremental update (auto-detects last position) / 增量更新（自动检测上次位置）
python -m pipelines.incremental_backfill_time_bar_pipeline \
  --intervals 60,300,900

# Export training dataset to CSV / 导出训练数据集到 CSV
python -m pipelines.dataset_view \
  --interval 60 \
  --output dataset_train.csv
```

---

## 🔧 Configuration / 配置

### Environment Variables / 环境变量

| Variable / 变量 | Description / 说明 | Default / 默认值 |
|----------------|-------------------|-----------------|
| `CLICKHOUSE_HOST` | ClickHouse address / 地址 | `localhost` |
| `CLICKHOUSE_PORT` | ClickHouse HTTP port / HTTP 端口 | `8123` |
| `CLICKHOUSE_USERNAME` | ClickHouse username / 用户名 | `default` |
| `CLICKHOUSE_PASSWORD` | ClickHouse password / 密码 | *(empty)* |
| `COLLECTOR_SYMBOLS` | Trading pairs / 交易对（逗号分隔） | `BTC-USDT-PERP,ETH-USDT-PERP` |
| `HTTP_PROXY` | HTTP proxy (optional) / 代理（可选） | *(empty)* |
| `HTTPS_PROXY` | HTTPS proxy (optional) / 代理（可选） | *(empty)* |

### Custom Symbols / 自定义交易对

```bash
# Add more symbols / 添加更多交易对
export COLLECTOR_SYMBOLS=BTC-USDT-PERP,ETH-USDT-PERP,SOL-USDT-PERP,BNB-USDT-PERP
```

---

## 📁 Project Structure / 项目结构

```
quantdatacollecter/
├── collect.py                              # Main collector / 主采集程序
├── init_db.py                              # DB initialization / 数据库初始化
├── backends/
│   └── clickhouse.py                       # ClickHouse write backend / 写入后端
├── pipelines/
│   ├── sample_time_bars.py                 # Time bar aggregation / 时间条采样
│   ├── features_time_bar.py                # Feature engineering / 特征工程
│   ├── labels_time_bar.py                  # Label generation / 标签生成
│   ├── dataset_view.py                     # Dataset view / 数据集视图
│   ├── backfill_time_bar_pipeline.py       # Full backfill / 全量回补
│   ├── incremental_backfill_time_bar_pipeline.py  # Incremental / 增量回补
│   ├── common.py                           # Shared helpers / 共享工具
│   └── repair_pipeline_runs.py             # Repair stale runs / 修复卡住任务
├── tests/                                  # Unit tests / 单元测试
├── docker-compose.yml                      # Docker orchestration / Docker 编排
├── Dockerfile                              # Docker image / Docker 镜像
├── requirements.txt                        # Python deps / 依赖
├── .env.example                            # Env template / 环境变量模板
└── README.md                               # This file / 本文件
```

---

## 🧪 Testing / 测试

```bash
pytest tests/ -v
```

---

## 📊 Database Schema / 数据库表结构

### Raw Layer / 原始层
- `raw.trades` — Trade executions / 逐笔成交
- `raw.book_topk_raw` — Order book snapshots / 订单簿快照
- `raw.liquidations` — Liquidation events / 爆仓记录
- `raw.funding` — Funding rates / 资金费率
- `raw.open_interest` — Open interest / 持仓量

### Sample Layer / 采样层
- `sample.bars_time` — Time bar aggregated data (multi-timeframe) / 时间条聚合数据

### Feature Layer / 特征层
- `feature.features_time_bar` — 50+ quantitative features / 量化特征

### Label Layer / 标签层
- `label.labels_time_bar` — Prediction labels / 预测标签

### Meta Layer / 元数据层
- `meta.symbols` — Symbol metadata / 交易对元数据
- `meta.pipeline_runs` — Pipeline run records / 管道运行记录

---

## ⚠️ Notes / 注意事项

1. **Proxy / 代理**: Users in restricted regions need proxy to access Binance API / 中国大陆用户需配置代理
2. **Disk Space / 磁盘空间**: High-frequency data requires significant storage / 高频数据占用较大空间
3. **ClickHouse Version / 版本**: Latest version recommended / 建议使用最新版本
4. **Data Retention / 数据保留**: Order book snapshots have 90-day TTL by default / 订单簿快照默认保留 90 天

---

## 📝 License / 许可证

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

**Built for quants, by quants.** / **为量化研究者打造。**

⭐ Star this repo if you find it useful! / 如果觉得有用，请点个 Star！

</div>
