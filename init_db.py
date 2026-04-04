"""Initialize layered ClickHouse databases for the orderflow pipeline."""

from __future__ import annotations

import os
from typing import Iterable


RAW_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS raw.trades (
    timestamp    DateTime64(9, 'UTC'),
    receipt_ts   DateTime64(6, 'UTC'),
    exchange     LowCardinality(String),
    symbol       LowCardinality(String),
    side         LowCardinality(String),
    price        Float64,
    quantity     Float64,
    notional     Float64,
    trade_id     Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (exchange, symbol, timestamp)
"""


RAW_BOOK_TOPK_DDL = """
CREATE TABLE IF NOT EXISTS raw.book_topk_raw (
    exchange         LowCardinality(String),
    symbol           LowCardinality(String),
    timestamp        DateTime64(3, 'UTC'),
    receipt_ts       DateTime64(3, 'UTC'),
    mid_price        Float64,
    best_bid         Float64,
    best_ask         Float64,
    spread_bps       Float64,
    microprice       Float64,
    bid_px           Array(Float64),
    bid_sz           Array(Float64),
    ask_px           Array(Float64),
    ask_sz           Array(Float64),
    bid_depth_5      Float64,
    bid_depth_10     Float64,
    ask_depth_5      Float64,
    ask_depth_10     Float64,
    imbalance_top1   Float64,
    imbalance_top5   Float64,
    imbalance_top10  Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (exchange, symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
"""


RAW_LIQUIDATIONS_DDL = """
CREATE TABLE IF NOT EXISTS raw.liquidations (
    timestamp    DateTime64(9, 'UTC'),
    receipt_ts   DateTime64(6, 'UTC'),
    exchange     LowCardinality(String),
    symbol       LowCardinality(String),
    side         LowCardinality(String),
    price        Float64,
    quantity     Float64,
    status       LowCardinality(String),
    order_id     Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (exchange, symbol, timestamp)
"""


RAW_FUNDING_DDL = """
CREATE TABLE IF NOT EXISTS raw.funding (
    timestamp      DateTime64(9, 'UTC'),
    receipt_ts     DateTime64(6, 'UTC'),
    exchange       LowCardinality(String),
    symbol         LowCardinality(String),
    mark_price     Nullable(Float64),
    rate           Float64,
    predicted_rate Nullable(Float64)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (exchange, symbol, timestamp)
"""


RAW_OPEN_INTEREST_DDL = """
CREATE TABLE IF NOT EXISTS raw.open_interest (
    timestamp      DateTime64(9, 'UTC'),
    receipt_ts     DateTime64(6, 'UTC'),
    exchange       LowCardinality(String),
    symbol         LowCardinality(String),
    open_interest  Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (exchange, symbol, timestamp)
"""


SAMPLE_BARS_TIME_DDL = """
CREATE TABLE IF NOT EXISTS sample.bars_time (
    exchange              LowCardinality(String),
    symbol                LowCardinality(String),
    bar_interval_sec      UInt32,
    bar_time              DateTime('UTC'),
    open                  Float64,
    high                  Float64,
    low                   Float64,
    close                 Float64,
    volume                Float64,
    turnover              Float64,
    trade_count           UInt64,
    buy_volume            Float64,
    sell_volume           Float64,
    buy_turnover          Float64,
    sell_turnover         Float64,
    signed_sqrt_dollar_volume Float64 DEFAULT 0,
    vwap                  Float64,
    large_trade_count     UInt64 DEFAULT 0,
    large_trade_volume    Float64 DEFAULT 0,
    ob_snapshot_count     UInt32 DEFAULT 0,
    is_time_bar_complete  UInt8 DEFAULT 1,
    has_book_data         UInt8 DEFAULT 0,
    has_open_interest_data UInt8 DEFAULT 0,
    has_funding_data      UInt8 DEFAULT 0,
    avg_spread_bps        Float64 DEFAULT 0,
    avg_mid_price         Float64 DEFAULT 0,
    avg_microprice        Float64 DEFAULT 0,
    avg_bid1_size         Float64 DEFAULT 0,
    avg_ask1_size         Float64 DEFAULT 0,
    avg_top5_bid_depth    Float64 DEFAULT 0,
    avg_top5_ask_depth    Float64 DEFAULT 0,
    avg_top10_bid_depth   Float64 DEFAULT 0,
    avg_top10_ask_depth   Float64 DEFAULT 0,
    avg_imbalance_top1    Float64 DEFAULT 0,
    avg_imbalance_top5    Float64 DEFAULT 0,
    avg_imbalance_top10   Float64 DEFAULT 0,
    open_interest_open    Float64 DEFAULT 0,
    open_interest_close   Float64 DEFAULT 0,
    funding_rate          Nullable(Float64),
    liq_buy_volume        Float64 DEFAULT 0,
    liq_sell_volume       Float64 DEFAULT 0,
    liq_buy_turnover      Float64 DEFAULT 0,
    liq_sell_turnover     Float64 DEFAULT 0,
    liq_count             UInt32 DEFAULT 0,
    first_trade_ts        DateTime64(3, 'UTC'),
    last_trade_ts         DateTime64(3, 'UTC'),
    max_trade_gap_ms      UInt64 DEFAULT 0,
    _version              UInt64 DEFAULT 1
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY (bar_interval_sec, toYYYYMM(bar_time))
ORDER BY (exchange, symbol, bar_interval_sec, bar_time)
SETTINGS index_granularity = 8192
"""


SAMPLE_BARS_TIME_MIGRATIONS = [
    "ALTER TABLE sample.bars_time ADD COLUMN IF NOT EXISTS signed_sqrt_dollar_volume Float64 DEFAULT 0",
    "ALTER TABLE sample.bars_time ADD COLUMN IF NOT EXISTS is_time_bar_complete UInt8 DEFAULT 1",
    "ALTER TABLE sample.bars_time ADD COLUMN IF NOT EXISTS has_book_data UInt8 DEFAULT 0",
    "ALTER TABLE sample.bars_time ADD COLUMN IF NOT EXISTS has_open_interest_data UInt8 DEFAULT 0",
    "ALTER TABLE sample.bars_time ADD COLUMN IF NOT EXISTS has_funding_data UInt8 DEFAULT 0",
]


FEATURES_TIME_BAR_DDL = """
CREATE TABLE IF NOT EXISTS feature.features_time_bar (
    exchange                LowCardinality(String),
    symbol                  LowCardinality(String),
    bar_interval_sec        UInt32,
    bar_time                DateTime('UTC'),
    log_return              Float64,
    co_return               Float64,
    hl_range                Float64,
    close_position          Float64,
    gap_return              Float64,
    realized_vol_5          Float64,
    realized_vol_20         Float64,
    realized_vol_60         Float64,
    vol_ratio_5_20          Float64,
    volume_zscore_20        Float64,
    volume_ratio_5_20       Float64,
    buy_ratio               Float64,
    delta_volume            Float64,
    avg_trade_size          Float64,
    large_trade_pct         Nullable(Float64),
    close_vwap_bias         Float64,
    spread_bps              Float64,
    imbalance_top1          Float64,
    imbalance_top5          Float64,
    depth_ratio_5           Float64,
    microprice_bias         Float64,
    ob_quality              Float64,
    oi_change               Float64,
    oi_change_pct           Float64,
    oi_zscore_20            Float64,
    funding_rate            Nullable(Float64),
    liq_imbalance           Float64,
    liq_intensity           Float64,
    liq_volume_pct          Float64,
    autocorrelation_5       Float64,
    autocorrelation_20      Float64,
    kyle_lambda_approx      Float64,
    amihud_illiquidity      Float64,
    frac_diff_0_4           Float64,
    frac_diff_0_6           Float64,
    vpin_5                  Float64,
    vpin_10                 Float64,
    vpin_20                 Float64,
    price_acceleration      Float64,
    hour_of_day             UInt8,
    day_of_week             UInt8,
    _version                UInt64 DEFAULT 1
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY (bar_interval_sec, toYYYYMM(bar_time))
ORDER BY (exchange, symbol, bar_interval_sec, bar_time)
"""


FEATURES_TIME_BAR_MIGRATIONS = [
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS vol_ratio_5_20 Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS oi_zscore_20 Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS liq_volume_pct Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS autocorrelation_5 Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS autocorrelation_20 Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS kyle_lambda_approx Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS amihud_illiquidity Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS frac_diff_0_4 Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS frac_diff_0_6 Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS vpin_5 Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS vpin_10 Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS vpin_20 Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar ADD COLUMN IF NOT EXISTS price_acceleration Float64 DEFAULT 0",
    "ALTER TABLE feature.features_time_bar MODIFY COLUMN large_trade_pct Nullable(Float64)",
]


LABELS_TIME_BAR_DDL = """
CREATE TABLE IF NOT EXISTS label.labels_time_bar (
    exchange              LowCardinality(String),
    symbol                LowCardinality(String),
    bar_interval_sec      UInt32,
    bar_time              DateTime('UTC'),
    fwd_return_1          Nullable(Float64),
    fwd_return_5          Nullable(Float64),
    fwd_return_10         Nullable(Float64),
    fwd_return_20         Nullable(Float64),
    fwd_return_60         Nullable(Float64),
    fwd_direction_5       Int8,
    fwd_direction_10      Int8,
    fwd_label_3class_5    Int8,
    fwd_label_3class_10   Int8,
    label_threshold_5     Float64,
    label_threshold_10    Float64,
    fwd_max_up_5          Float64,
    fwd_max_down_5        Float64,
    fwd_realized_vol_5    Nullable(Float64),
    fwd_range_5           Float64,
    mae_5                 Float64,
    mae_10                Float64,
    mfe_5                 Float64,
    mfe_10                Float64,
    triple_barrier_signal Int8,
    is_valid              UInt8 DEFAULT 1,
    _version              UInt64 DEFAULT 1
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY (bar_interval_sec, toYYYYMM(bar_time))
ORDER BY (exchange, symbol, bar_interval_sec, bar_time)
"""


LABELS_TIME_BAR_MIGRATIONS = [
    "ALTER TABLE label.labels_time_bar MODIFY COLUMN fwd_return_1 Nullable(Float64)",
    "ALTER TABLE label.labels_time_bar MODIFY COLUMN fwd_return_5 Nullable(Float64)",
    "ALTER TABLE label.labels_time_bar MODIFY COLUMN fwd_return_10 Nullable(Float64)",
    "ALTER TABLE label.labels_time_bar MODIFY COLUMN fwd_return_20 Nullable(Float64)",
    "ALTER TABLE label.labels_time_bar MODIFY COLUMN fwd_return_60 Nullable(Float64)",
    "ALTER TABLE label.labels_time_bar MODIFY COLUMN fwd_realized_vol_5 Nullable(Float64)",
    "ALTER TABLE label.labels_time_bar ADD COLUMN IF NOT EXISTS triple_barrier_signal Int8 DEFAULT 0",
]


META_SYMBOLS_DDL = """
CREATE TABLE IF NOT EXISTS meta.symbols (
    exchange          LowCardinality(String),
    symbol            LowCardinality(String),
    base_asset        LowCardinality(String),
    quote_asset       LowCardinality(String),
    contract_type     LowCardinality(String),
    tick_size         Float64,
    lot_size          Float64,
    listing_date      Date,
    is_active         UInt8 DEFAULT 1,
    vol_bar_threshold Float64 DEFAULT 0,
    updated_at        DateTime('UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (exchange, symbol)
"""


META_PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id           UUID DEFAULT generateUUIDv4(),
    layer            LowCardinality(String),
    table_name       String,
    bar_interval_sec UInt32,
    symbols          Array(String),
    time_range_start DateTime('UTC'),
    time_range_end   DateTime('UTC'),
    row_count        UInt64,
    status           LowCardinality(String),
    git_commit       String DEFAULT '',
    config_json      String DEFAULT '{}',
    started_at       DateTime('UTC') DEFAULT now(),
    finished_at      Nullable(DateTime('UTC'))
) ENGINE = MergeTree()
ORDER BY (layer, started_at, table_name)
"""


DATABASE_COMMANDS = [
    "CREATE DATABASE IF NOT EXISTS raw",
    "CREATE DATABASE IF NOT EXISTS sample",
    "CREATE DATABASE IF NOT EXISTS feature",
    "CREATE DATABASE IF NOT EXISTS label",
    "CREATE DATABASE IF NOT EXISTS dataset",
    "CREATE DATABASE IF NOT EXISTS meta",
]


TABLE_COMMANDS = [
    RAW_TRADES_DDL,
    RAW_BOOK_TOPK_DDL,
    RAW_LIQUIDATIONS_DDL,
    RAW_FUNDING_DDL,
    RAW_OPEN_INTEREST_DDL,
    SAMPLE_BARS_TIME_DDL,
    *SAMPLE_BARS_TIME_MIGRATIONS,
    FEATURES_TIME_BAR_DDL,
    LABELS_TIME_BAR_DDL,
    *LABELS_TIME_BAR_MIGRATIONS,
    META_SYMBOLS_DDL,
    META_PIPELINE_RUNS_DDL,
    *FEATURES_TIME_BAR_MIGRATIONS,
]


def get_clickhouse_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USERNAME", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def build_schema_commands() -> list[str]:
    return DATABASE_COMMANDS + TABLE_COMMANDS


def bootstrap_schema(commands: Iterable[str] | None = None) -> None:
    client = get_clickhouse_client()
    for command in commands or build_schema_commands():
        client.command(command)


def describe_database(database: str) -> None:
    client = get_clickhouse_client()
    tables = client.command(f"SHOW TABLES FROM {database}")
    print(f"\nDatabase: {database}")
    print("Tables:", tables)


def main() -> None:
    bootstrap_schema()
    for database in ("raw", "sample", "feature", "label", "dataset", "meta"):
        describe_database(database)


if __name__ == "__main__":
    main()
