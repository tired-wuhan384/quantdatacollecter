"""Build and run time-bar sampling SQL."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pipelines.common import execute_pipeline_sql, get_client


VALID_INTERVALS = {60, 300, 900, 1800, 3600, 14400, 86400}
SAMPLE_BARS_TIME_INSERT_COLUMNS = """
    exchange,
    symbol,
    bar_interval_sec,
    bar_time,
    open,
    high,
    low,
    close,
    volume,
    turnover,
    trade_count,
    buy_volume,
    sell_volume,
    buy_turnover,
    sell_turnover,
    signed_sqrt_dollar_volume,
    vwap,
    large_trade_count,
    large_trade_volume,
    ob_snapshot_count,
    is_time_bar_complete,
    has_book_data,
    has_open_interest_data,
    has_funding_data,
    avg_spread_bps,
    avg_mid_price,
    avg_microprice,
    avg_bid1_size,
    avg_ask1_size,
    avg_top5_bid_depth,
    avg_top5_ask_depth,
    avg_top10_bid_depth,
    avg_top10_ask_depth,
    avg_imbalance_top1,
    avg_imbalance_top5,
    avg_imbalance_top10,
    open_interest_open,
    open_interest_close,
    funding_rate,
    liq_buy_volume,
    liq_sell_volume,
    liq_buy_turnover,
    liq_sell_turnover,
    liq_count,
    first_trade_ts,
    last_trade_ts,
    max_trade_gap_ms,
    _version
""".strip()


def _normalize_timestamp_literal(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _parse_utc_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _build_time_condition(column: str, start: str | None, end: str | None) -> str:
    conditions: list[str] = []
    if start:
        start_literal = _normalize_timestamp_literal(start)
        conditions.append(f"{column} >= toDateTime64('{start_literal}', 3, 'UTC')")
    if end:
        end_literal = _normalize_timestamp_literal(end)
        conditions.append(f"{column} < toDateTime64('{end_literal}', 3, 'UTC')")
    return " AND ".join(conditions) if conditions else "1"


def _build_time_grid_context(start: str | None, end: str | None, interval_sec: int) -> dict[str, str | int] | None:
    if not start or not end:
        return None
    start_dt = _parse_utc_datetime(start)
    end_dt = _parse_utc_datetime(end)
    bar_count = max(int((end_dt - start_dt).total_seconds() // interval_sec), 0)
    return {
        "start_literal": _normalize_timestamp_literal(start),
        "bar_count": bar_count,
    }


def _validate_interval_sec(interval_sec: int) -> None:
    if interval_sec not in VALID_INTERVALS:
        supported = ", ".join(str(value) for value in sorted(VALID_INTERVALS))
        raise ValueError(f"Unsupported interval: {interval_sec}s. Valid: {supported}")
    if interval_sec > 60 and interval_sec % 60 != 0:
        raise ValueError(f"Rollup interval must be a multiple of 60, got {interval_sec}")


def build_trade_bar_insert_sql(
    interval_sec: int = 60,
    start: str | None = None,
    end: str | None = None,
) -> str:
    _validate_interval_sec(interval_sec)
    trade_condition = _build_time_condition("timestamp", start, end)
    book_condition = _build_time_condition("timestamp", start, end)
    oi_condition = _build_time_condition("timestamp", start, end)
    funding_condition = _build_time_condition("timestamp", start, end)
    liq_condition = _build_time_condition("timestamp", start, end)
    grid_context = _build_time_grid_context(start, end, interval_sec)
    if grid_context:
        grid_ctes = f""",
symbols AS (
    SELECT DISTINCT
        exchange,
        symbol
    FROM raw.trades
    WHERE {trade_condition}
),
time_grid AS (
    SELECT
        exchange,
        symbol,
        toDateTime(
            toUnixTimestamp(toDateTime64('{grid_context["start_literal"]}', 3, 'UTC')) + number * {interval_sec},
            'UTC'
        ) AS bar_time
    FROM symbols
    CROSS JOIN numbers({grid_context["bar_count"]})
),
trade_bars_ordered AS (
    SELECT
        exchange,
        symbol,
        bar_time,
        close
    FROM trade_bars
    ORDER BY exchange, symbol, bar_time
),
grid_with_prev_close AS (
    SELECT
        g.exchange,
        g.symbol,
        g.bar_time,
        prev_t.close AS prev_close
    FROM time_grid AS g
    ASOF LEFT JOIN trade_bars_ordered AS prev_t
        ON g.exchange = prev_t.exchange
        AND g.symbol = prev_t.symbol
        AND prev_t.bar_time <= g.bar_time
)"""
        base_source = """
FROM grid_with_prev_close AS g
LEFT JOIN trade_bars AS t
    ON g.exchange = t.exchange
    AND g.symbol = t.symbol
    AND g.bar_time = t.bar_time
LEFT JOIN book_bars AS b
    ON g.exchange = b.exchange
    AND g.symbol = b.symbol
    AND g.bar_time = b.bar_time
LEFT JOIN oi_bars AS oi
    ON g.exchange = oi.exchange
    AND g.symbol = oi.symbol
    AND g.bar_time = oi.bar_time
LEFT JOIN funding_bars AS f
    ON g.exchange = f.exchange
    AND g.symbol = f.symbol
    AND g.bar_time = f.bar_time
LEFT JOIN liquidation_bars AS l
    ON g.exchange = l.exchange
    AND g.symbol = l.symbol
    AND g.bar_time = l.bar_time
WHERE ifNull(t.trade_count, 0) > 0 OR g.prev_close IS NOT NULL
"""
        key_select = f"""
    g.exchange,
    g.symbol,
    {interval_sec} AS bar_interval_sec,
    g.bar_time,
    ifNull(t.open, g.prev_close) AS open,
    ifNull(t.high, g.prev_close) AS high,
    ifNull(t.low, g.prev_close) AS low,
    ifNull(t.close, g.prev_close) AS close,
"""
        time_quality_select = """
    if(ifNull(t.trade_count, 0) > 0 OR g.prev_close IS NOT NULL, 1, 0) AS is_time_bar_complete,
"""
        trade_vwap_select = "ifNull(t.vwap, ifNull(t.close, g.prev_close)) AS vwap,"
        trade_meta_select = """
    ifNull(t.first_trade_ts, toDateTime64(g.bar_time, 3, 'UTC')) AS first_trade_ts,
    ifNull(t.last_trade_ts, toDateTime64(g.bar_time, 3, 'UTC')) AS last_trade_ts,
"""
    else:
        grid_ctes = ""
        base_source = """
FROM trade_bars AS t
LEFT JOIN book_bars AS b
    ON t.exchange = b.exchange
    AND t.symbol = b.symbol
    AND t.bar_time = b.bar_time
LEFT JOIN oi_bars AS oi
    ON t.exchange = oi.exchange
    AND t.symbol = oi.symbol
    AND t.bar_time = oi.bar_time
LEFT JOIN funding_bars AS f
    ON t.exchange = f.exchange
    AND t.symbol = f.symbol
    AND t.bar_time = f.bar_time
LEFT JOIN liquidation_bars AS l
    ON t.exchange = l.exchange
    AND t.symbol = l.symbol
    AND t.bar_time = l.bar_time
"""
        key_select = f"""
    t.exchange,
    t.symbol,
    {interval_sec} AS bar_interval_sec,
    t.bar_time,
    t.open,
    t.high,
    t.low,
    t.close,
"""
        time_quality_select = """
    1 AS is_time_bar_complete,
"""
        trade_vwap_select = "t.vwap AS vwap,"
        trade_meta_select = """
    t.first_trade_ts,
    t.last_trade_ts,
"""
    return f"""
INSERT INTO sample.bars_time (
    {SAMPLE_BARS_TIME_INSERT_COLUMNS}
)
WITH
trade_rows AS (
    SELECT
        exchange,
        symbol,
        timestamp,
        toStartOfInterval(timestamp, INTERVAL {interval_sec} SECOND) AS bar_time,
        price,
        quantity,
        notional,
        side,
        lagInFrame(timestamp, 1, timestamp) OVER (
            PARTITION BY exchange, symbol, toStartOfInterval(timestamp, INTERVAL {interval_sec} SECOND)
            ORDER BY timestamp
        ) AS prev_trade_ts
    FROM raw.trades
    WHERE {trade_condition}
),
trade_bars AS (
    SELECT
        exchange,
        symbol,
        bar_time,
        argMin(price, timestamp) AS open,
        max(price) AS high,
        min(price) AS low,
        argMax(price, timestamp) AS close,
        sum(quantity) AS volume,
        sum(notional) AS turnover,
        count() AS trade_count,
        sumIf(quantity, side = 'buy') AS buy_volume,
        sumIf(quantity, side = 'sell') AS sell_volume,
        sumIf(notional, side = 'buy') AS buy_turnover,
        sumIf(notional, side = 'sell') AS sell_turnover,
        sumIf(sqrt(notional), side = 'buy') - sumIf(sqrt(notional), side = 'sell') AS signed_sqrt_dollar_volume,
        sum(notional) / nullIf(sum(quantity), 0) AS vwap,
        0 AS large_trade_count,
        0.0 AS large_trade_volume,
        min(timestamp) AS first_trade_ts,
        max(timestamp) AS last_trade_ts,
        max(dateDiff('millisecond', prev_trade_ts, timestamp)) AS max_trade_gap_ms
    FROM trade_rows
    GROUP BY exchange, symbol, bar_time
),
book_bars AS (
    SELECT
        exchange,
        symbol,
        toStartOfInterval(timestamp, INTERVAL {interval_sec} SECOND) AS bar_time,
        count() AS ob_snapshot_count,
        avg(spread_bps) AS avg_spread_bps,
        avg(mid_price) AS avg_mid_price,
        avg(microprice) AS avg_microprice,
        avg(bid_sz[1]) AS avg_bid1_size,
        avg(ask_sz[1]) AS avg_ask1_size,
        avg(bid_depth_5) AS avg_top5_bid_depth,
        avg(ask_depth_5) AS avg_top5_ask_depth,
        avg(bid_depth_10) AS avg_top10_bid_depth,
        avg(ask_depth_10) AS avg_top10_ask_depth,
        avg(imbalance_top1) AS avg_imbalance_top1,
        avg(imbalance_top5) AS avg_imbalance_top5,
        avg(imbalance_top10) AS avg_imbalance_top10
    FROM raw.book_topk_raw
    WHERE {book_condition}
    GROUP BY exchange, symbol, bar_time
),
oi_bars AS (
    SELECT
        exchange,
        symbol,
        toStartOfInterval(timestamp, INTERVAL {interval_sec} SECOND) AS bar_time,
        argMin(open_interest, timestamp) AS open_interest_open,
        argMax(open_interest, timestamp) AS open_interest_close
    FROM raw.open_interest
    WHERE {oi_condition}
    GROUP BY exchange, symbol, bar_time
),
funding_bars AS (
    SELECT
        exchange,
        symbol,
        toStartOfInterval(timestamp, INTERVAL {interval_sec} SECOND) AS bar_time,
        argMax(rate, timestamp) AS funding_rate
    FROM raw.funding
    WHERE {funding_condition}
    GROUP BY exchange, symbol, bar_time
),
liquidation_bars AS (
    SELECT
        exchange,
        symbol,
        toStartOfInterval(timestamp, INTERVAL {interval_sec} SECOND) AS bar_time,
        sumIf(quantity, side = 'buy') AS liq_buy_volume,
        sumIf(quantity, side = 'sell') AS liq_sell_volume,
        sumIf(price * quantity, side = 'buy') AS liq_buy_turnover,
        sumIf(price * quantity, side = 'sell') AS liq_sell_turnover,
        count() AS liq_count
    FROM raw.liquidations
    WHERE {liq_condition}
    GROUP BY exchange, symbol, bar_time
){grid_ctes}
SELECT
    {key_select.strip()}
    ifNull(t.volume, 0.0) AS volume,
    ifNull(t.turnover, 0.0) AS turnover,
    ifNull(t.trade_count, 0) AS trade_count,
    ifNull(t.buy_volume, 0.0) AS buy_volume,
    ifNull(t.sell_volume, 0.0) AS sell_volume,
    ifNull(t.buy_turnover, 0.0) AS buy_turnover,
    ifNull(t.sell_turnover, 0.0) AS sell_turnover,
    ifNull(t.signed_sqrt_dollar_volume, 0.0) AS signed_sqrt_dollar_volume,
    {trade_vwap_select}
    ifNull(t.large_trade_count, 0) AS large_trade_count,
    ifNull(t.large_trade_volume, 0.0) AS large_trade_volume,
    ifNull(b.ob_snapshot_count, 0) AS ob_snapshot_count,
    {time_quality_select.strip()}
    if(ifNull(b.ob_snapshot_count, 0) > 0, 1, 0) AS has_book_data,
    if(isNull(oi.bar_time), 0, 1) AS has_open_interest_data,
    if(isNull(f.bar_time), 0, 1) AS has_funding_data,
    ifNull(b.avg_spread_bps, 0.0) AS avg_spread_bps,
    ifNull(b.avg_mid_price, 0.0) AS avg_mid_price,
    ifNull(b.avg_microprice, 0.0) AS avg_microprice,
    ifNull(b.avg_bid1_size, 0.0) AS avg_bid1_size,
    ifNull(b.avg_ask1_size, 0.0) AS avg_ask1_size,
    ifNull(b.avg_top5_bid_depth, 0.0) AS avg_top5_bid_depth,
    ifNull(b.avg_top5_ask_depth, 0.0) AS avg_top5_ask_depth,
    ifNull(b.avg_top10_bid_depth, 0.0) AS avg_top10_bid_depth,
    ifNull(b.avg_top10_ask_depth, 0.0) AS avg_top10_ask_depth,
    ifNull(b.avg_imbalance_top1, 0.0) AS avg_imbalance_top1,
    ifNull(b.avg_imbalance_top5, 0.0) AS avg_imbalance_top5,
    ifNull(b.avg_imbalance_top10, 0.0) AS avg_imbalance_top10,
    ifNull(oi.open_interest_open, 0.0) AS open_interest_open,
    ifNull(oi.open_interest_close, 0.0) AS open_interest_close,
    f.funding_rate AS funding_rate,
    ifNull(l.liq_buy_volume, 0.0) AS liq_buy_volume,
    ifNull(l.liq_sell_volume, 0.0) AS liq_sell_volume,
    ifNull(l.liq_buy_turnover, 0.0) AS liq_buy_turnover,
    ifNull(l.liq_sell_turnover, 0.0) AS liq_sell_turnover,
    ifNull(l.liq_count, 0) AS liq_count,
    {trade_meta_select.strip()}
    ifNull(t.max_trade_gap_ms, 0) AS max_trade_gap_ms,
    toUInt64(toUnixTimestamp(now())) AS _version
{base_source}
"""


def build_rollup_insert_sql(
    interval_sec: int,
    start: str | None = None,
    end: str | None = None,
) -> str:
    _validate_interval_sec(interval_sec)
    source_multiple = interval_sec // 60
    bar_time_condition = _build_time_condition("bar_time", start, end)
    return f"""
INSERT INTO sample.bars_time (
    {SAMPLE_BARS_TIME_INSERT_COLUMNS}
)
SELECT
    exchange,
    symbol,
    {interval_sec} AS bar_interval_sec,
    rollup_bar_time AS bar_time,
    open_value AS open,
    high_value AS high,
    low_value AS low,
    close_value AS close,
    volume_sum AS volume,
    turnover_sum AS turnover,
    trade_count_sum AS trade_count,
    buy_volume_sum AS buy_volume,
    sell_volume_sum AS sell_volume,
    buy_turnover_sum AS buy_turnover,
    sell_turnover_sum AS sell_turnover,
    signed_sqrt_dollar_volume_sum AS signed_sqrt_dollar_volume,
    turnover_sum / nullIf(volume_sum, 0) AS vwap,
    large_trade_count_sum AS large_trade_count,
    large_trade_volume_sum AS large_trade_volume,
    ob_snapshot_count_sum AS ob_snapshot_count,
    is_time_bar_complete_value AS is_time_bar_complete,
    has_book_data_value AS has_book_data,
    has_open_interest_data_value AS has_open_interest_data,
    has_funding_data_value AS has_funding_data,
    avg_spread_bps_value AS avg_spread_bps,
    avg_mid_price_value AS avg_mid_price,
    avg_microprice_value AS avg_microprice,
    avg_bid1_size_value AS avg_bid1_size,
    avg_ask1_size_value AS avg_ask1_size,
    avg_top5_bid_depth_value AS avg_top5_bid_depth,
    avg_top5_ask_depth_value AS avg_top5_ask_depth,
    avg_top10_bid_depth_value AS avg_top10_bid_depth,
    avg_top10_ask_depth_value AS avg_top10_ask_depth,
    avg_imbalance_top1_value AS avg_imbalance_top1,
    avg_imbalance_top5_value AS avg_imbalance_top5,
    avg_imbalance_top10_value AS avg_imbalance_top10,
    open_interest_open_value AS open_interest_open,
    open_interest_close_value AS open_interest_close,
    funding_rate_value AS funding_rate,
    liq_buy_volume_sum AS liq_buy_volume,
    liq_sell_volume_sum AS liq_sell_volume,
    liq_buy_turnover_sum AS liq_buy_turnover,
    liq_sell_turnover_sum AS liq_sell_turnover,
    liq_count_sum AS liq_count,
    first_trade_ts_value AS first_trade_ts,
    last_trade_ts_value AS last_trade_ts,
    max_trade_gap_ms_value AS max_trade_gap_ms,
    toUInt64(toUnixTimestamp(now())) AS _version
FROM (
    SELECT
        exchange,
        symbol,
        toStartOfInterval(bar_time, INTERVAL {interval_sec} SECOND) AS rollup_bar_time,
        argMin(open, bar_time) AS open_value,
        max(high) AS high_value,
        min(low) AS low_value,
        argMax(close, bar_time) AS close_value,
        sum(volume) AS volume_sum,
        sum(turnover) AS turnover_sum,
        sum(trade_count) AS trade_count_sum,
        sum(buy_volume) AS buy_volume_sum,
        sum(sell_volume) AS sell_volume_sum,
        sum(buy_turnover) AS buy_turnover_sum,
        sum(sell_turnover) AS sell_turnover_sum,
        sum(signed_sqrt_dollar_volume) AS signed_sqrt_dollar_volume_sum,
        sum(large_trade_count) AS large_trade_count_sum,
        sum(large_trade_volume) AS large_trade_volume_sum,
        sum(ob_snapshot_count) AS ob_snapshot_count_sum,
        if(count() = {source_multiple} AND min(is_time_bar_complete) = 1, 1, 0) AS is_time_bar_complete_value,
        max(has_book_data) AS has_book_data_value,
        max(has_open_interest_data) AS has_open_interest_data_value,
        max(has_funding_data) AS has_funding_data_value,
        ifNull(sum(avg_spread_bps * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_spread_bps_value,
        ifNull(sum(avg_mid_price * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_mid_price_value,
        ifNull(sum(avg_microprice * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_microprice_value,
        ifNull(sum(avg_bid1_size * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_bid1_size_value,
        ifNull(sum(avg_ask1_size * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_ask1_size_value,
        ifNull(sum(avg_top5_bid_depth * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_top5_bid_depth_value,
        ifNull(sum(avg_top5_ask_depth * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_top5_ask_depth_value,
        ifNull(sum(avg_top10_bid_depth * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_top10_bid_depth_value,
        ifNull(sum(avg_top10_ask_depth * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_top10_ask_depth_value,
        ifNull(sum(avg_imbalance_top1 * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_imbalance_top1_value,
        ifNull(sum(avg_imbalance_top5 * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_imbalance_top5_value,
        ifNull(sum(avg_imbalance_top10 * ob_snapshot_count) / nullIf(sum(ob_snapshot_count), 0), 0.0) AS avg_imbalance_top10_value,
        argMin(open_interest_open, bar_time) AS open_interest_open_value,
        argMax(open_interest_close, bar_time) AS open_interest_close_value,
        argMax(funding_rate, bar_time) AS funding_rate_value,
        sum(liq_buy_volume) AS liq_buy_volume_sum,
        sum(liq_sell_volume) AS liq_sell_volume_sum,
        sum(liq_buy_turnover) AS liq_buy_turnover_sum,
        sum(liq_sell_turnover) AS liq_sell_turnover_sum,
        sum(liq_count) AS liq_count_sum,
        min(first_trade_ts) AS first_trade_ts_value,
        max(last_trade_ts) AS last_trade_ts_value,
        max(max_trade_gap_ms) AS max_trade_gap_ms_value
    FROM sample.bars_time
    WHERE bar_interval_sec = 60
      AND {bar_time_condition}
    GROUP BY exchange, symbol, rollup_bar_time
    HAVING count() >= 1
) AS rollup_base
"""


def build_trade_count_reconciliation_sql(
    start: str | None = None,
    end: str | None = None,
    exchange: str | None = None,
    symbol: str | None = None,
) -> str:
    raw_conditions = [_build_time_condition("timestamp", start, end)]
    sample_conditions = ["bar_interval_sec = 60", _build_time_condition("bar_time", start, end)]
    if exchange:
        raw_conditions.append(f"exchange = '{exchange}'")
        sample_conditions.append(f"exchange = '{exchange}'")
    if symbol:
        raw_conditions.append(f"symbol = '{symbol}'")
        sample_conditions.append(f"symbol = '{symbol}'")
    raw_where = " AND ".join(condition for condition in raw_conditions if condition)
    sample_where = " AND ".join(condition for condition in sample_conditions if condition)
    return f"""
WITH
raw_counts AS (
    SELECT
        exchange,
        symbol,
        toStartOfInterval(timestamp, INTERVAL 60 SECOND) AS bar_time,
        count() AS raw_trade_count
    FROM raw.trades
    WHERE {raw_where}
    GROUP BY exchange, symbol, bar_time
),
bar_counts AS (
    SELECT
        exchange,
        symbol,
        bar_time,
        sum(trade_count) AS bar_trade_count
    FROM sample.bars_time
    WHERE {sample_where}
    GROUP BY exchange, symbol, bar_time
)
SELECT
    coalesce(r.exchange, b.exchange) AS exchange,
    coalesce(r.symbol, b.symbol) AS symbol,
    coalesce(r.bar_time, b.bar_time) AS bar_time,
    ifNull(raw_trade_count, 0) AS raw_trade_count,
    ifNull(bar_trade_count, 0) AS bar_trade_count,
    raw_trade_count - bar_trade_count AS trade_count_delta
FROM raw_counts AS r
FULL OUTER JOIN bar_counts AS b
    ON r.exchange = b.exchange
    AND r.symbol = b.symbol
    AND r.bar_time = b.bar_time
WHERE ifNull(raw_trade_count, 0) != ifNull(bar_trade_count, 0)
ORDER BY exchange, symbol, bar_time
"""


def build_rollup_gap_check_sql(
    interval_sec: int,
    start: str | None = None,
    end: str | None = None,
    exchange: str | None = None,
    symbol: str | None = None,
) -> str:
    _validate_interval_sec(interval_sec)
    if interval_sec <= 60:
        raise ValueError("Rollup verification requires an interval greater than 60 seconds")
    source_multiple = interval_sec // 60
    conditions = ["bar_interval_sec = 60", _build_time_condition("bar_time", start, end)]
    if exchange:
        conditions.append(f"exchange = '{exchange}'")
    if symbol:
        conditions.append(f"symbol = '{symbol}'")
    where_clause = " AND ".join(condition for condition in conditions if condition)
    return f"""
SELECT
    exchange,
    symbol,
    toStartOfInterval(bar_time, INTERVAL {interval_sec} SECOND) AS rollup_bar_time,
    count() AS source_bar_count
FROM sample.bars_time
WHERE {where_clause}
GROUP BY exchange, symbol, rollup_bar_time
HAVING count() != {source_multiple}
ORDER BY exchange, symbol, rollup_bar_time
"""


def run(interval_sec: int, start: str | None = None, end: str | None = None) -> None:
    _validate_interval_sec(interval_sec)
    client = get_client()
    sql = (
        build_trade_bar_insert_sql(interval_sec, start=start, end=end)
        if interval_sec == 60
        else build_rollup_insert_sql(interval_sec, start=start, end=end)
    )
    execute_pipeline_sql(client, "sample", "bars_time", interval_sec, sql, start=start, end=end)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()
    run(args.interval, start=args.start, end=args.end)


if __name__ == "__main__":
    main()
