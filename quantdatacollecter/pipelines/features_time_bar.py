"""Build and run feature SQL from time bars."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from pipelines.common import execute_pipeline_sql, get_client


ROLLING_LOOKBACK_BARS = 61


def _fractional_weights(d: float, lag: int) -> list[float]:
    weights = [1.0]
    for k in range(1, lag + 1):
        weights.append(-weights[-1] * (d - k + 1) / k)
    return weights


def _build_frac_diff_expr(d: float, lag: int = 5) -> str:
    weights = _fractional_weights(d, lag)
    terms = [f"({weights[0]:.10f} * close)"]
    for k in range(1, lag + 1):
        terms.append(f"({weights[k]:.10f} * lagInFrame(close, {k}) OVER lag_w)")
    return " + ".join(terms)


def _normalize_timestamp_literal(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _build_time_condition(column: str, start: str | None, end: str | None) -> str:
    conditions: list[str] = []
    if start:
        conditions.append(f"{column} >= toDateTime64('{_normalize_timestamp_literal(start)}', 3, 'UTC')")
    if end:
        conditions.append(f"{column} < toDateTime64('{_normalize_timestamp_literal(end)}', 3, 'UTC')")
    return " AND ".join(conditions) if conditions else "1"


def _lookback_start(start: str | None, interval_sec: int) -> str | None:
    if not start:
        return None
    normalized = start.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    dt -= timedelta(seconds=ROLLING_LOOKBACK_BARS * interval_sec)
    return dt.isoformat().replace("+00:00", "Z")


def build_feature_insert_sql(
    interval_sec: int,
    start: str | None = None,
    end: str | None = None,
) -> str:
    base_time_condition = _build_time_condition("bar_time", _lookback_start(start, interval_sec), end)
    target_time_condition = _build_time_condition("bar_time", start, end)
    return f"""
INSERT INTO feature.features_time_bar (
    exchange,
    symbol,
    bar_interval_sec,
    bar_time,
    log_return,
    co_return,
    hl_range,
    close_position,
    gap_return,
    realized_vol_5,
    realized_vol_20,
    realized_vol_60,
    vol_ratio_5_20,
    volume_zscore_20,
    volume_ratio_5_20,
    buy_ratio,
    delta_volume,
    avg_trade_size,
    large_trade_pct,
    close_vwap_bias,
    spread_bps,
    imbalance_top1,
    imbalance_top5,
    depth_ratio_5,
    microprice_bias,
    ob_quality,
    oi_change,
    oi_change_pct,
    oi_zscore_20,
    funding_rate,
    liq_imbalance,
    liq_intensity,
    liq_volume_pct,
    autocorrelation_5,
    autocorrelation_20,
    kyle_lambda_approx,
    amihud_illiquidity,
    frac_diff_0_4,
    frac_diff_0_6,
    vpin_5,
    vpin_10,
    vpin_20,
    price_acceleration,
    hour_of_day,
    day_of_week,
    _version
)
SELECT
    exchange,
    symbol,
    bar_interval_sec,
    bar_time,
    log_return,
    co_return,
    hl_range,
    close_position,
    gap_return,
    realized_vol_5,
    realized_vol_20,
    realized_vol_60,
    realized_vol_5 / nullIf(realized_vol_20, 0) AS vol_ratio_5_20,
    volume_zscore_20,
    volume_ratio_5_20,
    buy_ratio,
    delta_volume,
    avg_trade_size,
    large_trade_pct,
    close_vwap_bias,
    spread_bps,
    imbalance_top1,
    imbalance_top5,
    depth_ratio_5,
    microprice_bias,
    ob_quality,
    oi_change,
    oi_change_pct,
    oi_zscore_20,
    funding_rate,
    liq_imbalance,
    liq_intensity,
    liq_volume_pct,
    autocorrelation_5,
    autocorrelation_20,
    kyle_lambda_approx,
    /* turnover in sample.bars_time is bar notional (sum(raw.trades.notional)); this matches Amihud's dollar-volume denominator */
    abs(log_return) / nullIf(turnover, 0) AS amihud_illiquidity,
    frac_diff_0_4,
    frac_diff_0_6,
    vpin_5,
    vpin_10,
    vpin_20,
    log_return - prev_log_return AS price_acceleration,
    hour_of_day,
    day_of_week,
    toUInt64(toUnixTimestamp(now())) AS _version
FROM (
    SELECT
        exchange,
        symbol,
        bar_interval_sec,
        bar_time,
        turnover,
        close,
        log_return,
        prev_log_return,
        co_return,
        hl_range,
        close_position,
        gap_return,
        frac_diff_0_4,
        frac_diff_0_6,
        stddevSamp(log_return) OVER w5 AS realized_vol_5,
        stddevSamp(log_return) OVER w20 AS realized_vol_20,
        stddevSamp(log_return) OVER w60 AS realized_vol_60,
        (volume - avg(volume) OVER w20) / nullIf(stddevSamp(volume) OVER w20, 0) AS volume_zscore_20,
        avg(volume) OVER w5 / nullIf(avg(volume) OVER w20, 0) AS volume_ratio_5_20,
        buy_ratio,
        delta_volume,
        /* Rolling no-intercept OLS slope on signed square-root dollar volume. */
        (
            sum(
                if(
                    isNull(log_return),
                    CAST(NULL, 'Nullable(Float64)'),
                    log_return * signed_sqrt_dollar_volume
                )
            ) OVER w20
        ) / nullIf(
            sum(
                if(
                    isNull(log_return),
                    CAST(NULL, 'Nullable(Float64)'),
                    signed_sqrt_dollar_volume * signed_sqrt_dollar_volume
                )
            ) OVER w20,
            0
        ) AS kyle_lambda_approx,
        sum(abs(delta_volume)) OVER w5 / nullIf(sum(volume) OVER w5, 0) AS vpin_5,
        sum(abs(delta_volume)) OVER w10 / nullIf(sum(volume) OVER w10, 0) AS vpin_10,
        sum(abs(delta_volume)) OVER w20 / nullIf(sum(volume) OVER w20, 0) AS vpin_20,
        avg_trade_size,
        large_trade_pct,
        close_vwap_bias,
        spread_bps,
        imbalance_top1,
        imbalance_top5,
        depth_ratio_5,
        microprice_bias,
        ob_quality,
        oi_change,
        oi_change_pct,
        (oi_change - avg(oi_change) OVER w20) / nullIf(stddevSamp(oi_change) OVER w20, 0) AS oi_zscore_20,
        funding_rate,
        liq_imbalance,
        liq_count / nullIf(trade_count, 0) AS liq_intensity,
        (liq_buy_volume + liq_sell_volume) / nullIf(volume, 0) AS liq_volume_pct,
        corrStable(log_return, prev_log_return) OVER w5 AS autocorrelation_5,
        corrStable(log_return, prev_log_return) OVER w20 AS autocorrelation_20,
        hour_of_day,
        day_of_week
    FROM (
        SELECT
            exchange,
            symbol,
            bar_interval_sec,
            bar_time,
            close,
            volume,
            turnover,
            trade_count,
            buy_ratio,
            delta_volume,
            signed_sqrt_dollar_volume,
            avg_trade_size,
            large_trade_pct,
            close_vwap_bias,
            spread_bps,
            imbalance_top1,
            imbalance_top5,
            depth_ratio_5,
            microprice_bias,
            ob_quality,
            oi_change,
            oi_change_pct,
            funding_rate,
            liq_imbalance,
            liq_buy_volume,
            liq_sell_volume,
            liq_count,
            hour_of_day,
            day_of_week,
            log_return,
            lagInFrame(log_return, 1) OVER lag_w AS prev_log_return,
            co_return,
            hl_range,
            close_position,
            gap_return,
            {_build_frac_diff_expr(0.4)} AS frac_diff_0_4,
            {_build_frac_diff_expr(0.6)} AS frac_diff_0_6
        FROM (
            SELECT
                exchange,
                symbol,
                bar_interval_sec,
                bar_time,
                close,
                log(close / nullIf(prev_close, 0)) AS log_return,
                (close - open) / nullIf(open, 0) AS co_return,
                (high - low) / nullIf(close, 0) AS hl_range,
                if(high = low, 0.5, (close - low) / (high - low)) AS close_position,
                (open - prev_close) / nullIf(prev_close, 0) AS gap_return,
                volume,
                turnover,
                trade_count,
                buy_volume / nullIf(volume, 0) AS buy_ratio,
                buy_volume - sell_volume AS delta_volume,
                signed_sqrt_dollar_volume,
                volume / nullIf(trade_count, 0) AS avg_trade_size,
                CAST(NULL, 'Nullable(Float64)') AS large_trade_pct,
                (close - vwap) / nullIf(vwap, 0) AS close_vwap_bias,
                avg_spread_bps AS spread_bps,
                avg_imbalance_top1 AS imbalance_top1,
                avg_imbalance_top5 AS imbalance_top5,
                avg_top5_bid_depth / nullIf(avg_top5_ask_depth, 0) AS depth_ratio_5,
                (avg_microprice - avg_mid_price) / nullIf(avg_mid_price, 0) AS microprice_bias,
                ob_snapshot_count AS ob_quality,
                if(
                    has_open_interest_data = 1 AND prev_has_open_interest_data = 1,
                    open_interest_close - prev_oi_close,
                    0.0
                ) AS oi_change,
                if(
                    has_open_interest_data = 1 AND prev_has_open_interest_data = 1,
                    (open_interest_close - prev_oi_close) / nullIf(prev_oi_close, 0),
                    0.0
                ) AS oi_change_pct,
                funding_rate,
                (liq_buy_volume - liq_sell_volume) / nullIf(liq_buy_volume + liq_sell_volume, 0) AS liq_imbalance,
                liq_buy_volume,
                liq_sell_volume,
                liq_count,
                toHour(bar_time, 'UTC') AS hour_of_day,
                toDayOfWeek(bar_time, 0, 'UTC') AS day_of_week
            FROM (
                SELECT
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
                    signed_sqrt_dollar_volume,
                    large_trade_volume,
                    vwap,
                    avg_spread_bps,
                    avg_imbalance_top1,
                    avg_imbalance_top5,
                    avg_top5_bid_depth,
                    avg_top5_ask_depth,
                    avg_microprice,
                    avg_mid_price,
                    ob_snapshot_count,
                    open_interest_close,
                    has_open_interest_data,
                    funding_rate,
                    liq_buy_volume,
                    liq_sell_volume,
                    liq_count,
                    lagInFrame(close, 1) OVER base_w AS prev_close,
                    lagInFrame(open_interest_close, 1) OVER base_w AS prev_oi_close,
                    lagInFrame(has_open_interest_data, 1, 0) OVER base_w AS prev_has_open_interest_data
                FROM sample.bars_time
                WHERE bar_interval_sec = {interval_sec}
                  AND {base_time_condition}
                WINDOW base_w AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time)
            ) AS base
        ) AS with_basic_features
        WINDOW lag_w AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time)
    ) AS enriched
    WINDOW
        w5 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
        w10 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),
        w20 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),
        w60 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING)
) AS scored
WHERE {target_time_condition}
"""


def build_feature_quality_check_sql(
    interval_sec: int,
    start: str | None = None,
    end: str | None = None,
    exchange: str | None = None,
    symbol: str | None = None,
) -> str:
    conditions = [f"bar_interval_sec = {interval_sec}", _build_time_condition("bar_time", start, end)]
    if exchange:
        conditions.append(f"exchange = '{exchange}'")
    if symbol:
        conditions.append(f"symbol = '{symbol}'")
    where_clause = " AND ".join(condition for condition in conditions if condition)
    return f"""
SELECT
    count() AS row_count,
    countIf(isNull(log_return)) AS log_return_null,
    countIf(isNull(realized_vol_20)) AS realized_vol_20_null,
    countIf(NOT isFinite(realized_vol_20)) AS realized_vol_20_bad_rows,
    countIf(NOT isFinite(volume_zscore_20)) AS volume_zscore_20_bad_rows,
    countIf(NOT isFinite(autocorrelation_5)) AS autocorrelation_5_bad_rows,
    countIf(NOT isFinite(autocorrelation_20)) AS autocorrelation_20_bad_rows,
    countIf(NOT isFinite(oi_zscore_20)) AS oi_zscore_20_bad_rows,
    countIf(NOT isFinite(kyle_lambda_approx)) AS kyle_lambda_bad,
    countIf(NOT isFinite(amihud_illiquidity)) AS amihud_bad,
    countIf(NOT isFinite(vol_ratio_5_20)) AS vol_ratio_bad,
    countIf(buy_ratio < 0 OR buy_ratio > 1) AS buy_ratio_out_of_range,
    countIf(close_position < 0 OR close_position > 1) AS close_position_out_of_range,
    countIf(abs(log_return) > 0.5) AS extreme_return_count,
    uniqExact(ifNull(large_trade_pct, toFloat64(-1))) AS large_trade_pct_distinct_values
FROM feature.features_time_bar
WHERE {where_clause}
"""


def run(interval_sec: int, start: str | None = None, end: str | None = None) -> None:
    client = get_client()
    execute_pipeline_sql(
        client,
        "feature",
        "features_time_bar",
        interval_sec,
        build_feature_insert_sql(interval_sec, start=start, end=end),
        start=start,
        end=end,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()
    run(args.interval, start=args.start, end=args.end)


if __name__ == "__main__":
    main()
