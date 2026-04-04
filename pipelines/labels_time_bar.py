"""Build and run label SQL from time bars."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from pipelines.common import execute_pipeline_sql, get_client


LOOKFORWARD_BARS = 60
LABELS_TIME_BAR_INSERT_COLUMNS = """
    exchange,
    symbol,
    bar_interval_sec,
    bar_time,
    fwd_return_1,
    fwd_return_5,
    fwd_return_10,
    fwd_return_20,
    fwd_return_60,
    fwd_direction_5,
    fwd_direction_10,
    fwd_label_3class_5,
    fwd_label_3class_10,
    label_threshold_5,
    label_threshold_10,
    fwd_max_up_5,
    fwd_max_down_5,
    fwd_realized_vol_5,
    fwd_range_5,
    mae_5,
    mae_10,
    mfe_5,
    mfe_10,
    triple_barrier_signal,
    is_valid,
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


def _build_time_condition(column: str, start: str | None, end: str | None) -> str:
    conditions: list[str] = []
    if start:
        conditions.append(f"{column} >= toDateTime64('{_normalize_timestamp_literal(start)}', 3, 'UTC')")
    if end:
        conditions.append(f"{column} < toDateTime64('{_normalize_timestamp_literal(end)}', 3, 'UTC')")
    return " AND ".join(conditions) if conditions else "1"


def _lookforward_end(end: str | None, interval_sec: int) -> str | None:
    if not end:
        return None
    normalized = end.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    dt += timedelta(seconds=LOOKFORWARD_BARS * interval_sec)
    return dt.isoformat().replace("+00:00", "Z")


def build_label_insert_sql(
    interval_sec: int,
    start: str | None = None,
    end: str | None = None,
) -> str:
    base_time_condition = _build_time_condition("bar_time", start, _lookforward_end(end, interval_sec))
    target_time_condition = _build_time_condition("bar_time", start, end)
    return f"""
INSERT INTO label.labels_time_bar (
    {LABELS_TIME_BAR_INSERT_COLUMNS}
)
SELECT
    exchange,
    symbol,
    bar_interval_sec,
    bar_time,
    fwd_return_1,
    fwd_return_5,
    fwd_return_10,
    fwd_return_20,
    fwd_return_60,
    fwd_direction_5,
    fwd_direction_10,
    fwd_label_3class_5,
    fwd_label_3class_10,
    label_threshold_5,
    label_threshold_10,
    fwd_max_up_5,
    fwd_max_down_5,
    fwd_realized_vol_5,
    fwd_range_5,
    mae_5,
    mae_10,
    mfe_5,
    mfe_10,
    triple_barrier_signal,
    is_valid,
    _version
FROM (
    SELECT
        exchange,
        symbol,
        bar_interval_sec,
        bar_time,
        fwd_return_1,
        fwd_return_5,
        fwd_return_10,
        fwd_return_20,
        fwd_return_60,
        fwd_direction_5,
        fwd_direction_10,
        multiIf(
            fwd_return_5 > label_threshold_5, 1,
            fwd_return_5 < -label_threshold_5, -1,
            0
        ) AS fwd_label_3class_5,
        multiIf(
            fwd_return_10 > label_threshold_10, 1,
            fwd_return_10 < -label_threshold_10, -1,
            0
        ) AS fwd_label_3class_10,
        label_threshold_5,
        label_threshold_10,
        up_5 AS fwd_max_up_5,
        down_5 AS fwd_max_down_5,
        fwd_realized_vol_5,
        fwd_range_5,
        multiIf(
            fwd_direction_5 > 0, down_5,
            fwd_direction_5 < 0, up_5,
            0.0
        ) AS mae_5,
        multiIf(
            fwd_direction_10 > 0, down_10,
            fwd_direction_10 < 0, up_10,
            0.0
        ) AS mae_10,
        multiIf(
            fwd_direction_5 > 0, up_5,
            fwd_direction_5 < 0, down_5,
            0.0
        ) AS mfe_5,
        multiIf(
            fwd_direction_10 > 0, up_10,
            fwd_direction_10 < 0, down_10,
            0.0
        ) AS mfe_10,
        multiIf(
            tb_up_hit_idx > 0 AND (tb_down_hit_idx = 0 OR tb_up_hit_idx <= tb_down_hit_idx), 1,
            tb_down_hit_idx > 0 AND (tb_up_hit_idx = 0 OR tb_down_hit_idx < tb_up_hit_idx), -1,
            0
        ) AS triple_barrier_signal,
        if(fwd_bar_count = 60 AND isNotNull(close_t60), 1, 0) AS is_valid,
        toUInt64(toUnixTimestamp(now())) AS _version
    FROM (
        SELECT
            exchange,
            symbol,
            bar_interval_sec,
            bar_time,
            close,
            high,
            low,
            close_t60,
            fwd_bar_count,
            fwd_return_1,
            fwd_return_5,
            fwd_return_10,
            fwd_return_20,
            fwd_return_60,
            multiIf(
                fwd_return_5 > 0, 1,
                fwd_return_5 < 0, -1,
                0
            ) AS fwd_direction_5,
            multiIf(
                fwd_return_10 > 0, 1,
                fwd_return_10 < 0, -1,
                0
            ) AS fwd_direction_10,
            if(isFinite(hist_vol), hist_vol * 0.5, 0.0) AS label_threshold_5,
            if(isFinite(hist_vol), hist_vol * 0.7, 0.0) AS label_threshold_10,
            greatest(max(high) OVER w5 / nullIf(close, 0) - 1, 0.0) AS up_5,
            greatest(1 - min(low) OVER w5 / nullIf(close, 0), 0.0) AS down_5,
            greatest(max(high) OVER w10 / nullIf(close, 0) - 1, 0.0) AS up_10,
            greatest(1 - min(low) OVER w10 / nullIf(close, 0), 0.0) AS down_10,
            arrayFirstIndex(
                h -> h >= close * (1 + if(isFinite(hist_vol), hist_vol * 0.7, 0.0)),
                groupArray(20)(high) OVER w20
            ) AS tb_up_hit_idx,
            arrayFirstIndex(
                l -> l <= close * (1 - if(isFinite(hist_vol), hist_vol * 0.7, 0.0)),
                groupArray(20)(low) OVER w20
            ) AS tb_down_hit_idx,
            stddevSamp(next_bar_return) OVER rv5 AS fwd_realized_vol_5,
            (max(high) OVER w5 - min(low) OVER w5) / nullIf(close, 0) AS fwd_range_5
        FROM (
            SELECT
                exchange,
                symbol,
                bar_interval_sec,
                bar_time,
                close,
                high,
                low,
                close_t60,
                fwd_bar_count,
                next_bar_return,
                if(isNotNull(close_t1), close_t1 / nullIf(close, 0) - 1, CAST(NULL, 'Nullable(Float64)')) AS fwd_return_1,
                if(isNotNull(close_t5), close_t5 / nullIf(close, 0) - 1, CAST(NULL, 'Nullable(Float64)')) AS fwd_return_5,
                if(isNotNull(close_t10), close_t10 / nullIf(close, 0) - 1, CAST(NULL, 'Nullable(Float64)')) AS fwd_return_10,
                if(isNotNull(close_t20), close_t20 / nullIf(close, 0) - 1, CAST(NULL, 'Nullable(Float64)')) AS fwd_return_20,
                if(isNotNull(close_t60), close_t60 / nullIf(close, 0) - 1, CAST(NULL, 'Nullable(Float64)')) AS fwd_return_60,
                stddevSamp(next_bar_return) OVER hist_w AS hist_vol
            FROM (
                SELECT
                    exchange,
                    symbol,
                    bar_interval_sec,
                    bar_time,
                    close,
                    high,
                    low,
                    close_t1,
                    close_t5,
                    close_t10,
                    close_t20,
                    close_t60,
                    fwd_bar_count,
                    if(isNotNull(close_t1), close_t1 / nullIf(close, 0) - 1, CAST(NULL, 'Nullable(Float64)')) AS next_bar_return
                FROM (
                    SELECT
                        exchange,
                        symbol,
                        bar_interval_sec,
                        bar_time,
                        close,
                        high,
                        low,
                        leadInFrame(toNullable(close), 1) OVER base_w AS close_t1,
                        leadInFrame(toNullable(close), 5) OVER base_w AS close_t5,
                        leadInFrame(toNullable(close), 10) OVER base_w AS close_t10,
                        leadInFrame(toNullable(close), 20) OVER base_w AS close_t20,
                        leadInFrame(toNullable(close), 60) OVER base_w AS close_t60,
                        count() OVER fwd_count_60 AS fwd_bar_count
                    FROM sample.bars_time
                    WHERE bar_interval_sec = {interval_sec}
                      AND {base_time_condition}
                    WINDOW
                        base_w AS (
                            PARTITION BY exchange, symbol, bar_interval_sec
                            ORDER BY bar_time
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ),
                        fwd_count_60 AS (
                            PARTITION BY exchange, symbol, bar_interval_sec
                            ORDER BY bar_time
                            ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
                        )
                ) AS forward_closes
            ) AS with_returns
            WINDOW
                hist_w AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING)
        ) AS hist_ready
        WINDOW
            rv5 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING),
            w5 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING),
            w10 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING),
            w20 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING)
        ) AS prepared
    ) AS labeled
WHERE {target_time_condition}
"""


def build_label_quality_check_sql(
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
    sum(is_valid = 0) AS invalid_row_count,
    minIf(bar_time, is_valid = 0) AS first_invalid_bar_time,
    maxIf(bar_time, is_valid = 0) AS last_invalid_bar_time
FROM label.labels_time_bar
WHERE {where_clause}
"""


def run(interval_sec: int, start: str | None = None, end: str | None = None) -> None:
    client = get_client()
    execute_pipeline_sql(
        client,
        "label",
        "labels_time_bar",
        interval_sec,
        build_label_insert_sql(interval_sec, start=start, end=end),
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
