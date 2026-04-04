"""Create and query the dataset view that joins samples, features, and labels."""

from __future__ import annotations

import argparse
import csv

from pipelines.common import build_time_condition, get_client


KEY_COLUMNS = ["exchange", "symbol", "bar_interval_sec", "bar_time"]
SAMPLE_CONTEXT_COLUMNS = ["volume", "turnover", "trade_count", "ob_snapshot_count"]
SAMPLE_QUALITY_SOURCE_COLUMNS = [
    "is_time_bar_complete",
    "has_book_data",
    "has_open_interest_data",
    "has_funding_data",
]
FEATURE_COLUMNS = [
    "log_return",
    "co_return",
    "hl_range",
    "close_position",
    "gap_return",
    "realized_vol_5",
    "realized_vol_20",
    "realized_vol_60",
    "vol_ratio_5_20",
    "volume_zscore_20",
    "volume_ratio_5_20",
    "buy_ratio",
    "delta_volume",
    "avg_trade_size",
    "large_trade_pct",
    "close_vwap_bias",
    "spread_bps",
    "imbalance_top1",
    "imbalance_top5",
    "depth_ratio_5",
    "microprice_bias",
    "ob_quality",
    "oi_change",
    "oi_change_pct",
    "oi_zscore_20",
    "funding_rate",
    "liq_imbalance",
    "liq_intensity",
    "liq_volume_pct",
    "autocorrelation_5",
    "autocorrelation_20",
    "kyle_lambda_approx",
    "amihud_illiquidity",
    "price_acceleration",
    "hour_of_day",
    "day_of_week",
]
LABEL_COLUMNS = [
    "fwd_return_1",
    "fwd_return_5",
    "fwd_return_10",
    "fwd_return_20",
    "fwd_return_60",
    "fwd_direction_5",
    "fwd_direction_10",
    "fwd_label_3class_5",
    "fwd_label_3class_10",
    "label_threshold_5",
    "label_threshold_10",
    "fwd_max_up_5",
    "fwd_max_down_5",
    "fwd_realized_vol_5",
    "fwd_range_5",
    "mae_5",
    "mae_10",
    "mfe_5",
    "mfe_10",
]
QUALITY_COLUMNS = [
    "label_is_valid",
    "sample_has_trades",
    "sample_has_book",
    "sample_is_time_bar_complete",
    "sample_has_open_interest",
    "sample_has_funding",
    "feature_window_ready",
    "training_ready",
]
TRAIN_DATASET_COLUMNS = KEY_COLUMNS + SAMPLE_CONTEXT_COLUMNS + FEATURE_COLUMNS + LABEL_COLUMNS
ALL_DATASET_COLUMNS = TRAIN_DATASET_COLUMNS + QUALITY_COLUMNS


def get_dataset_select_columns(column_set: str = "all") -> list[str]:
    if column_set == "train":
        return TRAIN_DATASET_COLUMNS
    if column_set == "all":
        return ALL_DATASET_COLUMNS
    raise ValueError(f"Unsupported dataset column set: {column_set}")


def _render_select_columns(columns: list[str]) -> str:
    return ",\n    ".join(columns)


def _build_dataset_filters(
    alias: str,
    interval_sec: int | None = None,
    start: str | None = None,
    end: str | None = None,
    exchange: str | None = None,
    symbol: str | None = None,
) -> str:
    conditions: list[str] = []
    if interval_sec is not None:
        conditions.append(f"{alias}.bar_interval_sec = {interval_sec}")
    time_condition = build_time_condition(f"{alias}.bar_time", start, end)
    if time_condition != "1":
        conditions.append(time_condition)
    if exchange:
        conditions.append(f"{alias}.exchange = '{exchange}'")
    if symbol:
        conditions.append(f"{alias}.symbol = '{symbol}'")
    return " AND ".join(condition for condition in conditions if condition) if conditions else "1"


def _build_dataset_core_sql(
    interval_sec: int | None = None,
    start: str | None = None,
    end: str | None = None,
    exchange: str | None = None,
    symbol: str | None = None,
) -> str:
    feature_window_ready_expr = " AND ".join(
        [
            "isFinite(f.realized_vol_20)",
            "isFinite(f.realized_vol_60)",
            "isFinite(f.volume_zscore_20)",
            "isFinite(f.oi_zscore_20)",
        ]
    )
    feature_select = ",\n    ".join(f"f.{column} AS {column}" for column in FEATURE_COLUMNS)
    label_select = ",\n    ".join(f"l.{column} AS {column}" for column in LABEL_COLUMNS)
    table_filters = [
        _build_dataset_filters("f", interval_sec=interval_sec, start=start, end=end, exchange=exchange, symbol=symbol),
        _build_dataset_filters("l", interval_sec=interval_sec, start=start, end=end, exchange=exchange, symbol=symbol),
        _build_dataset_filters("s", interval_sec=interval_sec, start=start, end=end, exchange=exchange, symbol=symbol),
    ]
    where_clause = " AND ".join(condition for condition in table_filters if condition != "1") or "1"
    return f"""
SELECT
    f.exchange AS exchange,
    f.symbol AS symbol,
    f.bar_interval_sec AS bar_interval_sec,
    f.bar_time AS bar_time,
    s.volume AS volume,
    s.turnover AS turnover,
    s.trade_count AS trade_count,
    s.ob_snapshot_count AS ob_snapshot_count,
    {feature_select},
    {label_select},
    l.is_valid AS label_is_valid,
    if(s.trade_count > 0 AND s.volume > 0, 1, 0) AS sample_has_trades,
    if(s.has_book_data = 1, 1, 0) AS sample_has_book,
    if(s.is_time_bar_complete = 1, 1, 0) AS sample_is_time_bar_complete,
    if(s.has_open_interest_data = 1, 1, 0) AS sample_has_open_interest,
    if(s.has_funding_data = 1, 1, 0) AS sample_has_funding,
    if({feature_window_ready_expr}, 1, 0) AS feature_window_ready,
    if(
        s.trade_count > 0
        AND s.volume > 0
        AND s.has_book_data = 1
        AND s.is_time_bar_complete = 1
        AND {feature_window_ready_expr},
        1,
        0
    ) AS training_ready
FROM feature.features_time_bar AS f FINAL
INNER JOIN label.labels_time_bar AS l FINAL
    ON f.exchange = l.exchange
    AND f.symbol = l.symbol
    AND f.bar_interval_sec = l.bar_interval_sec
    AND f.bar_time = l.bar_time
INNER JOIN sample.bars_time AS s FINAL
    ON f.exchange = s.exchange
    AND f.symbol = s.symbol
    AND f.bar_interval_sec = s.bar_interval_sec
    AND f.bar_time = s.bar_time
WHERE {where_clause}
"""


def build_dataset_view_sql() -> str:
    return f"""
CREATE VIEW IF NOT EXISTS dataset.dataset_time_bar AS
{_build_dataset_core_sql()}
"""


def refresh_dataset_view(client) -> None:
    client.command("DROP VIEW IF EXISTS dataset.dataset_time_bar")
    client.command(build_dataset_view_sql())


def build_dataset_select_sql(
    interval_sec: int | None = None,
    start: str | None = None,
    end: str | None = None,
    exchange: str | None = None,
    symbol: str | None = None,
    limit: int | None = None,
    training_ready_only: bool = False,
    label_valid_only: bool = False,
    column_set: str = "all",
) -> str:
    conditions: list[str] = []
    if training_ready_only:
        conditions.append("training_ready = 1")
    if label_valid_only:
        conditions.append("label_is_valid = 1")
    where_clause = " AND ".join(condition for condition in conditions if condition)
    if not where_clause:
        where_clause = "1"
    limit_clause = f"\nLIMIT {limit}" if limit else ""
    select_clause = _render_select_columns(get_dataset_select_columns(column_set))
    dataset_core_sql = _build_dataset_core_sql(
        interval_sec=interval_sec,
        start=start,
        end=end,
        exchange=exchange,
        symbol=symbol,
    )
    return f"""
SELECT
    {select_clause}
FROM (
    {dataset_core_sql}
) AS dataset_rows
WHERE {where_clause}
ORDER BY exchange, symbol, bar_time
{limit_clause}
"""


def build_dataset_row_count_check_sql(
    interval_sec: int | None = None,
    start: str | None = None,
    end: str | None = None,
    exchange: str | None = None,
    symbol: str | None = None,
) -> str:
    dataset_core_sql = _build_dataset_core_sql(
        interval_sec=interval_sec,
        start=start,
        end=end,
        exchange=exchange,
        symbol=symbol,
    )
    label_where = " AND ".join(
        [
            _build_dataset_filters(
                "l",
                interval_sec=interval_sec,
                start=start,
                end=end,
                exchange=exchange,
                symbol=symbol,
            ),
            "l.is_valid = 1",
        ]
    )
    return f"""
WITH
dataset_counts AS (
    SELECT count() AS dataset_row_count
    FROM (
        {dataset_core_sql}
    ) AS dataset_rows
    WHERE label_is_valid = 1
),
valid_label_counts AS (
    SELECT count() AS valid_label_row_count
    FROM label.labels_time_bar AS l FINAL
    WHERE {label_where}
)
SELECT
    dataset_row_count,
    valid_label_row_count,
    dataset_row_count - valid_label_row_count AS row_count_delta
FROM dataset_counts
CROSS JOIN valid_label_counts
"""


def query_dataset(
    client,
    interval_sec: int | None = None,
    start: str | None = None,
    end: str | None = None,
    exchange: str | None = None,
    symbol: str | None = None,
    limit: int | None = None,
    training_ready_only: bool = False,
    label_valid_only: bool = False,
    column_set: str = "all",
):
    return client.query(
        build_dataset_select_sql(
            interval_sec=interval_sec,
            start=start,
            end=end,
            exchange=exchange,
            symbol=symbol,
            limit=limit,
            training_ready_only=training_ready_only,
            label_valid_only=label_valid_only,
            column_set=column_set,
        )
    )


def export_dataset_csv(client, output_path: str, **query_kwargs) -> None:
    query_kwargs.setdefault("training_ready_only", True)
    query_kwargs.setdefault("label_valid_only", True)
    query_kwargs.setdefault("column_set", "train")
    result = query_dataset(client, **query_kwargs)
    header_columns = list(result.column_names) or get_dataset_select_columns(query_kwargs["column_set"])
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(header_columns)
        writer.writerows(result.result_rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--exchange")
    parser.add_argument("--symbol")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output")
    parser.add_argument("--skip-refresh", action="store_true")
    parser.add_argument("--training-ready-only", action="store_true")
    parser.add_argument("--label-valid-only", action="store_true")
    parser.add_argument("--column-set", choices=("all", "train"))
    args = parser.parse_args()

    client = get_client()
    if not args.skip_refresh:
        refresh_dataset_view(client)

    query_requested = any(
        value is not None
        for value in (
            args.interval,
            args.start,
            args.end,
            args.exchange,
            args.symbol,
            args.limit,
            args.output,
            args.column_set,
        )
    ) or args.training_ready_only or args.label_valid_only
    if not query_requested:
        return

    query_kwargs = {
        "interval_sec": args.interval,
        "start": args.start,
        "end": args.end,
        "exchange": args.exchange,
        "symbol": args.symbol,
        "limit": args.limit,
        "training_ready_only": args.training_ready_only or bool(args.output),
        "label_valid_only": args.label_valid_only or bool(args.output),
        "column_set": args.column_set or ("train" if args.output else "all"),
    }
    if args.output:
        export_dataset_csv(client, args.output, **query_kwargs)
        return

    result = query_dataset(client, **query_kwargs)
    for row in result.result_rows:
        print(row)


if __name__ == "__main__":
    main()

