"""Shared helpers for pipeline jobs."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import clickhouse_connect


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USERNAME", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def normalize_timestamp_literal(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def build_time_condition(column: str, start: str | None, end: str | None) -> str:
    conditions: list[str] = []
    if start:
        conditions.append(f"{column} >= toDateTime64('{normalize_timestamp_literal(start)}', 3, 'UTC')")
    if end:
        conditions.append(f"{column} < toDateTime64('{normalize_timestamp_literal(end)}', 3, 'UTC')")
    return " AND ".join(conditions) if conditions else "1"


def _run_range_expr(value: str | None) -> str:
    if not value:
        return "now()"
    normalized = normalize_timestamp_literal(value).split(".")[0]
    return f"toDateTime('{normalized}', 'UTC')"


def record_run_start(
    client,
    layer: str,
    table_name: str,
    interval_sec: int,
    start: str | None = None,
    end: str | None = None,
    symbols: list[str] | None = None,
) -> str:
    run_id = str(uuid.uuid4())
    symbols_sql = "[" + ", ".join(f"'{symbol}'" for symbol in (symbols or [])) + "]"
    client.command(
        "INSERT INTO meta.pipeline_runs "
        "(run_id, layer, table_name, bar_interval_sec, symbols, time_range_start, time_range_end, row_count, status) "
        f"VALUES ('{run_id}', '{layer}', '{table_name}', {interval_sec}, {symbols_sql}, "
        f"{_run_range_expr(start)}, {_run_range_expr(end)}, 0, 'running')"
    )
    return run_id


def record_run_finish(client, run_id: str, row_count: int, status: str = "done") -> None:
    client.command(
        "ALTER TABLE meta.pipeline_runs UPDATE "
        f"row_count = {row_count}, status = '{status}', finished_at = now() "
        f"WHERE run_id = '{run_id}'"
    )


def execute_pipeline_sql(
    client,
    layer: str,
    table_name: str,
    interval_sec: int,
    sql: str,
    start: str | None = None,
    end: str | None = None,
    symbols: list[str] | None = None,
) -> None:
    run_id = record_run_start(
        client,
        layer,
        table_name,
        interval_sec,
        start=start,
        end=end,
        symbols=symbols,
    )
    try:
        client.command(sql)
    except Exception:
        record_run_finish(client, run_id, 0, status="failed")
        raise
    record_run_finish(client, run_id, 0, status="done")


def build_repair_stale_runs_sql(max_age_minutes: int = 30, layer: str | None = None) -> str:
    layer_filter = f" AND layer = '{layer}'" if layer else ""
    return (
        "ALTER TABLE meta.pipeline_runs UPDATE "
        "status = 'failed', finished_at = now() "
        "WHERE status = 'running' "
        f"AND started_at < now() - INTERVAL {max_age_minutes} MINUTE"
        f"{layer_filter}"
    )


def repair_stale_runs(client, max_age_minutes: int = 30, layer: str | None = None) -> None:
    client.command(build_repair_stale_runs_sql(max_age_minutes=max_age_minutes, layer=layer))
