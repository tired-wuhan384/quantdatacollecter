"""Run incremental time-bar backfill with auto window detection."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Callable

from pipelines import dataset_view, features_time_bar, labels_time_bar, sample_time_bars
from pipelines.backfill_time_bar_pipeline import parse_intervals
from pipelines.common import get_client


def _parse_utc(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_interval_watermarks(client, interval_sec: int) -> tuple[datetime | None, datetime | None, datetime | None]:
    sql = f"""
SELECT
    (SELECT maxOrNull(bar_time) FROM sample.bars_time FINAL WHERE bar_interval_sec = {interval_sec}) AS sample_max,
    (SELECT maxOrNull(bar_time) FROM feature.features_time_bar FINAL WHERE bar_interval_sec = {interval_sec}) AS feature_max,
    (SELECT maxOrNull(bar_time) FROM label.labels_time_bar FINAL WHERE bar_interval_sec = {interval_sec}) AS label_max
"""
    row = client.query(sql).result_rows[0]
    return (_ensure_utc(row[0]), _ensure_utc(row[1]), _ensure_utc(row[2]))


def compute_interval_window(
    interval_sec: int,
    sample_max: datetime | None,
    feature_max: datetime | None,
    label_max: datetime | None,
    overlap_bars: int,
    bootstrap_start: str | None,
    start_override: str | None,
    end_override: str | None,
    safety_lag_seconds: int,
    now_utc: Callable[[], datetime] | None = None,
) -> tuple[str, str]:
    if overlap_bars < 0:
        raise ValueError("overlap_bars must be >= 0")
    if safety_lag_seconds < 0:
        raise ValueError("safety_lag_seconds must be >= 0")

    now_provider = now_utc or (lambda: datetime.now(timezone.utc))

    if start_override:
        start_dt = _parse_utc(start_override)
    else:
        watermarks = [value for value in (sample_max, feature_max, label_max) if value is not None]
        if watermarks:
            anchor = min(_ensure_utc(value) for value in watermarks if value is not None)
            if anchor is None:
                raise ValueError("Unable to resolve watermark anchor")
            start_dt = anchor - timedelta(seconds=interval_sec * overlap_bars)
        elif bootstrap_start:
            start_dt = _parse_utc(bootstrap_start)
        else:
            raise ValueError(
                "No historical watermark found for the interval; provide --bootstrap-start or --start"
            )

    if end_override:
        end_dt = _parse_utc(end_override)
    else:
        end_dt = _ensure_utc(now_provider()) - timedelta(seconds=safety_lag_seconds)

    if start_dt >= end_dt:
        raise ValueError(
            f"Resolved start ({_format_utc_z(start_dt)}) must be earlier than end ({_format_utc_z(end_dt)})"
        )

    return _format_utc_z(start_dt), _format_utc_z(end_dt)


def build_incremental_steps(
    interval_windows: dict[int, tuple[str, str]],
    refresh_dataset: bool = True,
) -> list[tuple[str, int | None, str | None, str | None]]:
    if not interval_windows:
        raise ValueError("interval_windows must not be empty")

    intervals = sorted(interval_windows)
    starts = [_parse_utc(interval_windows[interval][0]) for interval in intervals]
    ends = [_parse_utc(interval_windows[interval][1]) for interval in intervals]

    sample_60_start = _format_utc_z(min(starts))
    sample_60_end = _format_utc_z(max(ends))

    steps: list[tuple[str, int | None, str | None, str | None]] = [
        ("sample", 60, sample_60_start, sample_60_end)
    ]

    for interval in intervals:
        if interval != 60:
            start, end = interval_windows[interval]
            steps.append(("sample", interval, start, end))

    for interval in intervals:
        start, end = interval_windows[interval]
        steps.append(("feature", interval, start, end))
        steps.append(("label", interval, start, end))

    if refresh_dataset:
        steps.append(("dataset", None, None, None))

    return steps


def run_incremental_backfill(
    intervals: list[int],
    start: str | None = None,
    end: str | None = None,
    bootstrap_start: str | None = None,
    overlap_bars: int = 120,
    safety_lag_seconds: int = 120,
    refresh_dataset: bool = True,
    dry_run: bool = False,
) -> list[tuple[str, int | None, str | None, str | None]]:
    client = get_client()

    interval_windows: dict[int, tuple[str, str]] = {}
    for interval in sorted(set(intervals)):
        sample_max, feature_max, label_max = fetch_interval_watermarks(client, interval)
        interval_windows[interval] = compute_interval_window(
            interval_sec=interval,
            sample_max=sample_max,
            feature_max=feature_max,
            label_max=label_max,
            overlap_bars=overlap_bars,
            bootstrap_start=bootstrap_start,
            start_override=start,
            end_override=end,
            safety_lag_seconds=safety_lag_seconds,
        )

    steps = build_incremental_steps(interval_windows, refresh_dataset=refresh_dataset)

    if dry_run:
        return steps

    for stage, interval, stage_start, stage_end in steps:
        if stage == "sample":
            if interval is None:
                continue
            sample_time_bars.run(interval, start=stage_start, end=stage_end)
        elif stage == "feature":
            if interval is None:
                continue
            features_time_bar.run(interval, start=stage_start, end=stage_end)
        elif stage == "label":
            if interval is None:
                continue
            labels_time_bar.run(interval, start=stage_start, end=stage_end)
        elif stage == "dataset":
            dataset_view.refresh_dataset_view(client)

    return steps


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--intervals", required=True, help="Comma-separated intervals, e.g. 60,300,900")
    parser.add_argument("--start", help="Optional fixed start for all intervals")
    parser.add_argument("--end", help="Optional fixed end for all intervals")
    parser.add_argument("--bootstrap-start", help="Fallback start when an interval has no history")
    parser.add_argument("--overlap-bars", type=int, default=120)
    parser.add_argument("--safety-lag-seconds", type=int, default=120)
    parser.add_argument("--skip-dataset-refresh", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    steps = run_incremental_backfill(
        parse_intervals(args.intervals),
        start=args.start,
        end=args.end,
        bootstrap_start=args.bootstrap_start,
        overlap_bars=args.overlap_bars,
        safety_lag_seconds=args.safety_lag_seconds,
        refresh_dataset=not args.skip_dataset_refresh,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        for stage, interval, stage_start, stage_end in steps:
            interval_text = "-" if interval is None else str(interval)
            print(f"{stage:<7} interval={interval_text:<4} start={stage_start} end={stage_end}")


if __name__ == "__main__":
    main()
