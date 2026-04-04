from datetime import datetime, timezone

import pytest

from pipelines.incremental_backfill_time_bar_pipeline import (
    build_incremental_steps,
    compute_interval_window,
)


def _dt(year: int, month: int, day: int, hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def test_compute_interval_window_uses_min_layer_watermark_minus_overlap():
    start, end = compute_interval_window(
        interval_sec=60,
        sample_max=_dt(2026, 4, 4, 10, 0),
        feature_max=_dt(2026, 4, 4, 9, 58),
        label_max=_dt(2026, 4, 4, 9, 59),
        overlap_bars=2,
        bootstrap_start=None,
        start_override=None,
        end_override="2026-04-04T10:10:00Z",
        safety_lag_seconds=120,
        now_utc=lambda: _dt(2026, 4, 4, 10, 20),
    )

    assert start == "2026-04-04T09:56:00Z"
    assert end == "2026-04-04T10:10:00Z"


def test_compute_interval_window_uses_bootstrap_start_when_no_watermark():
    start, end = compute_interval_window(
        interval_sec=300,
        sample_max=None,
        feature_max=None,
        label_max=None,
        overlap_bars=5,
        bootstrap_start="2026-04-01T00:00:00Z",
        start_override=None,
        end_override="2026-04-04T00:00:00Z",
        safety_lag_seconds=120,
        now_utc=lambda: _dt(2026, 4, 4, 1, 0),
    )

    assert start == "2026-04-01T00:00:00Z"
    assert end == "2026-04-04T00:00:00Z"


def test_compute_interval_window_requires_start_when_no_history():
    with pytest.raises(ValueError, match="bootstrap-start"):
        compute_interval_window(
            interval_sec=900,
            sample_max=None,
            feature_max=None,
            label_max=None,
            overlap_bars=5,
            bootstrap_start=None,
            start_override=None,
            end_override="2026-04-04T00:00:00Z",
            safety_lag_seconds=120,
            now_utc=lambda: _dt(2026, 4, 4, 1, 0),
        )


def test_compute_interval_window_uses_safety_lag_when_end_not_provided():
    start, end = compute_interval_window(
        interval_sec=60,
        sample_max=_dt(2026, 4, 4, 10, 0),
        feature_max=_dt(2026, 4, 4, 10, 0),
        label_max=_dt(2026, 4, 4, 10, 0),
        overlap_bars=1,
        bootstrap_start=None,
        start_override=None,
        end_override=None,
        safety_lag_seconds=180,
        now_utc=lambda: _dt(2026, 4, 4, 10, 5),
    )

    assert start == "2026-04-04T09:59:00Z"
    assert end == "2026-04-04T10:02:00Z"


def test_build_incremental_steps_runs_sample_60_once_with_earliest_start():
    windows = {
        60: ("2026-04-04T09:58:00Z", "2026-04-04T10:10:00Z"),
        300: ("2026-04-04T09:40:00Z", "2026-04-04T10:10:00Z"),
        900: ("2026-04-04T09:00:00Z", "2026-04-04T10:10:00Z"),
    }

    steps = build_incremental_steps(windows, refresh_dataset=True)

    assert steps[0] == ("sample", 60, "2026-04-04T09:00:00Z", "2026-04-04T10:10:00Z")
    assert ("sample", 300, "2026-04-04T09:40:00Z", "2026-04-04T10:10:00Z") in steps
    assert ("sample", 900, "2026-04-04T09:00:00Z", "2026-04-04T10:10:00Z") in steps
    assert steps[-1] == ("dataset", None, None, None)
