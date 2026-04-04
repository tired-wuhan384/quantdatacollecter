from pipelines.backfill_time_bar_pipeline import build_backfill_steps


def test_build_backfill_steps_orders_sample_feature_label_and_dataset():
    steps = build_backfill_steps(
        intervals=[60, 300],
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T01:00:00Z",
        refresh_dataset=True,
    )

    assert steps == [
        ("sample", 60, "2026-03-02T00:00:00Z", "2026-03-02T01:00:00Z"),
        ("sample", 300, "2026-03-02T00:00:00Z", "2026-03-02T01:00:00Z"),
        ("feature", 60, "2026-03-02T00:00:00Z", "2026-03-02T01:00:00Z"),
        ("label", 60, "2026-03-02T00:00:00Z", "2026-03-02T01:00:00Z"),
        ("feature", 300, "2026-03-02T00:00:00Z", "2026-03-02T01:00:00Z"),
        ("label", 300, "2026-03-02T00:00:00Z", "2026-03-02T01:00:00Z"),
        ("dataset", None, "2026-03-02T00:00:00Z", "2026-03-02T01:00:00Z"),
    ]


def test_build_backfill_steps_adds_base_1m_sampling_for_rollups():
    steps = build_backfill_steps(
        intervals=[900],
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T03:00:00Z",
        refresh_dataset=False,
    )

    assert steps[0] == ("sample", 60, "2026-03-02T00:00:00Z", "2026-03-02T03:00:00Z")
    assert ("sample", 900, "2026-03-02T00:00:00Z", "2026-03-02T03:00:00Z") in steps
    assert ("dataset", None, "2026-03-02T00:00:00Z", "2026-03-02T03:00:00Z") not in steps
