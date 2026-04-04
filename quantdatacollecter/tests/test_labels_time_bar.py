from pipelines.labels_time_bar import build_label_insert_sql, build_label_quality_check_sql


def test_build_label_insert_sql_marks_tail_rows_invalid():
    sql = build_label_insert_sql(interval_sec=60)

    assert "INSERT INTO label.labels_time_bar (" in sql
    assert "leadInFrame(toNullable(close), 5) OVER base_w AS close_t5" in sql
    assert "AS is_valid" in sql


def test_build_label_insert_sql_precomputes_forward_closes_before_future_windows():
    sql = build_label_insert_sql(interval_sec=60)

    assert "leadInFrame(toNullable(close), 1) OVER base_w AS close_t1" in sql
    assert "ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING" in sql
    assert "stddevSamp(leadInFrame(close, 1) OVER w / nullIf(close, 0) - 1) OVER w5" not in sql


def test_build_label_insert_sql_scopes_future_windows_on_the_select_that_uses_them():
    sql = build_label_insert_sql(interval_sec=60)

    assert ") AS hist_ready\n        WINDOW" in sql
    assert "w5 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING)" in sql
    assert "w10 AS (PARTITION BY exchange, symbol, bar_interval_sec ORDER BY bar_time ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING)" in sql
    assert ") AS prepared\n            WINDOW" not in sql


def test_build_label_insert_sql_uses_lookforward_buffer_for_time_range():
    sql = build_label_insert_sql(
        interval_sec=60,
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T01:00:00Z",
    )

    assert "bar_time >= toDateTime64('2026-03-02 00:00:00.000', 3, 'UTC')" in sql
    assert "bar_time < toDateTime64('2026-03-02 02:00:00.000', 3, 'UTC')" in sql
    assert "WHERE bar_time >= toDateTime64('2026-03-02 00:00:00.000', 3, 'UTC')" in sql
    assert "AND bar_time < toDateTime64('2026-03-02 01:00:00.000', 3, 'UTC')" in sql


def test_build_label_insert_sql_computes_windowed_labels_inside_scored_subquery():
    sql = build_label_insert_sql(interval_sec=60)

    assert "if(isNotNull(close_t1), close_t1 / nullIf(close, 0) - 1, CAST(NULL, 'Nullable(Float64)')) AS fwd_return_1" in sql
    assert "max(high) OVER w5" in sql
    assert "SELECT\n    exchange,\n    symbol,\n    bar_interval_sec,\n    bar_time,\n    fwd_return_1," in sql
    assert ") AS labeled\nWHERE 1" in sql
    assert ") AS scored\nWHERE 1" not in sql


def test_build_label_insert_sql_uses_timestamp_versions():
    sql = build_label_insert_sql(interval_sec=60)

    assert "toUInt64(toUnixTimestamp(now())) AS _version" in sql


def test_build_label_insert_sql_uses_explicit_forward_return_series_for_future_vol():
    sql = build_label_insert_sql(interval_sec=60)

    assert "if(isNotNull(close_t1), close_t1 / nullIf(close, 0) - 1, CAST(NULL, 'Nullable(Float64)')) AS next_bar_return" in sql
    assert "stddevSamp(next_bar_return) OVER rv5 AS fwd_realized_vol_5" in sql
    assert "ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING" in sql


def test_build_label_insert_sql_uses_forward_bar_count_for_validity():
    sql = build_label_insert_sql(interval_sec=60)

    assert "count() OVER fwd_count_60 AS fwd_bar_count" in sql
    assert "if(fwd_bar_count = 60 AND isNotNull(close_t60), 1, 0) AS is_valid" in sql
    assert "if(close_t60 IS NULL, 0, 1) AS is_valid" not in sql


def test_build_label_insert_sql_base_window_includes_following_rows_for_lead():
    sql = build_label_insert_sql(interval_sec=60)

    assert "base_w AS (" in sql
    assert "PARTITION BY exchange, symbol, bar_interval_sec" in sql
    assert "ORDER BY bar_time" in sql
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in sql


def test_build_label_insert_sql_uses_dynamic_thresholds_for_3class_labels():
    sql = build_label_insert_sql(interval_sec=60)

    assert "stddevSamp(next_bar_return) OVER hist_w AS hist_vol" in sql
    assert "if(isFinite(hist_vol), hist_vol * 0.5, 0.0) AS label_threshold_5" in sql
    assert "if(isFinite(hist_vol), hist_vol * 0.7, 0.0) AS label_threshold_10" in sql
    assert "multiIf(" in sql
    assert "fwd_return_5 > label_threshold_5" in sql
    assert "fwd_return_10 < -label_threshold_10" in sql


def test_build_label_insert_sql_makes_mae_mfe_directional_instead_of_duplicates():
    sql = build_label_insert_sql(interval_sec=60)

    assert "fwd_direction_5 > 0, down_5" in sql
    assert "fwd_direction_10 > 0, down_10" in sql
    assert "greatest(1 - min(low) OVER w5 / nullIf(close, 0), 0.0) AS mae_5" not in sql
    assert "greatest(max(high) OVER w5 / nullIf(close, 0) - 1, 0.0) AS mfe_5" not in sql


def test_build_label_insert_sql_includes_triple_barrier_signal():
    sql = build_label_insert_sql(interval_sec=60)

    assert "triple_barrier_signal" in sql
    assert "arrayFirstIndex(" in sql
    assert "groupArray(20)(high) OVER w20" in sql
    assert "groupArray(20)(low) OVER w20" in sql
    assert "ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING" in sql


def test_build_label_quality_check_sql_flags_invalid_tail_rows():
    sql = build_label_quality_check_sql(
        interval_sec=60,
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T01:00:00Z",
    )

    assert "FROM label.labels_time_bar" in sql
    assert "bar_interval_sec = 60" in sql
    assert "sum(is_valid = 0)" in sql
    assert "2026-03-02 00:00:00.000" in sql
    assert "2026-03-02 01:00:00.000" in sql
