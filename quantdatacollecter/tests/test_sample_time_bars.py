import pytest

from pipelines import sample_time_bars
from pipelines.sample_time_bars import (
    build_rollup_gap_check_sql,
    build_rollup_insert_sql,
    build_trade_bar_insert_sql,
)


def test_build_trade_bar_insert_sql_uses_raw_trades_for_1m():
    sql = build_trade_bar_insert_sql(interval_sec=60)

    assert "INSERT INTO sample.bars_time (" in sql
    assert "FROM raw.trades" in sql
    assert "60 AS bar_interval_sec" in sql


def test_build_trade_bar_insert_sql_uses_explicit_target_columns():
    sql = build_trade_bar_insert_sql(interval_sec=60)

    assert "signed_sqrt_dollar_volume," in sql
    assert "is_time_bar_complete," in sql
    assert "has_book_data," in sql
    assert "first_trade_ts," in sql
    assert "last_trade_ts," in sql
    assert "max_trade_gap_ms," in sql
    assert "_version" in sql


def test_build_trade_bar_insert_sql_aggregates_signed_sqrt_dollar_volume():
    sql = build_trade_bar_insert_sql(interval_sec=60)

    assert "sumIf(sqrt(notional), side = 'buy') - sumIf(sqrt(notional), side = 'sell') AS signed_sqrt_dollar_volume" in sql
    assert "ifNull(t.signed_sqrt_dollar_volume, 0.0) AS signed_sqrt_dollar_volume" in sql


def test_build_trade_bar_insert_sql_joins_enriched_side_inputs():
    sql = build_trade_bar_insert_sql(interval_sec=60)

    assert "FROM raw.book_topk_raw" in sql
    assert "FROM raw.open_interest" in sql
    assert "FROM raw.funding" in sql
    assert "FROM raw.liquidations" in sql
    assert "LEFT JOIN book_bars AS b" in sql
    assert "LEFT JOIN oi_bars AS oi" in sql
    assert "LEFT JOIN funding_bars AS f" in sql
    assert "LEFT JOIN liquidation_bars AS l" in sql
    assert "ifNull(b.ob_snapshot_count, 0)" in sql
    assert "argMax(rate, timestamp) AS funding_rate" in sql


def test_build_trade_bar_insert_sql_adds_quality_flags_for_side_inputs():
    sql = build_trade_bar_insert_sql(interval_sec=60)

    assert "AS has_book_data" in sql
    assert "AS has_open_interest_data" in sql
    assert "AS has_funding_data" in sql
    assert "AS is_time_bar_complete" in sql


def test_build_trade_bar_insert_sql_applies_time_filters_to_raw_sources():
    sql = build_trade_bar_insert_sql(
        interval_sec=60,
        start="2026-03-01T00:00:00Z",
        end="2026-03-02T00:00:00Z",
    )

    assert "timestamp >= toDateTime64('2026-03-01 00:00:00.000', 3, 'UTC')" in sql
    assert "timestamp < toDateTime64('2026-03-02 00:00:00.000', 3, 'UTC')" in sql


def test_build_trade_bar_insert_sql_builds_time_grid_when_range_is_provided():
    sql = build_trade_bar_insert_sql(
        interval_sec=60,
        start="2026-03-01T00:00:00Z",
        end="2026-03-01T00:05:00Z",
    )

    assert "time_grid AS (" in sql
    assert "CROSS JOIN numbers(5)" in sql
    assert "grid_with_prev_close AS (" in sql
    assert "ASOF LEFT JOIN trade_bars_ordered AS prev_t" in sql


def test_build_rollup_insert_sql_uses_sample_bars_for_5m():
    sql = build_rollup_insert_sql(interval_sec=300)

    assert "INSERT INTO sample.bars_time (" in sql
    assert "FROM sample.bars_time" in sql
    assert "300 AS bar_interval_sec" in sql
    assert "WHERE bar_interval_sec = 60" in sql


def test_build_rollup_insert_sql_applies_time_filters_to_1m_source():
    sql = build_rollup_insert_sql(
        interval_sec=300,
        start="2026-03-01T00:00:00Z",
        end="2026-03-02T00:00:00Z",
    )

    assert "bar_time >= toDateTime64('2026-03-01 00:00:00.000', 3, 'UTC')" in sql
    assert "bar_time < toDateTime64('2026-03-02 00:00:00.000', 3, 'UTC')" in sql


def test_build_rollup_insert_sql_avoids_nested_aggregate_alias_reuse():
    sql = build_rollup_insert_sql(interval_sec=300)

    assert "sum(turnover) AS turnover_sum" in sql
    assert "sum(volume) AS volume_sum" in sql
    assert "sum(signed_sqrt_dollar_volume) AS signed_sqrt_dollar_volume_sum" in sql
    assert "signed_sqrt_dollar_volume_sum AS signed_sqrt_dollar_volume" in sql
    assert "turnover_sum / nullIf(volume_sum, 0) AS vwap" in sql


def test_build_rollup_insert_sql_weights_book_averages_by_snapshot_count():
    sql = build_rollup_insert_sql(interval_sec=300)

    assert "sum(avg_spread_bps * ob_snapshot_count)" in sql
    assert "sum(avg_mid_price * ob_snapshot_count)" in sql
    assert "sum(avg_microprice * ob_snapshot_count)" in sql
    assert "nullIf(sum(ob_snapshot_count), 0)" in sql


def test_build_rollup_insert_sql_rolls_up_bar_completeness_and_source_flags():
    sql = build_rollup_insert_sql(interval_sec=300)

    assert "if(count() = 5 AND min(is_time_bar_complete) = 1, 1, 0) AS is_time_bar_complete_value" in sql
    assert "max(has_book_data) AS has_book_data_value" in sql
    assert "max(has_open_interest_data) AS has_open_interest_data_value" in sql
    assert "max(has_funding_data) AS has_funding_data_value" in sql


def test_build_rollup_insert_sql_keeps_partial_rollups_instead_of_dropping_them():
    sql = build_rollup_insert_sql(interval_sec=300)

    assert "HAVING count() >= 1" in sql


def test_build_rollup_gap_check_sql_uses_expected_source_multiple():
    sql = build_rollup_gap_check_sql(interval_sec=300)

    assert "FROM sample.bars_time" in sql
    assert "bar_interval_sec = 60" in sql
    assert "INTERVAL 300 SECOND" in sql
    assert "HAVING count() != 5" in sql


def test_run_rejects_unsupported_interval(monkeypatch):
    monkeypatch.setattr(sample_time_bars, "get_client", lambda: object())
    monkeypatch.setattr(sample_time_bars, "execute_pipeline_sql", lambda *args, **kwargs: None)

    with pytest.raises(ValueError, match="Unsupported interval"):
        sample_time_bars.run(90)
