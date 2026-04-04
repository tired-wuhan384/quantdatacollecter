from pipelines.dataset_view import (
    build_dataset_row_count_check_sql,
    build_dataset_select_sql,
    build_dataset_view_sql,
)


def test_build_dataset_view_sql_joins_on_full_primary_key():
    sql = build_dataset_view_sql()

    assert "f.exchange = l.exchange" in sql
    assert "f.symbol = l.symbol" in sql
    assert "f.bar_interval_sec = l.bar_interval_sec" in sql
    assert "f.bar_time = l.bar_time" in sql


def test_build_dataset_select_sql_applies_interval_and_time_filters():
    sql = build_dataset_select_sql(
        interval_sec=60,
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T01:00:00Z",
        exchange="BINANCE_FUTURES",
        symbol="BTC-USDT-PERP",
        limit=100,
    )

    assert "FROM feature.features_time_bar AS f FINAL" in sql
    assert "FROM dataset.dataset_time_bar" not in sql
    assert "f.bar_interval_sec = 60" in sql
    assert "l.bar_interval_sec = 60" in sql
    assert "s.bar_interval_sec = 60" in sql
    assert "f.bar_time >= toDateTime64('2026-03-02 00:00:00.000', 3, 'UTC')" in sql
    assert "l.bar_time >= toDateTime64('2026-03-02 00:00:00.000', 3, 'UTC')" in sql
    assert "s.bar_time >= toDateTime64('2026-03-02 00:00:00.000', 3, 'UTC')" in sql
    assert "f.bar_time < toDateTime64('2026-03-02 01:00:00.000', 3, 'UTC')" in sql
    assert "f.exchange = 'BINANCE_FUTURES'" in sql
    assert "f.symbol = 'BTC-USDT-PERP'" in sql
    assert "LIMIT 100" in sql


def test_build_dataset_row_count_check_sql_compares_dataset_against_valid_labels():
    sql = build_dataset_row_count_check_sql(
        interval_sec=60,
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T01:00:00Z",
    )

    assert "FROM feature.features_time_bar AS f FINAL" in sql
    assert "FROM label.labels_time_bar AS l FINAL" in sql
    assert "f.bar_interval_sec = 60" in sql
    assert "l.is_valid = 1" in sql
    assert "2026-03-02 00:00:00.000" in sql
    assert "2026-03-02 01:00:00.000" in sql
