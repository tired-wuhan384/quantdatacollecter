from pipelines.dataset_view import build_dataset_view_sql


def test_build_dataset_view_sql_aliases_key_columns_for_clickhouse_view_schema():
    sql = build_dataset_view_sql()

    assert "f.exchange AS exchange" in sql
    assert "f.symbol AS symbol" in sql
    assert "f.bar_interval_sec AS bar_interval_sec" in sql
    assert "f.bar_time AS bar_time" in sql
    assert "f.funding_rate AS funding_rate" in sql
