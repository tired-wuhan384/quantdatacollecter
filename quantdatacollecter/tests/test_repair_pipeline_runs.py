from pipelines.repair_pipeline_runs import build_repair_summary_sql


def test_build_repair_summary_sql_filters_running_rows_and_optional_layer():
    sql = build_repair_summary_sql(max_age_minutes=45, layer="feature")

    assert "FROM meta.pipeline_runs" in sql
    assert "status = 'running'" in sql
    assert "started_at < now() - INTERVAL 45 MINUTE" in sql
    assert "layer = 'feature'" in sql
