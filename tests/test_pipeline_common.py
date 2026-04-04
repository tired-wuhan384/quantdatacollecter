import pytest

from pipelines.common import build_repair_stale_runs_sql, execute_pipeline_sql


class FakeClient:
    def __init__(self, fail=False):
        self.fail = fail
        self.commands = []

    def command(self, sql):
        self.commands.append(sql)
        if self.fail and "SELECT 1" in sql:
            raise RuntimeError("boom")


def test_execute_pipeline_sql_marks_failed_runs_on_error():
    client = FakeClient(fail=True)

    with pytest.raises(RuntimeError):
        execute_pipeline_sql(client, "feature", "features_time_bar", 60, "SELECT 1")

    assert any("status = 'failed'" in sql for sql in client.commands)


def test_execute_pipeline_sql_records_requested_time_range():
    client = FakeClient()

    execute_pipeline_sql(
        client,
        "feature",
        "features_time_bar",
        60,
        "SELECT 2",
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T01:00:00Z",
    )

    insert_sql = client.commands[0]
    assert "2026-03-02 00:00:00" in insert_sql
    assert "2026-03-02 01:00:00" in insert_sql


def test_build_repair_stale_runs_sql_marks_old_running_rows_failed():
    sql = build_repair_stale_runs_sql(max_age_minutes=30, layer="label")

    assert "status = 'failed'" in sql
    assert "status = 'running'" in sql
    assert "started_at < now() - INTERVAL 30 MINUTE" in sql
    assert "layer = 'label'" in sql
