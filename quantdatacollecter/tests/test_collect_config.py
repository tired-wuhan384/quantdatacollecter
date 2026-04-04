from collect import CLICKHOUSE


def test_collect_defaults_to_raw_database():
    assert CLICKHOUSE["database"] == "raw"
