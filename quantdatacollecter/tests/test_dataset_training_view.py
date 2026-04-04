from pipelines.dataset_view import (
    build_dataset_select_sql,
    build_dataset_view_sql,
    get_dataset_select_columns,
)


def test_build_dataset_view_sql_joins_sample_and_adds_training_flags():
    sql = build_dataset_view_sql()

    assert "sample.bars_time AS s FINAL" in sql
    assert "AS sample_has_trades" in sql
    assert "AS sample_has_book" in sql
    assert "AS sample_is_time_bar_complete" in sql
    assert "AS feature_window_ready" in sql
    assert "AS training_ready" in sql


def test_build_dataset_view_sql_uses_final_instead_of_argmax_subqueries():
    sql = build_dataset_view_sql()

    assert "feature.features_time_bar AS f FINAL" in sql
    assert "label.labels_time_bar AS l FINAL" in sql
    assert "sample.bars_time AS s FINAL" in sql
    assert "argMax(" not in sql


def test_get_dataset_select_columns_supports_train_and_all_presets():
    train_columns = get_dataset_select_columns("train")
    all_columns = get_dataset_select_columns("all")

    assert "log_return" in train_columns
    assert "fwd_return_5" in train_columns
    assert "training_ready" not in train_columns
    assert "training_ready" in all_columns
    assert "sample_has_book" in all_columns


def test_build_dataset_select_sql_can_filter_training_ready_rows_and_use_whitelist():
    sql = build_dataset_select_sql(
        interval_sec=60,
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T01:00:00Z",
        training_ready_only=True,
        column_set="train",
    )

    assert "FROM feature.features_time_bar AS f FINAL" in sql
    assert "training_ready = 1" in sql
    outer_select = sql.split("FROM (", 1)[0]
    assert "label_is_valid" not in outer_select
    assert "sample_has_book" not in outer_select
    assert "log_return" in sql
    assert "fwd_return_5" in sql


def test_build_dataset_view_sql_requires_book_data_and_complete_time_bars_for_training():
    sql = build_dataset_view_sql()

    assert "s.has_book_data = 1" in sql
    assert "s.is_time_bar_complete = 1" in sql


def test_build_dataset_view_sql_keeps_invalid_labels_available_for_inference():
    sql = build_dataset_view_sql()

    assert "WHERE l.is_valid = 1" not in sql


def test_build_dataset_select_sql_can_optionally_filter_valid_labels():
    sql = build_dataset_select_sql(
        interval_sec=60,
        label_valid_only=True,
    )

    assert "label_is_valid = 1" in sql
