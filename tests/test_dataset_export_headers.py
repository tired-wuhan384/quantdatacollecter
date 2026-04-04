from pathlib import Path

from pipelines.dataset_view import export_dataset_csv


class EmptyResult:
    column_names = ()
    result_rows = []


class FakeClient:
    pass


def test_export_dataset_csv_writes_whitelist_header_for_empty_results(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("pipelines.dataset_view.query_dataset", lambda client, **kwargs: EmptyResult())
    output_path = tmp_path / "dataset.csv"

    export_dataset_csv(FakeClient(), str(output_path), column_set="train", training_ready_only=True)

    header = output_path.read_text(encoding="utf-8").splitlines()[0]
    assert "exchange" in header
    assert "symbol" in header
    assert "fwd_return_5" in header
    assert "training_ready" not in header


def test_export_dataset_csv_defaults_to_valid_labels(monkeypatch, tmp_path: Path):
    captured_kwargs = {}

    def fake_query_dataset(client, **kwargs):
        captured_kwargs.update(kwargs)
        return EmptyResult()

    monkeypatch.setattr("pipelines.dataset_view.query_dataset", fake_query_dataset)
    output_path = tmp_path / "dataset.csv"

    export_dataset_csv(FakeClient(), str(output_path))

    assert captured_kwargs["training_ready_only"] is True
    assert captured_kwargs["label_valid_only"] is True
