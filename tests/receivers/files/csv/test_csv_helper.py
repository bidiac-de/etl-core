from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import dask.dataframe as dd
import pandas as pd

import etl_core.receivers.files.csv.csv_helper as h


def _make_csv(tmp_path: Path, content: str) -> Path:
    f = tmp_path / "test.csv"
    f.write_text(content)
    return f


def test_read_csv_row_and_bulk_and_bigdata(tmp_path: Path) -> None:
    content = "id,name\n1,Alice\n2,Bob\n"
    path = _make_csv(tmp_path, content)

    rows = list(h.read_csv_row(path, ","))
    assert rows == [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]

    df = h.read_csv_bulk(path, ",")
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {"id", "name"}
    assert len(df) == 2

    ddf = h.read_csv_bigdata(path, ",", blocksize="1MB")
    assert isinstance(ddf, dd.DataFrame)
    pdf = ddf.compute()

    pd.testing.assert_frame_equal(
        pdf.reset_index(drop=True),
        df.reset_index(drop=True),
        check_dtype=False,
    )


def test_write_csv_row_creates_and_appends(tmp_path: Path) -> None:
    path = tmp_path / "rows.csv"
    h.write_csv_row(path, {"a": 1, "b": 2}, ",")
    assert path.exists()
    text1 = path.read_text().strip().splitlines()
    assert text1[0] == "a,b"
    assert text1[1] == "1,2"

    h.write_csv_row(path, {"a": 3, "b": 4}, ",")
    text2 = path.read_text().strip().splitlines()
    assert text2[-1] == "3,4"


def test_write_csv_bulk_and_empty(tmp_path: Path) -> None:
    df = pd.DataFrame([{"x": 10, "y": 11}, {"x": 12, "y": 13}])
    path = tmp_path / "bulk.csv"
    h.write_csv_bulk(path, df, ",")
    out = pd.read_csv(path)
    pd.testing.assert_frame_equal(out, df)

    empty = pd.DataFrame(columns=["x", "y"])
    path2 = tmp_path / "empty.csv"
    h.write_csv_bulk(path2, empty, ",")
    assert path2.exists()
    assert path2.read_text() == ""


def test_write_csv_bigdata_variants(tmp_path: Path) -> None:
    df = pd.DataFrame([{"x": 1, "y": 2}, {"x": 3, "y": 4}])
    ddf = dd.from_pandas(df, npartitions=1)
    out_path = tmp_path / "big.csv"
    h.write_csv_bigdata(out_path, ddf, ",")
    out = pd.read_csv(out_path)
    pd.testing.assert_frame_equal(out, df)


def test_write_csv_bigdata_handles_various_result_shapes(
    tmp_path: Path, monkeypatch
) -> None:
    df = pd.DataFrame([{"a": 1}])
    ddf = dd.from_pandas(df, npartitions=1)
    path = tmp_path / "result.csv"

    results = [ddf]

    def fake_to_csv(*_, **__):
        return results

    monkeypatch.setattr(h.dd.DataFrame, "to_csv", fake_to_csv)

    called: Dict[str, Any] = {}

    def fake_compute(*a, **k):
        called["yes"] = True
        return None

    monkeypatch.setattr(h.dd, "compute", fake_compute)
    h.write_csv_bigdata(path, ddf, ",")
    assert "yes" in called


def test_write_csv_bigdata_result_with_compute_method(
    tmp_path: Path, monkeypatch
) -> None:
    df = pd.DataFrame([{"a": 5}])
    ddf = dd.from_pandas(df, npartitions=1)
    path = tmp_path / "file.csv"

    class Dummy:
        def compute(self):
            Dummy.computed = True

    def fake_to_csv(*_, **__):
        return Dummy()

    monkeypatch.setattr(h.dd.DataFrame, "to_csv", fake_to_csv)
    h.write_csv_bigdata(path, ddf, ",")
    assert getattr(Dummy, "computed", False)


def test_write_csv_bigdata_result_none_does_nothing(
    tmp_path: Path, monkeypatch
) -> None:
    df = pd.DataFrame([{"a": 1}])
    ddf = dd.from_pandas(df, npartitions=1)
    path = tmp_path / "noop.csv"

    def fake_to_csv(*_, **__):
        return None

    monkeypatch.setattr(h.dd.DataFrame, "to_csv", fake_to_csv)
    h.write_csv_bigdata(path, ddf, ",")
    assert not path.exists()
