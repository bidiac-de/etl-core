import gzip
from pathlib import Path

import pandas as pd
import pytest

import etl_core.receivers.files.json.json_helper as J


def test_atomic_write_cleanup_exception(tmp_path, monkeypatch):
    target = tmp_path / "out.json"

    monkeypatch.setattr(
        Path,
        "unlink",
        lambda self, missing_ok=False: (_ for _ in ()).throw(RuntimeError("nope")),
    )

    J.dump_records_auto(target, [{"ok": 1}])
    assert target.read_text(encoding="utf-8") == '[\n  {\n    "ok": 1\n  }\n]'


def test_to_json_safe_scalar_defensive_excepts(monkeypatch):
    monkeypatch.setattr(
        J.math, "isnan", lambda v: (_ for _ in ()).throw(ValueError("boom"))
    )  # type: ignore[attr-defined]
    monkeypatch.setattr(
        J.pd, "isna", lambda v: (_ for _ in ()).throw(RuntimeError("nope"))
    )  # type: ignore[attr-defined]

    obj = object()
    assert J._to_json_safe_scalar(obj) is obj


def test_load_json_records_ndjson_fast_path(tmp_path):
    p = tmp_path / "items.jsonl"
    p.write_text('{"a":1}\n2\n\n{"b":3}\nBAD\n', encoding="utf-8")
    out = J.load_json_records(p)
    assert out == [{"a": 1}, {"_value": 2}, {"b": 3}]


def test_read_json_row_empty_file(tmp_path):
    p = tmp_path / "empty.json"
    p.write_text("", encoding="utf-8")
    assert list(J.read_json_row(p)) == []


def test_read_json_row_invalid_top_level(tmp_path):
    p = tmp_path / "scalar.json"
    p.write_text("42", encoding="utf-8")
    with pytest.raises(ValueError):
        list(J.read_json_row(p))


def test_read_json_row_array_incremental(tmp_path):
    data = '[   \n  {"x":1},  {"y":2}  \n]'
    p = tmp_path / "arr.json"
    p.write_text(data, encoding="utf-8")
    rows = list(J.read_json_row(p, chunk_size=3))
    assert rows == [{"x": 1}, {"y": 2}]


def test_read_json_row_single_object_incremental(tmp_path):
    p = tmp_path / "one.json"
    p.write_text('{"a": 1, "b": 2}', encoding="utf-8")
    out = list(J.read_json_row(p, chunk_size=4))
    assert out == [{"a": 1, "b": 2}]


def test_unflatten_record_nested_list_growth():
    flat = {"a[1][2]": "v"}
    out = J.unflatten_record(flat)
    assert out == {"a": [None, None, "v"]}


def test_flatten_partition_empty_and_nested():
    empty = pd.DataFrame()
    assert J._flatten_partition(empty).empty

    df = pd.DataFrame([{"x": {"y": 1}, "z": [2, 3], "w": "ok"}])
    got = J._flatten_partition(df)
    assert set(got.columns) == {"x.y", "z[0]", "z[1]", "w"}
    assert (
        got.loc[0, "x.y"] == 1
        and got.loc[0, "z[0]"] == 2
        and got.loc[0, "z[1]"] == 3
        and got.loc[0, "w"] == "ok"
    )


def test_stream_json_array_to_ndjson_on_error(tmp_path, monkeypatch):
    src = tmp_path / "src.json"
    dst = tmp_path / "dst.jsonl"
    src.write_text("[1, 2, 3]", encoding="utf-8")

    orig_append = J.append_ndjson_record

    calls: list[int] = []

    def flaky_append(path, rec):
        calls.append(1)
        if len(calls) == 1:
            raise IOError("disk full")
        return orig_append(path, rec)

    monkeypatch.setattr(J, "append_ndjson_record", flaky_append)

    errors: list[str] = []
    n = J.stream_json_array_to_ndjson(
        src,
        dst,
        on_error=lambda e: errors.append(str(e)),
        chunk_size=2,
    )

    assert n == 2
    assert len(errors) == 1

    lines = [
        ln.strip() for ln in dst.read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert lines == ['{"_value": 2}', '{"_value": 3}']


def test_open_text_auto_gzip_roundtrip(tmp_path):
    gz = tmp_path / "x.json.gz"
    with gzip.open(gz, "wt", encoding="utf-8") as f:
        f.write("[1]\n")
    with J.open_text_auto(gz, "rt") as f:
        assert f.read() == "[1]\n"
