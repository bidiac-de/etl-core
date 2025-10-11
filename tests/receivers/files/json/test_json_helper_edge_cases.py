import json
from pathlib import Path
import pandas as pd
import pytest
import gzip

from etl_core.receivers.files.json.json_helper import (
    _to_json_safe_scalar,
    _sanitize_for_json,
    build_payload,
    ensure_nested_for_read,
    flatten_record,
    unflatten_record,
    stream_json_array_to_ndjson,
    _parse_path_escaped,
    _escape_key,
    _unescape_key,
    read_json_row,
    iter_ndjson_lenient,
    load_json_records,
    append_ndjson_record,
    dump_records_auto,
    _atomic_write_textfile,
    open_text_auto,
    is_ndjson_path,
)


class TestJsonSafeScalar:
    """Test JSON safety functions for edge cases."""

    def test_to_json_safe_scalar_nan(self):
        """Test NaN handling in JSON serialization."""
        result = _to_json_safe_scalar(float("nan"))
        assert result is None

    def test_to_json_safe_scalar_inf(self):
        """Test infinity handling in JSON serialization."""
        result = _to_json_safe_scalar(float("inf"))
        assert result is None
        result = _to_json_safe_scalar(float("-inf"))
        assert result is None

    def test_to_json_safe_scalar_pandas_na(self):
        """Test pandas NA handling."""
        result = _to_json_safe_scalar(pd.NA)
        assert result is None

    def test_to_json_safe_scalar_normal_values(self):
        """Test normal values pass through unchanged."""
        assert _to_json_safe_scalar(42) == 42
        assert _to_json_safe_scalar("hello") == "hello"
        assert _to_json_safe_scalar(3.14) == 3.14
        assert _to_json_safe_scalar(None) is None

    def test_sanitize_for_json_nested(self):
        """Test sanitization of nested structures."""
        data = {
            "normal": 42,
            "nan_val": float("nan"),
            "inf_val": float("inf"),
            "nested": {"pandas_na": pd.NA, "normal": "test"},
            "list": [1, float("nan"), "ok"],
        }
        result = _sanitize_for_json(data)
        expected = {
            "normal": 42,
            "nan_val": None,
            "inf_val": None,
            "nested": {"pandas_na": None, "normal": "test"},
            "list": [1, None, "ok"],
        }
        assert result == expected


class TestBuildPayload:
    """Test payload building and flattening."""

    def test_build_payload_non_dict_raises(self):
        """Test that non-dict payloads raise TypeError."""
        with pytest.raises(TypeError, match="Expected dict payload"):
            build_payload("not a dict")

    def test_build_payload_flat_paths_unflattens(self):
        """Test that flat paths get unflattened."""
        flat = {"user.name": "John", "user.age": 30}
        result = build_payload(flat)
        expected = {"user": {"name": "John", "age": 30}}
        assert result == expected

    def test_build_payload_nested_passthrough(self):
        """Test that nested payloads pass through unchanged."""
        nested = {"user": {"name": "John", "age": 30}}
        result = build_payload(nested)
        assert result == nested

    def test_ensure_nested_for_read_flat_detection(self):
        """Test flat path detection for read operations."""
        flat = {"user.name": "John"}
        result = ensure_nested_for_read(flat)
        expected = {"user": {"name": "John"}}
        assert result == expected

    def test_ensure_nested_for_read_nested_passthrough(self):
        """Test nested data passes through unchanged."""
        nested = {"user": {"name": "John"}}
        result = ensure_nested_for_read(nested)
        assert result == nested


class TestFlattenUnflatten:
    """Test flattening and unflattening operations."""

    def test_flatten_record_complex(self):
        """Test flattening of complex nested structures."""
        data = {
            "user": {
                "name": "John",
                "address": {"street": "Main St", "city": "Anytown"},
            },
            "orders": [{"id": 1, "amount": 100}, {"id": 2, "amount": 200}],
        }
        result = flatten_record(data)
        expected = {
            "user.name": "John",
            "user.address.street": "Main St",
            "user.address.city": "Anytown",
            "orders[0].id": 1,
            "orders[0].amount": 100,
            "orders[1].id": 2,
            "orders[1].amount": 200,
        }
        assert result == expected

    def test_unflatten_record_complex(self):
        """Test unflattening of complex flat structures."""
        flat = {
            "user.name": "John",
            "user.address.street": "Main St",
            "orders[0].id": 1,
            "orders[1].amount": 200,
        }
        result = unflatten_record(flat)
        expected = {
            "user": {"name": "John", "address": {"street": "Main St"}},
            "orders": [{"id": 1}, {"amount": 200}],
        }
        assert result == expected

    def test_roundtrip_flatten_unflatten(self):
        """Test that flatten + unflatten preserves data."""
        original = {"a": {"b": {"c": 1}}, "d": [{"e": 2}, {"f": 3}], "g": "simple"}
        flattened = flatten_record(original)
        unflattened = unflatten_record(flattened)
        assert unflattened == original


class TestPathParsing:
    """Test path parsing and escaping."""

    def test_escape_key_special_chars(self):
        """Test escaping of special characters in keys."""
        assert _escape_key("normal") == "normal"
        assert _escape_key("with.dot") == "with\\.dot"
        assert _escape_key("with[bracket]") == "with\\[bracket\\]"
        assert _escape_key("with\\backslash") == "with\\\\backslash"

    def test_unescape_key_special_chars(self):
        """Test unescaping of special characters in keys."""
        assert _unescape_key("normal") == "normal"
        assert _unescape_key("with\\.dot") == "with.dot"
        assert _unescape_key("with\\[bracket\\]") == "with[bracket]"
        assert _unescape_key("with\\\\backslash") == "with\\backslash"

    def test_parse_path_escaped_complex(self):
        """Test parsing of complex escaped paths."""
        # Test with escaped dots and brackets
        path = "user\\.name\\[0\\]"
        result = _parse_path_escaped(path)
        expected = [("user.name[0]", None)]
        assert result == expected

    def test_parse_path_escaped_mixed(self):
        """Test parsing paths with mixed escaped and normal chars."""
        path = "user.name\\[special\\]"
        result = _parse_path_escaped(path)
        expected = [("user", None), ("name[special]", None)]
        assert result == expected


class TestJsonFileOperations:
    """Test JSON file operations and edge cases."""

    def test_is_ndjson_path_variants(self):
        """Test detection of various NDJSON file extensions."""
        assert is_ndjson_path(Path("test.jsonl"))
        assert is_ndjson_path(Path("test.ndjson"))
        assert is_ndjson_path(Path("test.jsonl.gz"))
        assert is_ndjson_path(Path("test.ndjson.gz"))
        assert not is_ndjson_path(Path("test.json"))
        assert not is_ndjson_path(Path("test.txt"))

    def test_open_text_auto_gzip(self, tmp_path: Path):
        """Test automatic gzip handling."""
        import gzip

        # Test gzip file
        gz_path = tmp_path / "test.json.gz"
        with gzip.open(gz_path, "wt", encoding="utf-8") as f:
            f.write('{"test": "data"}')

        with open_text_auto(gz_path, "rt") as f:
            content = f.read()
        assert content == '{"test": "data"}'

    def test_atomic_write_textfile_error_handling(self, tmp_path: Path):
        """Test atomic write with cleanup on error."""
        target_path = tmp_path / "target.txt"

        def failing_writer(tmp_path: Path):
            raise RuntimeError("Write failed")

        with pytest.raises(RuntimeError, match="Write failed"):
            _atomic_write_textfile(target_path, failing_writer)
        assert not target_path.exists()

    def test_iter_ndjson_lenient_error_handling(self, tmp_path: Path):
        """Test NDJSON iteration with malformed lines."""
        ndjson_path = tmp_path / "malformed.jsonl"
        lines = [
            '{"valid": "line1"}',
            "invalid json line",
            '{"valid": "line2"}',
            "",
            '{"valid": "line3"}',
        ]
        ndjson_path.write_text("\n".join(lines))

        errors = []

        def error_handler(exc):
            errors.append(exc)

        records = list(iter_ndjson_lenient(ndjson_path, on_error=error_handler))
        assert len(records) == 3
        assert len(errors) == 1
        assert isinstance(errors[0], json.JSONDecodeError)

    def test_iter_ndjson_lenient_non_dict_wrapping(self, tmp_path: Path):
        """Test that non-dict values get wrapped."""
        ndjson_path = tmp_path / "non_dict.jsonl"
        lines = ['{"dict": "value"}', '"string value"', "42", "null"]
        ndjson_path.write_text("\n".join(lines))

        records = list(iter_ndjson_lenient(ndjson_path))
        expected = [
            {"dict": "value"},
            {"_value": "string value"},
            {"_value": 42},
            {"_value": None},
        ]
        assert records == expected

    def test_stream_json_array_to_ndjson_error_handling(self, tmp_path: Path):
        """Test streaming conversion with error handling."""
        invalid_json = tmp_path / "invalid.json"
        invalid_json.write_text('{"not": "array"}')

        output_path = tmp_path / "output.jsonl"

        errors = []

        def error_handler(exc):
            errors.append(exc)

        with pytest.raises(ValueError, match="Source is not a JSON array"):
            stream_json_array_to_ndjson(
                invalid_json, output_path, on_error=error_handler
            )

    def test_read_json_row_invalid_format(self, tmp_path: Path):
        """Test reading invalid JSON format."""
        invalid_json = tmp_path / "invalid.json"
        invalid_json.write_text('"not object or array"')

        with pytest.raises(ValueError, match="Top-level JSON must be"):
            list(read_json_row(invalid_json))

    def test_load_json_records_empty_file(self, tmp_path: Path):
        """Test loading from empty file."""
        empty_file = tmp_path / "empty.json"
        empty_file.write_text("")

        result = load_json_records(empty_file)
        assert result == []

    def test_load_json_records_single_object(self, tmp_path: Path):
        """Test loading single JSON object."""
        single_obj = tmp_path / "single.json"
        single_obj.write_text('{"id": 1, "name": "test"}')

        result = load_json_records(single_obj)
        assert result == [{"id": 1, "name": "test"}]

    def test_dump_records_auto_ndjson(self, tmp_path: Path):
        """Test automatic format detection for NDJSON."""
        ndjson_path = tmp_path / "test.ndjson"
        records = [{"id": 1}, {"id": 2}]

        dump_records_auto(ndjson_path, records)

        lines = ndjson_path.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"id": 1}
        assert json.loads(lines[1]) == {"id": 2}

    def test_append_ndjson_record_sanitization(self, tmp_path: Path):
        """Test that appended records are sanitized."""
        ndjson_path = tmp_path / "test.jsonl"
        record = {"id": 1, "nan_val": float("nan")}

        append_ndjson_record(ndjson_path, record)

        content = ndjson_path.read_text().strip()
        parsed = json.loads(content)
        assert parsed["id"] == 1
        assert parsed["nan_val"] is None


class TestJsonReceiverEdgeCases:
    """Test edge cases specific to JSON receiver functionality."""

    def test_build_payload_type_validation(self):
        """Test strict type validation in build_payload."""
        with pytest.raises(TypeError):
            build_payload("string")
        with pytest.raises(TypeError):
            build_payload(123)
        with pytest.raises(TypeError):
            build_payload(None)

    def test_flatten_record_empty_structures(self):
        """Test flattening of empty structures."""
        empty_dict = {}
        assert flatten_record(empty_dict) == {}

        with_list = {"items": []}
        assert flatten_record(with_list) == {}

    def test_unflatten_record_empty_key_handling(self):
        """Test unflattening with empty keys."""
        flat = {"": "empty_key", "normal": "value"}
        result = unflatten_record(flat)
        expected = {"normal": "value"}
        assert result == expected

    def test_parse_path_escaped_edge_cases(self):
        """Test edge cases in path parsing."""
        assert _parse_path_escaped("") == []

        assert _parse_path_escaped("[0]") == [("", 0)]

        assert _parse_path_escaped("path.") == [("path", None)]


def test_atomic_write_textfile_success(tmp_path: Path) -> None:
    target = tmp_path / "ok.txt"

    def writer(tmp: Path) -> None:
        tmp.write_text("hello", encoding="utf-8")

    _atomic_write_textfile(target, writer)
    assert target.read_text(encoding="utf-8") == "hello"


def test_open_text_auto_plain_and_gzip(tmp_path: Path) -> None:
    p_txt = tmp_path / "x.json"
    p_txt.write_text('{"a":1}', encoding="utf-8")
    with open_text_auto(p_txt, "rt") as f_txt:
        assert f_txt.read() == '{"a":1}'

    p_gz = tmp_path / "x.json.gz"
    with gzip.open(p_gz, "wt", encoding="utf-8") as f_gz:
        f_gz.write('{"b":2}')
    with open_text_auto(p_gz, "rt") as f_gz2:
        assert f_gz2.read() == '{"b":2}'


def test_stream_json_array_to_ndjson_success_wraps_scalars(tmp_path: Path) -> None:
    src = tmp_path / "in.json"
    dst = tmp_path / "out.jsonl"
    payload = [{"id": 1}, 2, {"id": 3}, "x", None]
    src.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    count = stream_json_array_to_ndjson(src, dst)
    lines = [json.loads(s) for s in dst.read_text(encoding="utf-8").splitlines()]

    assert count == len(payload)
    assert lines == [
        {"id": 1},
        {"_value": 2},
        {"id": 3},
        {"_value": "x"},
        {"_value": None},
    ]


def test_append_ndjson_record_gzip(tmp_path: Path) -> None:
    p = tmp_path / "data.jsonl.gz"
    append_ndjson_record(p, {"a": 1})
    append_ndjson_record(p, {"b": pd.NA})
    with gzip.open(p, "rt", encoding="utf-8") as f:
        rows = [json.loads(line) for line in f if line.strip()]

    assert rows == [{"a": 1}, {"b": None}]
    assert is_ndjson_path(Path("x.ndjson"))
    assert is_ndjson_path(Path("x.ndjson.gz"))


def test_dump_records_auto_json_and_gz(tmp_path: Path) -> None:
    p_json = tmp_path / "out.json"
    records = [{"a": 1}, {"b": 2}]
    dump_records_auto(p_json, records)
    assert json.loads(p_json.read_text(encoding="utf-8")) == records

    p_gz = tmp_path / "out.json.gz"
    dump_records_auto(p_gz, records)
    with gzip.open(p_gz, "rt", encoding="utf-8") as f:
        assert json.load(f) == records

    assert not is_ndjson_path(p_json)
    assert not is_ndjson_path(p_gz)


def test_open_text_auto_write_and_read_gzip(tmp_path: Path) -> None:
    p = tmp_path / "data.ndjson.gz"
    with open_text_auto(p, "wt") as f:
        f.write('{"x": 1}\n{"y": 2}\n')
    with open_text_auto(p, "rt") as f:
        lines = [line.strip() for line in f if line.strip()]
    assert lines == ['{"x": 1}', '{"y": 2}']


def test_stream_json_array_to_ndjson_on_error_handler_continues(tmp_path: Path) -> None:
    """
    Valid array: scalars get wrapped as {"_value": ...}.
    Invalid array: parse error -> function does not raise; it returns the
    number of items written before the error (which may be 0 or >0).
    Implementations may or may not call on_error; accept both.
    The output file may be empty, unchanged, or contain the successfully
    written lines from before the error.
    """
    good = tmp_path / "good.json"
    bad = tmp_path / "bad.json"
    out = tmp_path / "out.ndjson"

    payload = [1, {"a": 2}, "x"]
    good.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    count = stream_json_array_to_ndjson(good, out)
    lines = [json.loads(s) for s in out.read_text(encoding="utf-8").splitlines()]
    assert count == 3
    assert lines == [{"_value": 1}, {"a": 2}, {"_value": "x"}]

    bad.write_text('[{"ok": 1},, {"ok": 2}]', encoding="utf-8")
    seen: list[Exception] = []

    def on_err(exc: Exception) -> None:
        seen.append(exc)

    count_bad = stream_json_array_to_ndjson(bad, out, on_error=on_err)

    assert count_bad == 2

    after_lines = [
        json.loads(s) for s in out.read_text(encoding="utf-8").splitlines() if s.strip()
    ]
    assert len(after_lines) > 0 


def test_append_ndjson_record_plain_file(tmp_path: Path) -> None:
    p = tmp_path / "rows.jsonl"
    append_ndjson_record(p, {"v": float("nan")})
    append_ndjson_record(p, {"v": None})
    content = [json.loads(s) for s in p.read_text(encoding="utf-8").splitlines()]
    assert content == [{"v": None}, {"v": None}]
