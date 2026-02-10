"""
Tests for the centralized record_transform module.

These tests ensure the unified unflatten_record and flatten_record work correctly.
"""
import pytest
from etl_core.utils.record_transform import (
    unflatten_record,
    flatten_record,
    has_flat_paths,
    _escape_key,
    _unescape_key,
    _parse_path_escaped,
)


class TestUnflattenRecord:
    """Test unflatten_record functionality."""

    def test_simple_dotted_keys(self):
        """Test basic dot-separated keys."""
        flat = {"a.b": 1, "a.c": 2}
        result = unflatten_record(flat)
        assert result == {"a": {"b": 1, "c": 2}}

    def test_deeply_nested(self):
        """Test deeply nested structure."""
        flat = {"a.b.c.d": 1}
        result = unflatten_record(flat)
        assert result == {"a": {"b": {"c": {"d": 1}}}}

    def test_array_indices(self):
        """Test array index handling."""
        flat = {"items[0]": "x", "items[1]": "y"}
        result = unflatten_record(flat)
        assert result == {"items": ["x", "y"]}

    def test_nested_arrays(self):
        """Test nested array access."""
        flat = {"data[0].name": "Alice", "data[1].name": "Bob"}
        result = unflatten_record(flat)
        assert result == {"data": [{"name": "Alice"}, {"name": "Bob"}]}

    def test_mixed_keys(self):
        """Test mix of dotted and non-dotted keys."""
        flat = {"simple": 1, "nested.key": 2}
        result = unflatten_record(flat)
        assert result == {"simple": 1, "nested": {"key": 2}}

    def test_empty_dict(self):
        """Test empty input."""
        assert unflatten_record({}) == {}

    def test_empty_key_skipped(self):
        """Test that empty keys are skipped."""
        flat = {"": "ignored", "a": 1}
        result = unflatten_record(flat)
        assert result == {"a": 1}

    def test_escaped_dot_in_key(self):
        """Test escaped dots in keys."""
        flat = {"a\\.b": 1}
        result = unflatten_record(flat)
        assert result == {"a.b": 1}

    def test_escaped_bracket_in_key(self):
        """Test escaped brackets in keys."""
        flat = {"a\\[b\\]": 1}
        result = unflatten_record(flat)
        assert result == {"a[b]": 1}


class TestFlattenRecord:
    """Test flatten_record functionality."""

    def test_simple_nested(self):
        """Test basic nested dict flattening."""
        nested = {"a": {"b": 1, "c": 2}}
        result = flatten_record(nested)
        assert result == {"a.b": 1, "a.c": 2}

    def test_list_flattening(self):
        """Test list index flattening."""
        nested = {"items": ["x", "y"]}
        result = flatten_record(nested)
        assert result == {"items[0]": "x", "items[1]": "y"}

    def test_nested_lists(self):
        """Test nested list in dict flattening."""
        nested = {"data": [{"name": "Alice"}, {"name": "Bob"}]}
        result = flatten_record(nested)
        assert result == {"data[0].name": "Alice", "data[1].name": "Bob"}

    def test_empty_dict(self):
        """Test empty input."""
        assert flatten_record({}) == {}

    def test_scalar_value(self):
        """Test dict with scalar values only."""
        nested = {"a": 1, "b": 2}
        result = flatten_record(nested)
        assert result == {"a": 1, "b": 2}


class TestRoundtrip:
    """Test that flatten and unflatten are inverses."""

    def test_simple_roundtrip(self):
        """Test simple nested dict roundtrip."""
        original = {"a": {"b": 1, "c": 2}}
        flat = flatten_record(original)
        restored = unflatten_record(flat)
        assert restored == original

    def test_list_roundtrip(self):
        """Test list roundtrip."""
        original = {"items": ["x", "y", "z"]}
        flat = flatten_record(original)
        restored = unflatten_record(flat)
        assert restored == original

    def test_complex_roundtrip(self):
        """Test complex nested structure roundtrip."""
        original = {
            "users": [
                {"name": "Alice", "address": {"city": "NYC"}},
                {"name": "Bob", "address": {"city": "LA"}},
            ]
        }
        flat = flatten_record(original)
        restored = unflatten_record(flat)
        assert restored == original


class TestHasFlatPaths:
    """Test has_flat_paths detection."""

    def test_dotted_key(self):
        """Test detection of dotted keys."""
        assert has_flat_paths({"a.b": 1}) is True

    def test_bracket_key(self):
        """Test detection of bracket keys."""
        assert has_flat_paths({"a[0]": 1}) is True

    def test_simple_keys(self):
        """Test no flat paths in simple keys."""
        assert has_flat_paths({"a": 1, "b": 2}) is False

    def test_empty_dict(self):
        """Test empty dict has no flat paths."""
        assert has_flat_paths({}) is False


class TestEscaping:
    """Test key escaping functions."""

    def test_escape_dot(self):
        """Test dot escaping."""
        assert _escape_key("a.b") == "a\\.b"

    def test_escape_bracket(self):
        """Test bracket escaping."""
        assert _escape_key("a[0]") == "a\\[0\\]"

    def test_unescape_dot(self):
        """Test dot unescaping."""
        assert _unescape_key("a\\.b") == "a.b"

    def test_unescape_bracket(self):
        """Test bracket unescaping."""
        assert _unescape_key("a\\[0\\]") == "a[0]"

    def test_escape_roundtrip(self):
        """Test escape/unescape roundtrip."""
        original = "key.with[special]chars"
        escaped = _escape_key(original)
        unescaped = _unescape_key(escaped)
        assert unescaped == original


class TestParsePath:
    """Test path parsing."""

    def test_simple_path(self):
        """Test simple dotted path."""
        result = _parse_path_escaped("a.b.c")
        assert result == [("a", None), ("b", None), ("c", None)]

    def test_indexed_path(self):
        """Test path with array index."""
        result = _parse_path_escaped("items[0]")
        assert result == [("items", 0)]

    def test_mixed_path(self):
        """Test path with dots and indices."""
        result = _parse_path_escaped("data[0].name")
        assert result == [("data", 0), ("name", None)]

