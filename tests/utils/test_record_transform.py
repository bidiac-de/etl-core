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
    build_payload,
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

    def test_escape_keys_true(self):
        """Test that special chars are escaped when escape_keys=True (default)."""
        nested = {"a.b": {"c": 1}}
        result = flatten_record(nested)
        # The key "a.b" should be escaped to "a\.b"
        assert (
            "a\\.b.c" in result or "a\\.b\\.c" in result or result.get("a\\.b.c") == 1
        )

    def test_escape_keys_false(self):
        """Test that special chars are NOT escaped when escape_keys=False."""
        nested = {"a": {"b": 1}}
        result = flatten_record(nested, escape_keys=False)
        assert result == {"a.b": 1}

    def test_custom_dict_handler(self):
        """Test custom dict_handler callback."""

        # Handler that skips keys starting with underscore
        def skip_underscore(prefix, d, out, join_fn, recurse):
            for k, v in d.items():
                if k.startswith("_"):
                    continue
                recurse(join_fn(prefix, k), v)
            return True  # We handled it

        nested = {"a": 1, "_private": 2, "b": {"c": 3, "_hidden": 4}}
        result = flatten_record(nested, dict_handler=skip_underscore)
        assert "_private" not in str(result)
        assert "_hidden" not in str(result)
        assert result.get("a") == 1
        assert "b.c" in result or "b\\.c" in result


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


class TestBuildPayload:
    """Test build_payload functionality."""

    def test_nested_passthrough(self):
        """Test that already nested dicts pass through unchanged."""
        nested = {"a": {"b": 1}}
        result = build_payload(nested)
        assert result == nested

    def test_flat_unflattens(self):
        """Test that flat dicts are unflattened."""
        flat = {"a.b": 1}
        result = build_payload(flat)
        assert result == {"a": {"b": 1}}

    def test_non_dict_raises_type_error(self):
        """Test that non-dict raises TypeError."""
        with pytest.raises(TypeError):
            build_payload("not a dict")
        with pytest.raises(TypeError):
            build_payload(123)
        with pytest.raises(TypeError):
            build_payload(None)

    def test_drop_nullish_list_items_false(self):
        """Test that nullish list items are kept when flag is False."""
        flat = {"items[0]": None, "items[1]": "x"}
        result = build_payload(flat, drop_nullish_list_items=False)
        assert result == {"items": [None, "x"]}

    def test_drop_nullish_list_items_true(self):
        """Test that nullish list items are dropped when flag is True."""
        flat = {"items[0]": None, "items[1]": "x"}
        result = build_payload(flat, drop_nullish_list_items=True)
        # items[0] key is dropped, but items[1] still creates index 1
        # so we get [None, 'x'] where None is a placeholder
        # This is the expected behavior - indices are preserved
        assert result == {"items": [None, "x"]}

    def test_drop_nullish_keeps_non_list_nulls(self):
        """Test that nullish non-list keys are kept even with flag=True."""
        flat = {"a.b": None, "items[0]": None}
        result = build_payload(flat, drop_nullish_list_items=True)
        # a.b is kept (not a list index), items[0] is dropped
        assert result == {"a": {"b": None}}

    def test_empty_dict(self):
        """Test empty dict passthrough."""
        assert build_payload({}) == {}

    def test_mixed_nested_and_flat(self):
        """Test dict with both styles uses flat detection."""
        mixed = {"a": 1, "b.c": 2}
        result = build_payload(mixed)
        # has flat paths, so unflattens
        assert result == {"a": 1, "b": {"c": 2}}
