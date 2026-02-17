from copy import deepcopy

from etl_core.api import helpers as H


def test_sanitize_errors_filters_only_expected_keys():
    errors = [
        {"type": "val", "loc": ["a"], "msg": "bad", "url": "u", "extra": 1},
        {"msg": "oops"},
    ]
    out = H._sanitize_errors(errors)
    assert out == [
        {"type": "val", "loc": ["a"], "msg": "bad", "url": "u"},
        {"msg": "oops"},
    ]


def test_error_payload_with_and_without_context():
    p1 = H._error_payload("CODE", "Message")
    assert p1 == {"code": "CODE", "message": "Message"}

    p2 = H._error_payload("X", "Y", a=1, b="t")
    assert (
        p2["code"] == "X"
        and p2["message"] == "Y"
        and p2["context"] == {"a": 1, "b": "t"}
    )


def test_exc_meta_contains_safe_fields():
    try:
        raise ValueError("boom")
    except Exception as exc:  # noqa: BLE001
        meta = H._exc_meta(exc)
        assert meta["type"] == "ValueError"
        assert "cause" in meta and "context" in meta


def test_inline_defs_no_defs_returns_copy():
    obj = {"type": "object", "properties": {}}
    out = H.inline_defs(obj)
    assert out is not obj and out == obj


def test_inline_defs_inlines_and_preserves_cycles():
    schema = {
        "$defs": {
            "Foo": {"type": "object", "properties": {"a": {"type": "string"}}},
            "Bar": {"$ref": "#/$defs/Foo"},
        },
        "type": "object",
        "properties": {
            "f": {"$ref": "#/$defs/Foo"},
            "b": {"$ref": "#/$defs/Bar"},
            "c": {"$ref": "#/$defs/Cycle"},
        },
    }
    schema["$defs"]["Cycle"] = {"allOf": [{"$ref": "#/$defs/Cycle"}]}

    out = H.inline_defs(schema)
    assert "$defs" in out
    assert out["properties"]["f"]["properties"]["a"]["type"] == "string"
    assert out["properties"]["b"]["properties"]["a"]["type"] == "string"
    assert out["properties"]["c"]["allOf"][0]["$ref"] == "#/$defs/Cycle"


def test_schema_post_processing_transforms_and_strip_order():
    schema = {
        "type": "object",
        "properties": {
            "x": {"type": "string", "order": 2},
            "y": {"type": "string", "enum": ["a", "b"], "order": 1},
        },
        "required": ["x"],
        "allOf": [{"$ref": "#/$defs/Foo"}],
        "$defs": {"Foo": {"type": "object"}},
    }

    out = H.schema_post_processing(schema, strip_order=True)

    props = out["properties"]
    assert (
        isinstance(props, list) and props[0]["name"] == "y" and props[1]["name"] == "x"
    )
    assert props[0]["schema"]["type"] == "select"
    assert "order" not in props[0]["schema"] and "order" not in props[1]["schema"]
    assert out.get("$ref") == "#/$defs/Foo"


def test_schema_post_processing_keep_order_when_requested():
    schema = {
        "type": "object",
        "properties": {"a": {"type": "string", "order": 5}},
        "$defs": {"Foo": {"type": "object"}},
    }
    keep = H.schema_post_processing(deepcopy(schema), strip_order=False)
    props = keep["properties"]
    assert props[0]["schema"]["order"] == 5


def test_schema_post_processing_collapses_simple_nullable_anyof():
    schema = {
        "type": "object",
        "properties": {
            "sheet_name": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "title": "Sheet Name",
                "description": "Optional sheet name",
                "default": None,
            }
        },
    }

    out = H.schema_post_processing(schema, strip_order=True)
    prop = out["properties"][0]["schema"]
    assert prop["type"] == "string"
    assert prop["nullable"] is True
    assert prop["default"] is None
    assert prop["title"] == "Sheet Name"
    assert prop["description"] == "Optional sheet name"
    assert "anyOf" not in prop


def test_schema_post_processing_collapses_simple_nullable_oneof_and_keeps_siblings():
    schema = {
        "type": "object",
        "properties": {
            "where_conditions": {
                "oneOf": [
                    {"type": "array", "items": {"type": "string"}, "title": "inner"},
                    {"type": "null"},
                ],
                "title": "Where Conditions",
                "description": "Optional predicates",
            }
        },
    }

    out = H.schema_post_processing(schema, strip_order=True)
    prop = out["properties"][0]["schema"]
    assert prop["type"] == "array"
    assert prop["nullable"] is True
    assert prop["title"] == "Where Conditions"
    assert prop["description"] == "Optional predicates"
    assert "oneOf" not in prop


def test_schema_post_processing_keeps_complex_union_unchanged():
    schema = {
        "type": "object",
        "properties": {
            "value": {
                "anyOf": [{"type": "string"}, {"type": "integer"}, {"type": "null"}],
                "title": "Value",
            }
        },
    }

    out = H.schema_post_processing(schema, strip_order=True)
    prop = out["properties"][0]["schema"]
    assert "anyOf" in prop
    assert "type" not in prop
