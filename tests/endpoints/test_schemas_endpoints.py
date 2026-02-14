from __future__ import annotations

from fastapi.testclient import TestClient


def _prop_names_list(schema: dict) -> set[str]:
    """
    The job schema's 'properties' is now a list of
    {'name': str, 'schema': dict, 'required': bool}.
    """
    props = schema.get("properties", [])
    if not isinstance(props, list):
        return set()
    names = []
    for entry in props:
        if isinstance(entry, dict):
            name = entry.get("name")
            if isinstance(name, str):
                names.append(name)
    return set(names)


def test_get_job_schema_structure(client: TestClient) -> None:
    response = client.get("/configs/job")
    assert response.status_code == 200
    schema = response.json()

    assert "properties" in schema
    assert isinstance(schema["properties"], list)

    prop_names = _prop_names_list(schema)
    for expected in {"name", "file_logging", "num_of_retries", "strategy_type"}:
        assert expected in prop_names

    assert "$defs" in schema


def test_schema_component_types(client: TestClient) -> None:
    response = client.get("/configs/component_types")
    assert response.status_code == 200
    types = response.json()
    assert isinstance(types, list)
    assert all(isinstance(t, str) for t in types)


def test_get_specific_schema_valid_form(client: TestClient) -> None:
    comp_types = client.get("/configs/component_types").json()
    if not comp_types:
        return
    comp = comp_types[0]
    response = client.get(f"/configs/{comp}/form")
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, dict)
    assert "x-class" in payload
    assert isinstance(payload["x-class"], dict)
    assert "x-ui" in payload
    assert isinstance(payload["x-ui"], dict)
    assert "context_selector" in payload["x-ui"]
    assert "rule_builder" in payload["x-ui"]
    assert "port_schema_editor" in payload["x-ui"]


def test_get_specific_schema_full_and_hidden(client: TestClient) -> None:
    comp_types = client.get("/configs/component_types").json()
    if not comp_types:
        return
    comp = comp_types[0]

    r_full = client.get(f"/configs/{comp}/full")
    assert r_full.status_code == 200
    full_schema = r_full.json()
    assert isinstance(full_schema, dict)
    assert "x-class" in full_schema

    r_hidden = client.get(f"/configs/{comp}/hidden")
    assert r_hidden.status_code == 200
    hidden_schema = r_hidden.json()
    assert isinstance(hidden_schema, dict)
    assert "x-class" in hidden_schema
    assert (
        hidden_schema.get("type") in (None, "object") or "properties" in hidden_schema
    )


def test_get_specific_schema_invalid_form(client: TestClient) -> None:
    response = client.get("/configs/unknown/form")
    assert response.status_code == 404
    payload = response.json()
    assert "error" in payload
    assert isinstance(payload["error"], dict)
    assert payload["error"]["code"] == "SCHEMA_COMPONENT_UNKNOWN"


def test_merge_split_form_does_not_expose_class_port_specs(client: TestClient) -> None:
    merge_form = client.get("/configs/merge/form")
    if merge_form.status_code == 200:
        merge_props = {
            entry["name"]
            for entry in merge_form.json().get("properties", [])
            if isinstance(entry, dict) and isinstance(entry.get("name"), str)
        }
        assert "INPUT_PORTS" not in merge_props
        assert "OUTPUT_PORTS" not in merge_props

    split_form = client.get("/configs/split/form")
    if split_form.status_code == 200:
        split_props = {
            entry["name"]
            for entry in split_form.json().get("properties", [])
            if isinstance(entry, dict) and isinstance(entry.get("name"), str)
        }
        assert "INPUT_PORTS" not in split_props
        assert "OUTPUT_PORTS" not in split_props
