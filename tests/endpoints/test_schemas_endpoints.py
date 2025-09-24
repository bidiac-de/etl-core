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

    # 1) properties exist and are now an ordered LIST of entries
    assert "properties" in schema
    assert isinstance(schema["properties"], list)

    # 2) check a few expected fields by name
    prop_names = _prop_names_list(schema)
    for expected in {"name", "file_logging", "num_of_retries", "strategy_type"}:
        assert expected in prop_names

    # 3) Job schema keeps $defs (router does not inline at this endpoint)
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
    # class variables should be attached for GUI
    assert "x-class" in payload
    assert isinstance(payload["x-class"], dict)


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
    # Hidden-only schema should be an object with (possibly empty) properties
    assert (
        hidden_schema.get("type") in (None, "object") or "properties" in hidden_schema
    )


def test_get_specific_schema_invalid_form(client: TestClient) -> None:
    response = client.get("/configs/unknown/form")
    assert response.status_code == 404
    assert "detail" in response.json()
