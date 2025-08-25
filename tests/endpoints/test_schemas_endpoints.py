from __future__ import annotations
from fastapi.testclient import TestClient


def test_get_job_schema_structure(client: TestClient) -> None:
    response = client.get("/configs/job")
    assert response.status_code == 200
    schema = response.json()
    assert "properties" in schema
    assert "name" in schema["properties"]
    assert "file_logging" in schema["properties"]
    assert "num_of_retries" in schema["properties"]
    assert "strategy_type" in schema["properties"]
    assert "$defs" not in schema


def test_schema_component_types(client: TestClient) -> None:
    response = client.get("/configs/component_types")
    assert response.status_code == 200
    types = response.json()
    assert isinstance(types, list)
    assert all(isinstance(t, str) for t in types)


def test_get_specific_schema_valid(client: TestClient) -> None:
    comp_types = client.get("/configs/component_types").json()
    valid = comp_types[0]
    response = client.get(f"/configs/{valid}")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)


def test_get_specific_schema_invalid(client: TestClient) -> None:
    response = client.get("/configs/unknown")
    assert response.status_code == 404
    assert "detail" in response.json()
