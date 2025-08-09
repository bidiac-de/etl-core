from src.api.main import app
from fastapi.testclient import TestClient


client = TestClient(app)


def test_get_job_schema_structure():
    response = client.get("/schemas/job")
    assert response.status_code == 200
    schema = response.json()
    assert "properties" in schema
    assert "name" in schema["properties"]
    assert "file_logging" in schema["properties"]
    assert "num_of_retries" in schema["properties"]
    assert "strategy_type" in schema["properties"]
    assert "$defs" not in schema


def test_schema_component_types():
    response = client.get("/schemas/component_types")
    assert response.status_code == 200
    types = response.json()
    assert isinstance(types, list)
    assert all(isinstance(t, str) for t in types)


def test_get_specific_schema_valid():
    comp_types = client.get("/schemas/component_types").json()
    valid = comp_types[0]
    response = client.get(f"/schemas/{valid}")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)


def test_get_specific_schema_invalid():
    response = client.get("/schemas/unknown")
    assert response.status_code == 404
    assert "detail" in response.json()
