from __future__ import annotations

from fastapi.testclient import TestClient


def test_validation_errors_use_canonical_envelope(client: TestClient) -> None:
    response = client.post("/setup/validate", json={"key": ""})
    assert response.status_code == 422
    payload = response.json()
    assert "error" in payload
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert isinstance(payload["error"]["details"], list)


def test_business_errors_use_canonical_envelope(client: TestClient) -> None:
    response = client.get("/configs/unknown/form")
    assert response.status_code == 404
    payload = response.json()
    assert "error" in payload
    assert payload["error"]["code"] == "SCHEMA_COMPONENT_UNKNOWN"


def test_framework_404_uses_canonical_envelope(client: TestClient) -> None:
    response = client.get("/__this_path_should_not_exist__")
    assert response.status_code == 404
    payload = response.json()
    assert "error" in payload
    assert payload["error"]["code"] == "HTTP_404"
