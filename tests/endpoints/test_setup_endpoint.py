from __future__ import annotations

from fastapi.testclient import TestClient

from etl_core.context.environment import Environment


def test_setup_capabilities_shape(client: TestClient) -> None:
    response = client.get("/setup/capabilities")
    assert response.status_code == 200
    payload = response.json()

    assert payload["contract_version"] == "core-studio-v1"
    assert isinstance(payload["core_version"], str)
    assert isinstance(payload["setup_validation"], dict)
    assert isinstance(payload["environments"], list)
    assert isinstance(payload["rule_operators"], list)
    assert isinstance(payload["rule_logical_operators"], list)
    assert isinstance(payload["data_types"], list)

    env_values = {item["value"] for item in payload["environments"]}
    assert env_values == {env.value for env in Environment}


def test_setup_validate_open_mode_without_configured_key(
    client: TestClient, monkeypatch
) -> None:
    monkeypatch.delenv("ETL_SETUP_ACCESS_KEY", raising=False)
    response = client.post("/setup/validate", json={"key": "any"})
    assert response.status_code == 200
    assert response.json() == {"valid": True}


def test_setup_validate_shared_key_mode(client: TestClient, monkeypatch) -> None:
    monkeypatch.setenv("ETL_SETUP_ACCESS_KEY", "VALID_KEY")

    ok_response = client.post("/setup/validate", json={"key": "VALID_KEY"})
    assert ok_response.status_code == 200
    assert ok_response.json() == {"valid": True}

    bad_response = client.post("/setup/validate", json={"key": "INVALID"})
    assert bad_response.status_code == 200
    assert bad_response.json() == {"valid": False}


def test_setup_validate_requires_key(client: TestClient) -> None:
    response = client.post("/setup/validate", json={"key": ""})
    assert response.status_code == 422
    payload = response.json()
    assert "error" in payload
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert isinstance(payload["error"]["details"], list)
