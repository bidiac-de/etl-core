from __future__ import annotations

from fastapi.testclient import TestClient


def test_create_job_invalid_component_type(client: TestClient) -> None:
    config = {"components": [{"comp_type": "unknown", "name": "a", "description": "d"}]}
    response = client.post("/jobs/", json=config)
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    assert any("Unknown component type" in err.get("msg", "") for err in detail)


def test_create_job_invalid_retries(client: TestClient) -> None:
    config = {"num_of_retries": -1}
    response = client.post("/jobs/", json=config)
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    assert any("must be a non-negative integer" in err.get("msg", "") for err in detail)


def test_create_job_duplicate_component_names(client: TestClient) -> None:
    config = {
        "components": [
            {"comp_type": "test", "name": "dup", "description": "d"},
            {"comp_type": "test", "name": "dup", "description": "d"},
        ]
    }
    response = client.post("/jobs/", json=config)
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    assert any(
        "duplicate component names" in err.get("msg", "").lower() for err in detail
    )
