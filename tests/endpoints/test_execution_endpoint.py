from __future__ import annotations

from typing import Dict

from fastapi.testclient import TestClient

from etl_core.persistance.configs.job_config import JobConfig
from tests.helpers import detail_message


def _create_job(shared_job_handler, cfg: JobConfig) -> str:
    row = shared_job_handler.create_job_entry(cfg)
    return row.id


def test_start_execution_minimal_ok(client: TestClient, shared_job_handler) -> None:
    # Real handler, minimal job (no components)
    job_id = _create_job(shared_job_handler, JobConfig())

    resp = client.post(f"/execution/{job_id}")
    assert resp.status_code == 200

    data: Dict = resp.json()
    assert data.get("status") == "started"
    assert data.get("job_id") == job_id
    assert "execution_id" in data
    assert "max_attempts" in data


def test_start_execution_simple_chain_ok(
    client: TestClient, shared_job_handler
) -> None:
    # Real handler, real components (assumes 'test' component is registered)
    cfg = JobConfig(
        components=[
            {"comp_type": "test", "name": "a", "description": "", "next": ["b"]},
            {"comp_type": "test", "name": "b", "description": "", "next": []},
        ]
    )
    job_id = _create_job(shared_job_handler, cfg)

    resp = client.post(f"/execution/{job_id}")
    assert resp.status_code == 200

    data: Dict = resp.json()
    assert data.get("status") == "started"
    assert data.get("job_id") == job_id
    assert "execution_id" in data
    assert "max_attempts" in data


def test_start_execution_not_found(client: TestClient) -> None:
    missing_id = "6d1f6c89-0b2a-4f6b-90e2-6b2b9f2f0f00"
    resp = client.post(f"/execution/{missing_id}")
    assert resp.status_code == 404

    body = resp.json()
    assert "not found" in detail_message(body).lower()

    # If structured, assert the machine-readable code
    if isinstance(body.get("detail"), dict):
        assert body["detail"].get("code") in {"JOB_NOT_FOUND"}
