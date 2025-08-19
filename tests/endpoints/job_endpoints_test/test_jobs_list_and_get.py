from __future__ import annotations

from fastapi.testclient import TestClient

from sqlmodel import Session

from etl_core.persistance.db import engine
from etl_core.persistance.table_definitions import JobTable


def test_list_jobs_empty(client: TestClient) -> None:
    response = client.get("/jobs/")
    assert response.status_code == 200
    assert response.json() == []


def test_list_jobs_after_create(client: TestClient) -> None:
    _ = [client.post("/jobs/", json={}).json() for _ in range(2)]
    response = client.get("/jobs/")
    assert response.status_code == 200
    items = response.json()
    assert isinstance(items, list) and len(items) == 2
    for item in items:
        assert "components" not in item
        assert "metadata_" in item


def test_get_job_nonexistent(client: TestClient) -> None:
    response = client.get("/jobs/nonexistent")
    assert response.status_code == 404


def test_get_job_success(client: TestClient) -> None:
    job_id = client.post("/jobs/", json={}).json()
    response = client.get(f"/jobs/{job_id}")
    assert response.status_code == 200
    data = response.json()
    expected = {
        "id",
        "name",
        "num_of_retries",
        "file_logging",
        "strategy_type",
        "components",
    }
    assert expected.issubset(set(data.keys()))


def test_create_and_persist_job_default(client: TestClient) -> None:
    create_resp = client.post("/jobs/", json={})
    assert create_resp.status_code == 200
    job_id = create_resp.json()

    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record is not None
        assert record.name == "default_job_name"
