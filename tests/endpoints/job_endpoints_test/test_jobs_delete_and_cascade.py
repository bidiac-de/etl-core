from __future__ import annotations

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from etl_core.persistance.db import engine
from etl_core.persistance.table_definitions import (
    ComponentTable,
    JobTable,
    LayoutTable,
    MetaDataTable,
)


def test_delete_job_success_and_persist(client: TestClient) -> None:
    job_id = client.post("/jobs/", json={}).json()
    response = client.delete(f"/jobs/{job_id}")
    assert response.status_code == 200
    assert "message" in response.json()

    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record is None


def test_delete_job_not_found(client: TestClient) -> None:
    response = client.delete("/jobs/nonexistent")
    assert response.status_code == 404


def test_delete_job_cascades_all_related_rows(client: TestClient) -> None:
    job_id = client.post(
        "/jobs/",
        json={
            "name": "two_nodes",
            "components": [
                {
                    "comp_type": "test",
                    "name": "a",
                    "description": "",
                    "routes": {"out": ["b"]},
                },
                {"comp_type": "test", "name": "b", "description": "", "routes": {}},
            ],
        },
    ).json()

    resp = client.delete(f"/jobs/{job_id}")
    assert resp.status_code == 200

    with Session(engine) as session:
        assert session.get(JobTable, job_id) is None
        comps = list(session.exec(select(ComponentTable)))
        assert comps == []
        layouts = list(session.exec(select(LayoutTable)))
        metas = list(session.exec(select(MetaDataTable)))
        assert layouts == []
        assert metas == []
