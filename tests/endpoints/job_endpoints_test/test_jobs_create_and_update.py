from __future__ import annotations

from fastapi.testclient import TestClient
from sqlmodel import Session

from etl_core.persistance.db import engine
from etl_core.persistance.table_definitions import (
    ComponentTable,
    JobTable,
    MetaDataTable,
)


def test_create_job_with_components(client: TestClient, schema_row_min) -> None:
    config = {
        "name": "test_job",
        "num_of_retries": 2,
        "file_logging": True,
        "strategy_type": "row",
        "metadata_": {
            "user_id": 42,
            "timestamp": "2023-10-01T12:00:00",
        },
        "components": [
            {
                "comp_type": "test",
                "name": "test1",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
            },
            {
                "comp_type": "test",
                "name": "test2",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
            },
        ],
    }
    create_resp = client.post("/jobs/", json=config)
    job_id = create_resp.json()
    assert create_resp.status_code == 200

    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record.name == "test_job"
        assert record.num_of_retries == 2
        assert record.file_logging is True
        assert record.strategy_type == "row"
        meta = session.get(MetaDataTable, record.metadata_.id)
        assert meta.user_id == 42
        comp1 = session.get(ComponentTable, record.components[0].id)
        comp2 = session.get(ComponentTable, record.components[1].id)
        assert comp1.name == "test1"
        assert comp2.name == "test2"


def test_update_job_success_and_persist(client: TestClient) -> None:
    job_id = client.post("/jobs/", json={}).json()
    update_config = {"name": "updated_name"}
    response = client.put(f"/jobs/{job_id}", json=update_config)
    assert response.status_code == 200
    assert response.json() == job_id

    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record.name == "updated_name"


def test_update_job_not_found(client: TestClient) -> None:
    response = client.put("/jobs/nonexistent", json={"name": "x"})
    assert response.status_code == 404


def test_update_job_invalid_data(client: TestClient) -> None:
    job_id = client.post("/jobs/", json={}).json()
    response = client.put(f"/jobs/{job_id}", json={"name": ""})
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    assert any(
        "value must be a non-empty string" in err.get("msg", "").lower()
        for err in detail
    )
