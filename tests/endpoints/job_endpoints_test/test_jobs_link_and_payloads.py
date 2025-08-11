from __future__ import annotations

from typing import Dict, List

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from src.persistance.configs.job_config import JobConfig
from src.persistance.db import engine
from src.persistance.table_definitions import ComponentTable
from tests.endpoints.job_endpoints_test.job_endpoint_utils import (
    cfg_two,
    fetch_link_pairs,
    post_job,
)


def test_update_job_rewires_component_links(client: TestClient) -> None:
    job_id = post_job(client, cfg_two(a_to_b=True))

    with Session(engine) as session:
        pairs = fetch_link_pairs(session, job_id)
        assert pairs == {("a", "b")}

    resp = client.put(f"/jobs/{job_id}", json=cfg_two(a_to_b=False))
    assert resp.status_code == 200

    with Session(engine) as session:
        pairs = fetch_link_pairs(session, job_id)
        assert pairs == {("b", "a")}, "links should be replaced, not appended"


def test_component_payload_persisted_in_db(client: TestClient) -> None:
    cfg: Dict = {
        "name": "payload_job",
        "components": [
            {
                "comp_type": "multi_source",
                "name": "src",
                "description": "",
                "next": [],
                "count": 7,
            }
        ],
    }
    resp = client.post("/jobs/", json=cfg)
    assert resp.status_code == 200
    job_id = resp.json()

    with Session(engine) as session:
        rows: List[ComponentTable] = list(
            session.exec(select(ComponentTable).where(ComponentTable.job_id == job_id))
        )
        assert len(rows) == 1
        ct = rows[0]
        assert isinstance(ct.payload, dict)
        assert ct.payload == {"count": 7}


def test_component_payload_hydrates_to_runtime(shared_job_handler) -> None:
    cfg = JobConfig(
        name="payload_roundtrip",
        components=[
            {
                "comp_type": "multi_source",
                "name": "src",
                "description": "",
                "next": [],
                "count": 3,
            }
        ],
    )
    row = shared_job_handler.create_job_entry(cfg)
    job_id = row.id

    record = shared_job_handler.get_by_id(job_id)
    assert record is not None
    runtime = shared_job_handler.record_to_job(record)

    assert len(runtime.components) == 1
    comp = runtime.components[0]
    assert comp.comp_type == "multi_source"
    assert getattr(comp, "count", None) == 3
