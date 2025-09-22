from __future__ import annotations

from typing import Dict, Set, Tuple

from fastapi.testclient import TestClient
from sqlmodel import Session, select
from sqlalchemy.orm import aliased

from etl_core.persistence.table_definitions import ComponentLinkTable, ComponentTable


def post_job(client: TestClient, config: Dict | None = None) -> str:
    payload: Dict = config or {}
    resp = client.post("/jobs/", json=payload)
    assert resp.status_code == 200
    return resp.json()


def cfg_two(a_to_b: bool = True) -> Dict:
    """
    Minimal 2-node graph config.
    Stub component 'test' has ports: in='in', out='out'.
    Validation requires schemas for routed ports
    """
    min_schema = {"fields": [{"name": "id", "data_type": "integer", "nullable": False}]}

    a: Dict = {
        "comp_type": "test",
        "name": "a",
        "description": "",
        "out_port_schemas": {"out": min_schema},
        "in_port_schemas": {"in": min_schema},
        "routes": {"out": ["b"]} if a_to_b else {"out": []},
    }
    b: Dict = {
        "comp_type": "test",
        "name": "b",
        "description": "",
        "out_port_schemas": {"out": min_schema},
        "in_port_schemas": {"in": min_schema},
        "routes": {"out": []} if a_to_b else {"out": ["a"]},
    }

    return {
        "name": "two_nodes",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "metadata_": {"user_id": 1, "timestamp": "2023-10-01T00:00:00"},
        "components": [a, b],
    }


def fetch_link_pairs(session: Session, job_id: str) -> Set[Tuple[str, str]]:
    src = aliased(ComponentTable)
    dst = aliased(ComponentTable)

    stmt = (
        select(src.name, dst.name)
        .select_from(ComponentLinkTable)
        .join(ComponentLinkTable.src_component.of_type(src))
        .join(ComponentLinkTable.dst_component.of_type(dst))
        .where(ComponentLinkTable.job_id == job_id)
        .order_by(src.name, dst.name)
    )
    return {(s, d) for s, d in session.exec(stmt).all()}
