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
    Minimal 2-node graph config using routes only.
    Our 'test' stub has ports: in='in', out='out'.
    """
    return {
        "name": "two_nodes",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "metadata_": {"user_id": 1, "timestamp": "2023-10-01T00:00:00"},
        "components": [
            {
                "comp_type": "test",
                "name": "a",
                "description": "",
                "routes": {"out": ["b"]} if a_to_b else {"out": []},
            },
            {
                "comp_type": "test",
                "name": "b",
                "description": "",
                "routes": {"out": []} if a_to_b else {"out": ["a"]},
            },
        ],
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
