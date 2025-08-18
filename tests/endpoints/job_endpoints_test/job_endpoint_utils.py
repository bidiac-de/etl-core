from __future__ import annotations

from typing import Dict, List, Set, Tuple

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from src.persistance.table_definitions import (
    ComponentNextLink,
    ComponentTable,
)


def post_job(client: TestClient, config: Dict | None = None) -> str:
    payload: Dict = config or {}
    resp = client.post("/jobs/", json=payload)
    assert resp.status_code == 200
    return resp.json()


def cfg_two(a_to_b: bool = True) -> Dict:
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
                "next": ["b"] if a_to_b else [],
            },
            {
                "comp_type": "test",
                "name": "b",
                "description": "",
                "next": [] if a_to_b else ["a"],
            },
        ],
    }


def fetch_link_pairs(session: Session, job_id: str) -> Set[Tuple[str, str]]:
    pairs: Set[Tuple[str, str]] = set()
    rows: List[ComponentTable] = list(
        session.exec(select(ComponentTable).where(ComponentTable.job_id == job_id))
    )
    id_by_name = {r.name: r.id for r in rows}
    link_rows = list(session.exec(select(ComponentNextLink)))
    for lr in link_rows:
        # keep only links that belong to this job
        if lr.component_id in id_by_name.values() and lr.next_id in id_by_name.values():
            src = next(n for n, _id in id_by_name.items() if _id == lr.component_id)
            dst = next(n for n, _id in id_by_name.items() if _id == lr.next_id)
            pairs.add((src, dst))
    return pairs
