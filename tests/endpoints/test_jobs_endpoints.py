from fastapi.testclient import TestClient
from sqlmodel import Session, select
from src.persistance.db import engine
from src.persistance.table_definitions import ComponentNextLink
from src.persistance.table_definitions import (
    JobTable,
    MetaDataTable,
    LayoutTable,
    ComponentTable,
)


def test_list_jobs_empty(client: TestClient):
    response = client.get("/jobs/")
    assert response.status_code == 200
    assert response.json() == []


def test_create_and_persist_job_default(client: TestClient):
    # Create job with defaults
    create_resp = client.post("/jobs/", json={})
    assert create_resp.status_code == 200
    job_id = create_resp.json()

    # Verify directly in DB
    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record is not None
        assert record.name == "default_job_name"


def test_create_job_with_components(client: TestClient):
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
                "next": ["test2"],
            },
            {"comp_type": "test", "name": "test2", "description": "", "next": []},
        ],
    }
    create_resp = client.post("/jobs/", json=config)
    job_id = create_resp.json()
    assert create_resp.status_code == 200

    # Verify record fields in DB
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


def test_create_job_invalid_component_type(client: TestClient):
    config = {
        "components": [
            {"comp_type": "unknown", "name": "a", "description": "d", "next": []}
        ]
    }
    response = client.post("/jobs/", json=config)
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    # Expect a message about unknown component type
    assert any("Unknown component type" in err.get("msg", "") for err in detail)


def test_create_job_invalid_retries(client: TestClient):
    config = {"num_of_retries": -1}
    response = client.post("/jobs/", json=config)
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    # Expect a message about minimum value
    assert any("must be a non-negative integer" in err.get("msg", "") for err in detail)


def test_create_job_duplicate_component_names(client: TestClient):
    config = {
        "components": [
            {"comp_type": "test", "name": "dup", "description": "d", "next": []},
            {"comp_type": "test", "name": "dup", "description": "d", "next": []},
        ]
    }
    response = client.post("/jobs/", json=config)
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    # Expect a message about duplicate names
    assert any(
        "duplicate component names" in err.get("msg", "").lower() for err in detail
    )


def test_get_job_nonexistent(client: TestClient):
    response = client.get("/jobs/nonexistent")
    assert response.status_code == 404


def test_get_job_success(client: TestClient):
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


def test_update_job_success_and_persist(client: TestClient):
    job_id = client.post("/jobs/", json={}).json()
    update_config = {"name": "updated_name"}
    response = client.put(f"/jobs/{job_id}", json=update_config)
    assert response.status_code == 200
    assert response.json() == job_id

    # Verify in DB
    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record.name == "updated_name"


def test_update_job_not_found(client: TestClient):
    response = client.put("/jobs/nonexistent", json={"name": "x"})
    assert response.status_code == 404


def test_update_job_invalid_data(client: TestClient):
    job_id = client.post("/jobs/", json={}).json()
    response = client.put(f"/jobs/{job_id}", json={"name": ""})
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    assert any(
        "value must be a non-empty string" in err.get("msg", "").lower()
        for err in detail
    )


def test_delete_job_success_and_persist(client: TestClient):
    job_id = client.post("/jobs/", json={}).json()
    response = client.delete(f"/jobs/{job_id}")
    assert response.status_code == 200
    assert "message" in response.json()

    # Verify deletion in DB
    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record is None


def test_delete_job_not_found(client: TestClient):
    response = client.delete("/jobs/nonexistent")
    assert response.status_code == 404


def test_list_jobs_after_create(client: TestClient):
    # create two jobs
    _ = [client.post("/jobs/", json={}).json() for _ in range(2)]
    response = client.get("/jobs/")
    assert response.status_code == 200
    items = response.json()
    # Each item should exclude components and include metadata
    assert isinstance(items, list) and len(items) == 2
    for item in items:
        assert "components" not in item
        assert "metadata_" in item


def test_update_job_rewires_component_links(client: TestClient) -> None:
    job_id = _post_job(client, _cfg_two(a_to_b=True))

    # sanity check initial wiring: a -> b
    with Session(engine) as session:
        pairs = _fetch_link_pairs(session, job_id)
        assert pairs == {("a", "b")}

    # flip to b -> a
    resp = client.put(f"/jobs/{job_id}", json=_cfg_two(a_to_b=False))
    assert resp.status_code == 200

    with Session(engine) as session:
        pairs = _fetch_link_pairs(session, job_id)
        assert pairs == {("b", "a")}, "links should be fully replaced, not appended"


def test_delete_job_cascades_all_related_rows(client: TestClient) -> None:
    job_id = _post_job(client, _cfg_two())

    # delete
    resp = client.delete(f"/jobs/{job_id}")
    assert resp.status_code == 200

    # assert all related rows are gone
    with Session(engine) as session:
        assert session.get(JobTable, job_id) is None
        # all components removed
        comps = list(session.exec(select(ComponentTable)))
        assert comps == []
        # all layouts/metadata orphaned for components are gone
        layouts = list(session.exec(select(LayoutTable)))
        metas = list(session.exec(select(MetaDataTable)))
        assert layouts == []
        assert metas == []


def test_list_jobs_returns_metadata_but_not_components(client: TestClient) -> None:
    _ = _post_job(client, _cfg_two())
    resp = client.get("/jobs/")
    assert resp.status_code == 200
    items = resp.json()
    assert isinstance(items, list) and len(items) == 1
    assert "components" not in items[0]
    assert "metadata_" in items[0]
    assert "id" in items[0]


# helpers:
def _post_job(client: TestClient, config: dict | None = None) -> str:
    payload = config or {}
    resp = client.post("/jobs/", json=payload)
    assert resp.status_code == 200
    return resp.json()


def _cfg_two(a_to_b: bool = True) -> dict:
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


def _fetch_link_pairs(session: Session, job_id: str) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    rows = list(
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
