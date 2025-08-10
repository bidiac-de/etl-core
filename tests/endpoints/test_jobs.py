import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, delete
from src.api.main import app
from src.persistance.db import engine
from src.persistance.table_definitions import (
    JobTable,
    MetaDataTable,
    LayoutTable,
    ComponentTable,
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def clear_db():
    """
    Clear the Tables before each test to ensure isolation.
    """
    with Session(engine) as session:
        session.exec(delete(JobTable))
        session.exec(delete(MetaDataTable))
        session.exec(delete(ComponentTable))
        session.exec(delete(LayoutTable))
        session.commit()
    yield
    # cleanup (redundant but safe)
    with Session(engine) as session:
        session.exec(delete(JobTable))
        session.exec(delete(MetaDataTable))
        session.exec(delete(ComponentTable))
        session.exec(delete(LayoutTable))
        session.commit()


def test_list_jobs_empty():
    response = client.get("/jobs/")
    assert response.status_code == 200
    assert response.json() == []


def test_create_and_persist_job_default():
    # Create job with defaults
    create_resp = client.post("/jobs/", json={})
    assert create_resp.status_code == 200
    job_id = create_resp.json()

    # Verify directly in DB
    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record is not None
        assert record.name == "default_job_name"


def test_create_job_with_components():
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


def test_create_job_invalid_component_type():
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


def test_create_job_invalid_retries():
    config = {"num_of_retries": -1}
    response = client.post("/jobs/", json=config)
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    # Expect a message about minimum value
    assert any("must be a non-negative integer" in err.get("msg", "") for err in detail)


def test_create_job_duplicate_component_names():
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


def test_get_job_nonexistent():
    response = client.get("/jobs/nonexistent")
    assert response.status_code == 404


def test_get_job_success():
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


def test_update_job_success_and_persist():
    job_id = client.post("/jobs/", json={}).json()
    update_config = {"name": "updated_name"}
    response = client.put(f"/jobs/{job_id}", json=update_config)
    assert response.status_code == 200
    assert response.json() == job_id

    # Verify in DB
    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record.name == "updated_name"


def test_update_job_not_found():
    response = client.put("/jobs/nonexistent", json={"name": "x"})
    assert response.status_code == 404


def test_update_job_invalid_data():
    job_id = client.post("/jobs/", json={}).json()
    response = client.put(f"/jobs/{job_id}", json={"name": ""})
    assert response.status_code == 422
    detail = response.json().get("detail")
    assert isinstance(detail, list)
    assert any(
        "value must be a non-empty string" in err.get("msg", "").lower()
        for err in detail
    )


def test_delete_job_success_and_persist():
    job_id = client.post("/jobs/", json={}).json()
    response = client.delete(f"/jobs/{job_id}")
    assert response.status_code == 200
    assert "message" in response.json()

    # Verify deletion in DB
    with Session(engine) as session:
        record = session.get(JobTable, job_id)
        assert record is None


def test_delete_job_not_found():
    response = client.delete("/jobs/nonexistent")
    assert response.status_code == 404


def test_list_jobs_after_create():
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
