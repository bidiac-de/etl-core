from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List
import uuid

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from etl_core.api.routers import jobs as R
from etl_core.api import http_errors as HE
from etl_core.api.dependencies import get_job_handler
from etl_core.persistence.errors import PersistLinkageError, PersistNotFoundError


@pytest.fixture(autouse=True)
def _clear_jobs_cache_before_each_test() -> None:
    R.invalidate_job_caches()


class DummyJob:
    def __init__(self, job_id: str, data: Dict[str, Any]) -> None:
        self.id = job_id
        self._data = data

    def model_dump(self) -> Dict[str, Any]:
        return dict(self._data)


class DummyRow:
    def __init__(self, row_id: str) -> None:
        self.id = row_id


class DummyJobHandler:
    def __init__(self) -> None:
        self.calls = SimpleNamespace(list=0, load=0, create=0, update=0, delete=0)
        self._rows: List[Dict[str, Any]] = []
        self._job: DummyJob | None = None
        self._errors: Dict[str, BaseException] = {}

    def list_jobs_brief(self) -> List[Dict[str, Any]]:
        self.calls.list += 1
        err = self._errors.get("list")
        if err:
            raise err
        return list(self._rows)

    def load_runtime_job(self, job_id: str) -> DummyJob:
        self.calls.load += 1
        err = self._errors.get("load")
        if err:
            raise err
        if self._job is None:
            raise PersistNotFoundError(f"no job {job_id}")
        return self._job

    def create_job_entry(self, job_cfg: Any) -> DummyRow:
        self.calls.create += 1
        err = self._errors.get("create")
        if err:
            raise err
        return DummyRow("created-1")

    def update(self, job_id: str, job_cfg: Any) -> DummyRow:
        self.calls.update += 1
        err = self._errors.get("update")
        if err:
            raise err
        return DummyRow(job_id)

    def delete(self, job_id: str) -> None:
        self.calls.delete += 1
        err = self._errors.get("delete")
        if err:
            raise err
        return None


def _build_app(handler: DummyJobHandler) -> TestClient:
    app = FastAPI()
    app.include_router(R.router)
    app.dependency_overrides[get_job_handler] = lambda: handler
    return TestClient(app)


def test_get_job_ok_and_cached() -> None:
    job_id = f"j-{uuid.uuid4()}"
    handler = DummyJobHandler()
    handler._job = DummyJob(
        job_id, {"id": job_id, "name": "Job 1", "components": [{"x": 1}]}
    )
    client = _build_app(handler)

    r1 = client.get(f"/jobs/{job_id}")
    assert r1.status_code == 200
    assert r1.json()["id"] == job_id
    assert handler.calls.load == 1

    r2 = client.get(f"/jobs/{job_id}")
    assert r2.status_code == 200
    assert handler.calls.load == 1


def test_get_job_not_found_maps_404() -> None:
    handler = DummyJobHandler()
    client = _build_app(handler)
    res = client.get("/jobs/missing")
    assert res.status_code == 404


def test_list_jobs_ok_and_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = DummyJobHandler()
    handler._rows = [{"id": "a"}, {"id": "b"}]
    client = _build_app(handler)
    R.invalidate_job_caches()

    r1 = client.get("/jobs")
    assert r1.status_code == 200 and r1.json() == handler._rows
    assert handler.calls.list == 1

    r2 = client.get("/jobs")
    assert r2.status_code == 200 and handler.calls.list == 1  # cached


def test_list_jobs_db_error_maps_to_http_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = DummyJobHandler()
    client = _build_app(handler)
    R.invalidate_job_caches()

    handler._errors["list"] = SQLAlchemyError("db down")
    res = client.get("/jobs")
    assert res.status_code == 500


def _fake_jobcfg() -> Any:
    return SimpleNamespace(x=1)


def test_create_job_success(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = DummyJobHandler()
    out = R.create_job(_fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert out == "created-1"


def test_create_job_validation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = DummyJobHandler()
    monkeypatch.setattr(HE, "_sanitize_errors", lambda e: [])
    monkeypatch.setattr(HE, "_exc_meta", lambda e: {})

    handler._errors["create"] = ValidationError.from_exception_data("X", [])
    with pytest.raises(HTTPException) as ei:
        R.create_job(_fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert ei.value.status_code == 422


def test_create_job_integrity_and_sa_and_generic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = DummyJobHandler()
    monkeypatch.setattr(HE, "_exc_meta", lambda e: {})

    handler._errors["create"] = IntegrityError("bad", None, None)
    with pytest.raises(HTTPException) as ei1:
        R.create_job(_fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert ei1.value.status_code == 409

    handler._errors["create"] = SQLAlchemyError("db boom")
    with pytest.raises(HTTPException) as ei2:
        R.create_job(_fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert ei2.value.status_code == 500

    handler._errors["create"] = RuntimeError("x")
    with pytest.raises(HTTPException) as ei3:
        R.create_job(_fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert ei3.value.status_code == 500


def test_update_job_success(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = DummyJobHandler()
    out = R.update_job("j9", _fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert out == "j9"


def test_update_job_all_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = DummyJobHandler()
    monkeypatch.setattr(HE, "_sanitize_errors", lambda e: [])
    monkeypatch.setattr(HE, "_exc_meta", lambda e: {})

    handler._errors["update"] = ValidationError.from_exception_data("X", [])
    with pytest.raises(HTTPException) as e1:
        R.update_job("j1", _fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert e1.value.status_code == 422

    handler._errors["update"] = PersistLinkageError("bad link")
    with pytest.raises(HTTPException) as e2:
        R.update_job("j1", _fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert e2.value.status_code == 500

    handler._errors["update"] = IntegrityError("bad", None, None)
    with pytest.raises(HTTPException) as e3:
        R.update_job("j1", _fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert e3.value.status_code == 409

    handler._errors["update"] = SQLAlchemyError("db")
    with pytest.raises(HTTPException) as e4:
        R.update_job("j1", _fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert e4.value.status_code == 500

    handler._errors["update"] = PersistNotFoundError("nope")
    with pytest.raises(HTTPException) as e5:
        R.update_job("j1", _fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert e5.value.status_code == 404

    handler._errors["update"] = RuntimeError("x")
    with pytest.raises(HTTPException) as e6:
        R.update_job("j1", _fake_jobcfg(), handler)  # type: ignore[arg-type]
    assert e6.value.status_code == 500


def test_delete_job_success() -> None:
    handler = DummyJobHandler()
    out = R.delete_job("j1", handler)
    assert out["message"].startswith("Job 'j1'")


def test_delete_job_errors() -> None:
    handler = DummyJobHandler()

    handler._errors["delete"] = PersistNotFoundError("nope")
    with pytest.raises(HTTPException) as e1:
        R.delete_job("j1", handler)
    assert e1.value.status_code == 404

    handler._errors["delete"] = RuntimeError("x")
    with pytest.raises(HTTPException) as e2:
        R.delete_job("j1", handler)
    assert e2.value.status_code == 500
