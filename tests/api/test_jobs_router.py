from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from etl_core.api.routers import jobs as R
from etl_core.persistence.errors import PersistNotFoundError


class DummyJob:
    def __init__(self, id_: str, data: dict):
        self.id = id_
        self._data = data

    def model_dump(self):
        return dict(self._data)


class DummyJobHandler:
    def __init__(self):
        self.calls = SimpleNamespace(list=0, load=0, create=0, update=0, delete=0)
        self._rows = []
        self._job = None
        self._raise_on_list = None

    def list_jobs_brief(self):
        self.calls.list += 1
        if self._raise_on_list:
            raise self._raise_on_list
        return list(self._rows)

    def load_runtime_job(self, job_id: str):
        self.calls.load += 1
        if self._job is None:
            raise PersistNotFoundError(f"no job {job_id}")
        return self._job

    def create_job_entry(self, job_cfg):
        self.calls.create += 1
        return SimpleNamespace(id="new-id")

    def update(self, job_id, job_cfg):
        self.calls.update += 1
        return SimpleNamespace(id=job_id)

    def delete(self, job_id):
        self.calls.delete += 1


def test_cached_job_list_and_invalidation(monkeypatch):
    handler = DummyJobHandler()
    handler._rows = [{"id": "a"}, {"id": "b"}]

    # first call populates cache
    out1 = R._cached_job_list(handler)
    assert out1 == handler._rows
    assert handler.calls.list == 1

    # second returns from cache without calling handler
    out2 = R._cached_job_list(handler)
    assert out2 == handler._rows and handler.calls.list == 1

    # invalidate and ensure handler is called again
    R.invalidate_job_caches()
    out3 = R._cached_job_list(handler)
    assert out3 == handler._rows and handler.calls.list == 2


def test_cached_job_404_when_missing():
    handler = DummyJobHandler()
    with pytest.raises(HTTPException) as ei:
        R._cached_job("missing", handler)
    assert ei.value.status_code == 404


def test_cached_job_successful_and_caches():
    handler = DummyJobHandler()
    job = DummyJob("j1", {"name": "Job 1"})
    handler._job = job

    out = R._cached_job("j1", handler)
    assert out["id"] == "j1" and out["name"] == "Job 1"

    # second call should come from cache (no extra handler call)
    before = handler.calls.load
    out2 = R._cached_job("j1", handler)
    assert out2 == out and handler.calls.load == before


def test_list_jobs_db_error_maps_to_http(monkeypatch):
    handler = DummyJobHandler()
    import sqlalchemy

    # ensure no cached list interferes
    R.invalidate_job_caches()

    handler._raise_on_list = sqlalchemy.exc.SQLAlchemyError("boom")
    with pytest.raises(HTTPException) as ei:
        R._cached_job_list(handler)
    assert ei.value.status_code == 500
