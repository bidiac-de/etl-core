import types
import pytest
from unittest.mock import Mock
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

import etl_core.api.routers.jobs as J


class DummyObj:
    def __init__(self):
        self.name = "n"
        self.num_of_retries = 1
        self.file_logging = True
        self.strategy_type = "row"
        self.metadata_ = {"x": 1}
        self.components = [types.SimpleNamespace(x=1)]


def test_invalidate_job_caches_clears(monkeypatch):
    J._JOB_BY_ID_CACHE["id1"] = {"id": "id1"}
    J._JOB_LIST_CACHE = [{"id": "id1"}]
    J.invalidate_job_caches("id1")
    assert "id1" not in J._JOB_BY_ID_CACHE
    assert J._JOB_LIST_CACHE is None


def test_serialize_components_from_variants():
    # dict input
    d = {"components": [{"a": 1}]}
    out = J._serialize_components_from(d)
    assert out == [{"a": 1}]

    # simple object path
    class C:
        def __init__(self):
            self.y = 2
    o = types.SimpleNamespace(components=[C()])
    out2 = J._serialize_components_from(o)
    assert isinstance(out2[0], dict) and "y" in out2[0]

    # None path
    assert J._serialize_components_from({}) is None


def test_job_to_payload_model_dump_fails(monkeypatch):
    job = DummyObj()
    def bad_dump(): raise RuntimeError("x")
    job.model_dump = bad_dump
    out = J._job_to_payload(job, "jid")
    assert out["id"] == "jid"
    assert "components" in out


def test_cached_job_cache_hit(monkeypatch):
    J._JOB_BY_ID_CACHE["x"] = {"id": "x"}
    out = J._cached_job("x", Mock())
    assert out["id"] == "x"


def test_cached_job_errors(monkeypatch):
    handler = Mock()
    handler.load_runtime_job.side_effect = J.PersistNotFoundError("nope")
    with pytest.raises(HTTPException) as e:
        J._cached_job("id", handler)
    assert e.value.status_code == 404

    handler.load_runtime_job.side_effect = RuntimeError("boom")
    with pytest.raises(HTTPException) as e:
        J._cached_job("id", handler)
    assert e.value.status_code == 500


def test_cached_job_list_cache_and_errors(monkeypatch):
    # cache hit
    J._JOB_LIST_CACHE = [{"id": "1"}]
    out = J._cached_job_list(Mock())
    assert out == [{"id": "1"}]

    # SQLAlchemyError
    J._JOB_LIST_CACHE = None
    handler = Mock()
    handler.list_jobs_brief.side_effect = SQLAlchemyError("x")
    with pytest.raises(HTTPException):
        J._cached_job_list(handler)


def test_create_job_error_paths(monkeypatch):
    handler = Mock()
    cfg = Mock()
    handler.create_job_entry.side_effect = IntegrityError("s", "p", "o")
    with pytest.raises(HTTPException) as e:
        J.create_job(cfg, handler)
    assert e.value.status_code == 409

    handler.create_job_entry.side_effect = SQLAlchemyError("x")
    with pytest.raises(HTTPException) as e:
        J.create_job(cfg, handler)
    assert e.value.status_code == 500

    handler.create_job_entry.side_effect = RuntimeError("x")
    with pytest.raises(HTTPException) as e:
        J.create_job(cfg, handler)
    assert e.value.status_code == 500


def test_update_job_error_paths(monkeypatch):
    handler = Mock()
    cfg = Mock()
    handler.update.side_effect = J.PersistNotFoundError("x")
    with pytest.raises(HTTPException) as e:
        J.update_job("jid", cfg, handler)
    assert e.value.status_code == 404

    handler.update.side_effect = IntegrityError("s", "p", "o")
    with pytest.raises(HTTPException) as e:
        J.update_job("jid", cfg, handler)
    assert e.value.status_code == 409

    handler.update.side_effect = SQLAlchemyError("x")
    with pytest.raises(HTTPException) as e:
        J.update_job("jid", cfg, handler)
    assert e.value.status_code == 500

    handler.update.side_effect = RuntimeError("x")
    with pytest.raises(HTTPException) as e:
        J.update_job("jid", cfg, handler)
    assert e.value.status_code == 500


def test_delete_job_error(monkeypatch):
    handler = Mock()
    handler.delete.side_effect = RuntimeError("boom")
    with pytest.raises(HTTPException) as e:
        J.delete_job("jid", handler)
    assert e.value.status_code == 500
