from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Callable
import importlib
import pytest
from fastapi.testclient import TestClient


def _schemas_router_module(client: TestClient):
    """
    Resolve the *exact* schemas router module object that the mounted app uses.
    We find the /configs/job endpoint and import its module; both endpoints
    live in the same router module.
    """
    for route in client.app.routes:
        if getattr(route, "path", None) == "/configs/job":
            endpoint = getattr(route, "endpoint", None)
            if endpoint is None:
                break
            return importlib.import_module(endpoint.__module__)
    pytest.skip("Schemas router not mounted on /configs/job")


def _router_has_cache(client: TestClient) -> bool:
    mod = _schemas_router_module(client)
    return hasattr(mod, "invalidate_schema_caches") and hasattr(
        mod, "_JOB_SCHEMA_CACHE"
    )


def _require_cache_or_skip(client: TestClient) -> None:
    if not _router_has_cache(client):
        pytest.skip(
            "Schema caching layer not installed in etl_core.api.routers.schemas"
        )


def _get_first_public_component(client: TestClient) -> str | None:
    resp = client.get("/configs/component_types")
    assert resp.status_code == 200
    types = resp.json()
    assert isinstance(types, list)
    return types[0] if types else None


def test_job_schema_is_cached(
    fresh_client: Callable[[str], TestClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    First call should compute schema; second call should hit cache.
    Run against a fresh app so we patch before any cache warm-up.
    """
    client = fresh_client("test")
    _require_cache_or_skip(client)

    calls: Dict[str, int] = {"job_model_json_schema": 0, "inline_defs": 0}

    # Patch JobBase.model_json_schema
    import etl_core.persistance.base_models.job_base as job_base

    real_job_schema = job_base.JobBase.model_json_schema

    def _wrapped_job_schema() -> Dict[str, Any]:
        calls["job_model_json_schema"] += 1
        return real_job_schema()

    monkeypatch.setattr(
        job_base.JobBase, "model_json_schema", staticmethod(_wrapped_job_schema)
    )

    # Patch inline_defs on the live router module (job schema uses inline_defs)
    schemas_router = _schemas_router_module(client)
    real_inline_defs = schemas_router.inline_defs  # type: ignore[attr-defined]

    def _wrapped_inline_defs(schema: Dict[str, Any]) -> Dict[str, Any]:
        calls["inline_defs"] += 1
        return real_inline_defs(schema)

    monkeypatch.setattr(schemas_router, "inline_defs", _wrapped_inline_defs)

    # Start clean
    schemas_router.invalidate_schema_caches()  # type: ignore[attr-defined]

    # miss -> compute
    r1 = client.get("/configs/job")
    assert r1.status_code == 200
    # hit -> no recompute
    r2 = client.get("/configs/job")
    assert r2.status_code == 200

    assert calls["job_model_json_schema"] == 1
    assert calls["inline_defs"] == 1


def test_component_schema_is_cached_and_invalidates(
    fresh_client: Callable[[str], TestClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Ensure component schema caching works and invalidate_schema_caches()
    forces recompute. Component schemas are NOT inlined in the router.
    """
    client = fresh_client("test")
    _require_cache_or_skip(client)

    comp = _get_first_public_component(client)
    if not comp:
        pytest.skip("No public components available to test component schema caching")

    calls: Dict[str, int] = {"model_json_schema": 0}

    # Find the component class in the live registry
    import etl_core.components.component_registry as registry

    cls = registry.component_registry.get(comp)
    assert cls is not None, f"Component {comp!r} not in registry"

    real_comp_schema = cls.model_json_schema

    def _wrapped_comp_schema() -> Dict[str, Any]:
        calls["model_json_schema"] += 1
        return real_comp_schema()

    monkeypatch.setattr(cls, "model_json_schema", staticmethod(_wrapped_comp_schema))

    schemas_router = _schemas_router_module(client)
    schemas_router.invalidate_schema_caches()  # type: ignore[attr-defined]

    # miss
    r1 = client.get(f"/configs/{comp}/form")
    assert r1.status_code == 200
    # hit
    r2 = client.get(f"/configs/{comp}/form")
    assert r2.status_code == 200

    assert calls["model_json_schema"] == 1

    # invalidate -> recompute
    schemas_router.invalidate_schema_caches()  # type: ignore[attr-defined]
    r3 = client.get(f"/configs/{comp}/form")
    assert r3.status_code == 200
    assert calls["model_json_schema"] == 2


def test_mode_is_part_of_cache_key(
    fresh_client: Callable[[str], TestClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    If registry mode changes, cached job schema should not be reused across modes.
    """
    client = fresh_client("test")
    _require_cache_or_skip(client)

    schemas_router = _schemas_router_module(client)
    schemas_router.invalidate_schema_caches()  # type: ignore[attr-defined]

    class _FakeMode:
        def __init__(self, value: str) -> None:
            self.value = value

        def __eq__(self, other: object) -> bool:
            return isinstance(other, _FakeMode) and self.value == other.value

    def _fake_mode_a() -> _FakeMode:
        return _FakeMode("development")

    def _fake_mode_b() -> _FakeMode:
        return _FakeMode("production")

    # Count calls
    import etl_core.persistance.base_models.job_base as job_base

    job_calls: Dict[str, int] = {"model_json_schema": 0}
    real_job_schema = job_base.JobBase.model_json_schema

    def _wrapped_job_schema() -> Dict[str, Any]:
        job_calls["model_json_schema"] += 1
        return real_job_schema()

    monkeypatch.setattr(
        job_base.JobBase, "model_json_schema", staticmethod(_wrapped_job_schema)
    )

    # A -> compute
    monkeypatch.setattr(schemas_router, "get_registry_mode", _fake_mode_a)
    assert client.get("/configs/job").status_code == 200

    # B -> compute again
    monkeypatch.setattr(schemas_router, "get_registry_mode", _fake_mode_b)
    assert client.get("/configs/job").status_code == 200

    # back to A -> hit A cache
    monkeypatch.setattr(schemas_router, "get_registry_mode", _fake_mode_a)
    assert client.get("/configs/job").status_code == 200

    assert job_calls["model_json_schema"] == 2


def test_job_schema_cache_is_thread_safe(
    fresh_client: Callable[[str], TestClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Fire many concurrent requests; the expensive function should still only run once.
    """
    client = fresh_client("test")
    _require_cache_or_skip(client)

    import etl_core.persistance.base_models.job_base as job_base

    schemas_router = _schemas_router_module(client)
    schemas_router.invalidate_schema_caches()  # type: ignore[attr-defined]

    calls = {"job_model_json_schema": 0}
    real_job_schema = job_base.JobBase.model_json_schema

    def _wrapped_job_schema() -> Dict[str, Any]:
        calls["job_model_json_schema"] += 1
        return real_job_schema()

    monkeypatch.setattr(
        job_base.JobBase, "model_json_schema", staticmethod(_wrapped_job_schema)
    )

    def _hit() -> int:
        r = client.get("/configs/job")
        return r.status_code

    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = [pool.submit(_hit) for _ in range(40)]
        statuses = [f.result() for f in as_completed(futs)]

    assert all(s == 200 for s in statuses)
    assert calls["job_model_json_schema"] == 1
