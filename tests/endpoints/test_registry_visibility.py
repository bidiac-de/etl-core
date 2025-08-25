from __future__ import annotations

from typing import Set

import pytest
from fastapi.testclient import TestClient

STUB_TYPES: Set[str] = {
    "test",
    "failtest",
    "stub_fail_once",
    "multi_source",
    "multi_echo",
}


def test_production_hides_stub_components(fresh_client) -> None:
    """
    In production mode, hidden stub component types must not be listed.
    """
    client: TestClient = fresh_client("production")
    visible = set(client.get("/configs/component_types").json())
    leaked = STUB_TYPES & visible
    assert not leaked, f"Stub components leaked in production: {sorted(leaked)}"


@pytest.mark.parametrize("stub", sorted(STUB_TYPES))
def test_production_stub_schema_404(fresh_client, stub: str) -> None:
    """
    In production mode, requesting a stub component's schema should 404.
    """
    client: TestClient = fresh_client("production")
    resp = client.get(f"/configs/{stub}")
    assert resp.status_code == 404, f"Expected 404 for hidden component {stub!r}"
