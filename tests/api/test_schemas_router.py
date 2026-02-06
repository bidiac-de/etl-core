from types import SimpleNamespace
from typing import Any, Dict

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from etl_core.api.routers import schemas


@pytest.fixture(autouse=True)
def clear_caches():
    schemas.invalidate_schema_caches()
    yield
    schemas.invalidate_schema_caches()


@pytest.fixture()
def app_client(monkeypatch):
    """
    Mount just the /configs router into a minimal FastAPI app.
    Also provide very lightweight defaults for registry/mode so
    the endpoints can run without your full system.
    """
    monkeypatch.setattr(
        schemas, "get_registry_mode", lambda: SimpleNamespace(value="DEV")
    )

    monkeypatch.setattr(schemas, "schema_post_processing", lambda data, **_: data)

    monkeypatch.setattr(
        schemas.JobBase,
        "model_json_schema",
        classmethod(
            lambda cls: {"type": "object", "properties": {"a": {"type": "string"}}}
        ),
    )

    monkeypatch.setattr(schemas, "component_registry", {}, raising=False)

    monkeypatch.setattr(
        schemas, "component_meta", lambda comp: SimpleNamespace(hidden=False)
    )

    monkeypatch.setattr(schemas, "public_component_types", lambda: ["Alpha", "Beta"])

    app = FastAPI()
    app.include_router(schemas.router)
    return TestClient(app)


def test_get_job_schema_ok(app_client: TestClient):
    r = app_client.get("/configs/job")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "object"
    assert "properties" in body


def test_get_job_schema_validation_error_yields_422(
    app_client: TestClient, monkeypatch
):
    ve = ValidationError.from_exception_data(
        "JobBase",
        [{"type": "string_type", "loc": ("a",), "msg": "bad", "input": None}],
    )
    monkeypatch.setattr(
        schemas, "schema_post_processing", lambda data, **_: (_ for _ in ()).throw(ve)
    )

    r = app_client.get("/configs/job")
    assert r.status_code == 422


def test_get_job_schema_unexpected_error_yields_500(
    app_client: TestClient, monkeypatch
):
    monkeypatch.setattr(
        schemas,
        "schema_post_processing",
        lambda data, **_: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    r = app_client.get("/configs/job")
    assert r.status_code == 500


def test_list_component_types_ok(app_client: TestClient, monkeypatch):
    monkeypatch.setattr(schemas, "public_component_types", lambda: ["X", "Y", "Z"])
    schemas.invalidate_schema_caches()
    r = app_client.get("/configs/component_types")
    assert r.status_code == 200
    assert r.json() == ["X", "Y", "Z"]


def test_list_component_types_error_500(app_client: TestClient, monkeypatch):
    def boom():
        raise RuntimeError("nope")

    monkeypatch.setattr(schemas, "public_component_types", boom)
    schemas.invalidate_schema_caches()
    r = app_client.get("/configs/component_types")
    assert r.status_code == 500


class DummyComponent:
    ICON = "mdi:test-icon"

    @classmethod
    def model_json_schema(cls) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "name": {"type": "string", "title": "Name"},
                "x": {"type": "integer"},
                "y": {"type": "string"},
            },
            "required": ["name", "x"],
        }


def mount_dummy(monkeypatch, hidden=False):
    monkeypatch.setattr(
        schemas, "component_registry", {"Dummy": DummyComponent}, raising=False
    )
    monkeypatch.setattr(
        schemas, "component_meta", lambda comp: SimpleNamespace(hidden=hidden)
    )


def test_get_component_schema_form_ok(app_client: TestClient, monkeypatch):
    mount_dummy(monkeypatch, hidden=False)
    r = app_client.get("/configs/Dummy/form")
    assert r.status_code == 200
    payload = r.json()

    assert "x-class" in payload
    assert payload.get("comp-type") == "Dummy"
    assert (
        payload.get("properties", {}).get("name", {}).get("default") == "DummyComponent"
    )
    assert payload.get("icon") == "mdi:test-icon"


def test_get_component_schema_form_404_when_hidden_in_production(
    app_client: TestClient, monkeypatch
):
    monkeypatch.setattr(
        schemas, "get_registry_mode", lambda: schemas.RegistryMode.PRODUCTION
    )
    mount_dummy(monkeypatch, hidden=True)
    r = app_client.get("/configs/Dummy/form")
    assert r.status_code == 404


def test_get_component_schema_form_422_on_validation_error(
    app_client: TestClient, monkeypatch
):
    mount_dummy(monkeypatch, hidden=False)
    ve = ValidationError.from_exception_data(
        "Dummy",
        [{"type": "int_type", "loc": ("x",), "msg": "bad", "input": None}],
    )
    monkeypatch.setattr(
        schemas, "schema_post_processing", lambda data, **_: (_ for _ in ()).throw(ve)
    )
    r = app_client.get("/configs/Dummy/form")
    assert r.status_code == 422


def test_get_component_schema_form_500_on_other_error(
    app_client: TestClient, monkeypatch
):
    mount_dummy(monkeypatch, hidden=False)
    monkeypatch.setattr(
        schemas,
        "schema_post_processing",
        lambda data, **_: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    r = app_client.get("/configs/Dummy/form")
    assert r.status_code == 500


def test_get_component_schema_full_ok(app_client: TestClient, monkeypatch):
    mount_dummy(monkeypatch, hidden=False)
    r = app_client.get("/configs/Dummy/full")
    assert r.status_code == 200
    payload = r.json()
    assert "x-class" in payload
    assert "comp-type" not in payload


def test_get_component_schema_full_404_when_unknown_in_production(
    app_client: TestClient, monkeypatch
):
    monkeypatch.setattr(
        schemas, "get_registry_mode", lambda: schemas.RegistryMode.PRODUCTION
    )
    monkeypatch.setattr(schemas, "component_registry", {}, raising=False)
    monkeypatch.setattr(schemas, "component_meta", lambda comp: None)
    r = app_client.get("/configs/Nope/full")
    assert r.status_code == 404


def test_get_component_schema_hidden_ok(app_client: TestClient, monkeypatch):
    mount_dummy(monkeypatch, hidden=False)
    r = app_client.get("/configs/Dummy/hidden")
    assert r.status_code == 200
    payload = r.json()
    assert payload.get("type") == "object"
    assert isinstance(payload.get("properties"), dict)


def test_get_component_schema_hidden_404_in_production_when_hidden(
    app_client: TestClient, monkeypatch
):
    monkeypatch.setattr(
        schemas, "get_registry_mode", lambda: schemas.RegistryMode.PRODUCTION
    )
    mount_dummy(monkeypatch, hidden=True)
    r = app_client.get("/configs/Dummy/hidden")
    assert r.status_code == 404
