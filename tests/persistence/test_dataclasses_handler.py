from __future__ import annotations

from typing import Dict, Tuple, Type, TypeVar
from uuid import uuid4
from datetime import datetime

import pytest

import etl_core.persistence.handlers.dataclasses_handler as H
from etl_core.persistence.errors import PersistNotFoundError
from etl_core.persistence.table_definitions import (
    ComponentTable,
    JobTable,
    LayoutTable,
    MetaDataTable,
)

T = TypeVar("T", MetaDataTable, LayoutTable)


class FakeSession:
    """
    Tiny in-memory stand-in for sqlmodel.Session that does enough for the
    DataClassHandler tests: add(), flush(), get().
    """

    def __init__(self) -> None:
        self._store: Dict[Tuple[Type[object], str], object] = {}

    def add(self, row: object) -> None:
        if getattr(row, "id", None) is None:
            object.__setattr__(row, "id", str(uuid4()))
        key = (type(row), getattr(row, "id"))
        self._store[key] = row

    def flush(self) -> None:
        return

    def get(self, model: Type[T], row_id: str) -> T | None:
        return self._store.get((model, row_id))  # type: ignore[return-value]


def _dummy_job() -> JobTable:
    # Only id is needed for FK; other fields get defaults in the model.
    return JobTable(id=str(uuid4()))


def _dummy_component() -> ComponentTable:
    return ComponentTable(id=str(uuid4()))


@pytest.fixture()
def session() -> FakeSession:
    return FakeSession()


@pytest.fixture()
def handler() -> H.DataClassHandler:
    return H.DataClassHandler()


def test_create_metadata_for_job_and_component(
    session: FakeSession, handler: H.DataClassHandler
) -> None:
    job = _dummy_job()
    comp = _dummy_component()

    meta_job = handler.create_metadata_for_job(session, job, data={})
    assert isinstance(meta_job, MetaDataTable)
    assert getattr(meta_job, "job_id") == getattr(job, "id")
    assert isinstance(getattr(meta_job, "timestamp"), datetime)

    meta_comp = handler.create_metadata_for_component(session, comp, data={})
    assert isinstance(meta_comp, MetaDataTable)
    assert getattr(meta_comp, "component_id") == getattr(comp, "id")
    assert isinstance(getattr(meta_comp, "timestamp"), datetime)


def test_create_layout_for_component(
    session: FakeSession, handler: H.DataClassHandler
) -> None:
    comp = _dummy_component()

    layout = handler.create_layout_for_component(
        session, comp, data={"x_coordinate": 10, "y_coordinate": 20}
    )
    assert isinstance(layout, LayoutTable)
    assert getattr(layout, "component_id") == getattr(comp, "id")
    assert layout.x_coordinate == 10
    assert layout.y_coordinate == 20


def test_update_metadata_entry_by_row_and_by_id(
    session: FakeSession, handler: H.DataClassHandler
) -> None:
    job = _dummy_job()
    meta = handler.create_metadata_for_job(session, job, data={})

    # Update by passing object
    out1 = handler.update_metadata_entry(
        session, meta, {"user_id": "alice", "unknown": 123}
    )
    assert out1.user_id == "alice"  # unknown is ignored

    # Update by id
    out2 = handler.update_metadata_entry(session, out1.id, {"user_id": "bob"})
    assert out2.user_id == "bob"


def test_update_layout_entry_by_row_and_by_id(
    session: FakeSession, handler: H.DataClassHandler
) -> None:
    comp = _dummy_component()
    layout = handler.create_layout_for_component(
        session, comp, data={"x_coordinate": 1, "y_coordinate": 2}
    )

    # by row
    out1 = handler.update_layout_entry(
        session, layout, {"x_coordinate": 11, "unknown": "nope"}
    )
    assert out1.x_coordinate == 11

    # by id
    out2 = handler.update_layout_entry(session, out1.id, {"y_coordinate": 22})
    assert out2.y_coordinate == 22


def test_update_metadata_not_found_raises(
    session: FakeSession, handler: H.DataClassHandler
) -> None:
    with pytest.raises(PersistNotFoundError):
        handler.update_metadata_entry(session, "missing-id", {"user_id": "x"})


def test_update_layout_not_found_raises(
    session: FakeSession, handler: H.DataClassHandler
) -> None:
    with pytest.raises(PersistNotFoundError):
        handler.update_layout_entry(session, "missing-id", {"x_coordinate": 1})


def test_dump_helpers_exclude_none(
    session: FakeSession, handler: H.DataClassHandler
) -> None:
    comp = _dummy_component()

    meta = handler.create_metadata_for_component(session, comp, data={})
    layout = handler.create_layout_for_component(
        session, comp, data={"x_coordinate": 3, "y_coordinate": 4}
    )

    if hasattr(meta, "user_id"):
        meta.user_id = None  # type: ignore[assignment]

    dump_m = handler.dump_metadata(meta)
    dump_l = handler.dump_layout(layout)

    assert "id" in dump_m and "timestamp" in dump_m
    assert "user_id" not in dump_m  # excluded because None
    assert dump_l["x_coordinate"] == 3 and dump_l["y_coordinate"] == 4
