from __future__ import annotations

from typing import Any, Dict, Union

from sqlmodel import Session

from src.persistance.errors import PersistNotFoundError
from src.persistance.table_definitions import (
    ComponentTable,
    JobTable,
    LayoutTable,
    MetaDataTable,
)

RowOrId = Union[str, MetaDataTable, LayoutTable]


class DataClassHandler:
    """CRUD Handler for Metadata and Layout rows."""

    def create_metadata_for_job(
        self, session: Session, owner: JobTable, data: Dict[str, Any]
    ) -> MetaDataTable:
        row = MetaDataTable(**data, job_id=owner.id)
        session.add(row)
        session.flush()
        return row

    def create_metadata_for_component(
        self, session: Session, owner: ComponentTable, data: Dict[str, Any]
    ) -> MetaDataTable:
        row = MetaDataTable(**data, component_id=owner.id)
        session.add(row)
        session.flush()
        return row

    def create_layout_for_component(
        self, session: Session, owner: ComponentTable, data: Dict[str, Any]
    ) -> LayoutTable:
        row = LayoutTable(**data, component_id=owner.id)
        session.add(row)
        session.flush()
        return row

    def update_metadata_entry(
        self, session: Session, meta: RowOrId, data: Dict[str, Any]
    ) -> MetaDataTable:
        row = (
            meta
            if isinstance(meta, MetaDataTable)
            else session.get(MetaDataTable, meta)
        )
        if row is None:
            raise PersistNotFoundError("MetaData row not found")
        for k, v in data.items():
            if hasattr(row, k):
                setattr(row, k, v)
        session.add(row)
        session.flush()
        return row

    def update_layout_entry(
        self, session: Session, layout: RowOrId, data: Dict[str, Any]
    ) -> LayoutTable:
        row = (
            layout
            if isinstance(layout, LayoutTable)
            else session.get(LayoutTable, layout)
        )
        if row is None:
            raise PersistNotFoundError("Layout row not found")
        for k, v in data.items():
            if hasattr(row, k):
                setattr(row, k, v)
        session.add(row)
        session.flush()
        return row

    def dump_metadata(self, meta: MetaDataTable) -> Dict[str, Any]:
        return meta.model_dump()

    def dump_layout(self, layout: LayoutTable) -> Dict[str, Any]:
        return layout.model_dump()
