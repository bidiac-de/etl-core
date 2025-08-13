from typing import Any, Dict, Union
from sqlmodel import Session

from src.persistance.errors import PersistNotFoundError
from src.persistance.table_definitions import MetaDataTable, LayoutTable

RowOrId = Union[str, MetaDataTable, LayoutTable]


class DataClassHandler:
    """CRUD helper for any Metadata or Layout rows."""

    def create_metadata_entry(
        self, session: Session, data: Dict[str, Any]
    ) -> MetaDataTable:
        meta = MetaDataTable(**data)
        session.add(meta)
        session.flush()
        return meta

    def create_layout_entry(
        self, session: Session, data: Dict[str, Any]
    ) -> LayoutTable:
        layout = LayoutTable(**data)
        session.add(layout)
        session.flush()
        return layout

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
