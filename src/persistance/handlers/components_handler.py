from __future__ import annotations

from typing import List, cast
from sqlmodel import Session, select
from sqlmodel.sql.expression import Select
from sqlalchemy.sql.elements import ColumnElement

from src.persistance.table_definitions import (
    ComponentTable,
    ComponentNextLink,
)
from src.components.component_registry import component_registry
from src.persistance.handlers.dataclasses_handler import DataClassHandler


class ComponentHandler:
    """
    Handler for CRUD operations on ComponentTable as well
    as re-building runtime components.
    """

    def __init__(self, dc_handler: DataClassHandler):
        self.dc = dc_handler

    def create_all(
        self,
        session: Session,
        job_record,
        runtime_components: List,
    ) -> None:
        """Insert components for job_record. Links are wired afterwards."""
        for comp in runtime_components:
            layout = self.dc.create_layout_entry(session, comp.layout.model_dump())
            meta = self.dc.create_metadata_entry(session, comp.metadata_.model_dump())

            ct = ComponentTable(
                id=comp.id,
                name=comp.name,
                description=comp.description,
                comp_type=comp.comp_type,
                job=job_record,
                layout=layout,
                metadata_=meta,
            )
            session.add(ct)

        session.flush()
        self._wire_next_links(session, job_record, runtime_components)

    def replace_all(
        self,
        session: Session,
        job_record,
        domain_components: List,
    ) -> None:
        """
        Replace all components for the given job:
        - delete existing links + components
        - insert new rows + links
        This is simple, predictable, and keeps logic local.
        """
        self.delete_all(session, job_record)
        session.flush()
        self.create_all(session, job_record, domain_components)

    def delete_all(self, session: Session, job_record) -> None:
        """Delete all components for a job, including link rows and metadata/layout."""
        comp_ids = [c.id for c in (job_record.components or [])]
        if comp_ids:
            # Cast ORM-typed attributes to SQL expression columns for type checkers
            component_id_col = cast(ColumnElement[str], ComponentNextLink.component_id)
            next_id_col = cast(ColumnElement[str], ComponentNextLink.next_id)

            stmt_links = cast(
                Select[ComponentNextLink],
                select(ComponentNextLink).where(
                    component_id_col.in_(comp_ids) | next_id_col.in_(comp_ids)
                ),
            )
            for row in session.exec(stmt_links):
                session.delete(row)
            session.flush()

        # delete components (and their 1-1 layout/metadata)
        for comp in job_record.components or []:
            if comp.layout is not None:
                session.delete(comp.layout)
            if comp.metadata is not None:
                session.delete(comp.metadata)
            session.delete(comp)

    def hydrate_all(self, job_record) -> List:
        """Build domain components from a job_record with relationships loaded."""
        runtime_comps = []
        for ct in job_record.components:
            data = {
                "id": ct.id,
                "name": ct.name,
                "description": ct.description,
                "comp_type": ct.comp_type,
                "next": [n.name for n in ct.next_components],
                "layout": self.dc.dump_layout(ct.layout),
                "metadata": self.dc.dump_metadata(ct.metadata),
            }
            cls = component_registry[ct.comp_type]
            obj = cls(**data)
            # restore private id so the runtime matches DB row ids
            object.__setattr__(obj, "_id", ct.id)
            runtime_comps.append(obj)
        return runtime_comps

    def _wire_next_links(
        self,
        session: Session,
        job_record,
        domain_components: List,
    ) -> None:
        stmt_comps = cast(
            Select[ComponentTable],
            select(ComponentTable).where(ComponentTable.job_id == job_record.id),
        )
        name_to_id = {row.name: row.id for row in session.exec(stmt_comps)}
        for comp in domain_components:
            src_id = comp.id
            for nxt_name in comp.next:
                session.add(
                    ComponentNextLink(
                        component_id=src_id,
                        next_id=name_to_id[nxt_name],
                    )
                )
