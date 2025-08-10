from __future__ import annotations

from typing import List, cast, Dict
from sqlmodel import Session, select
from sqlmodel.sql.expression import Select
from sqlalchemy.sql.elements import ColumnElement

from src.components.base_component import Component
from src.persistance.table_definitions import (
    ComponentTable,
    ComponentNextLink,
    JobTable,
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

    def create_component_entry(
        self,
        session: Session,
        job_record: JobTable,
        cfg: Component,
    ) -> ComponentTable:
        """
        Insert a single component row for job_record and wire its `next` links.
        `cfg.next` must reference existing component names on the same job,
        or ValueError is raised.
        """
        ct = self._insert_component_row(session, job_record, cfg)
        session.flush()

        name_to_id = self._fetch_component_name_id_map(session, job_record)
        missing = [n for n in cfg.next if n not in name_to_id]
        if missing:
            raise ValueError(
                f"Unknown next-component(s) for '{cfg.name}': {sorted(missing)}"
            )

        self._wire_next_links_by_names(
            session,
            source_name=cfg.name,
            source_id=ct.id,
            next_names=cfg.next,
            name_to_id=name_to_id,
        )
        return ct

    def create_all_from_configs(
        self,
        session: Session,
        job_record: JobTable,
        component_cfgs: List[Component],
    ) -> Dict[str, str]:
        """
        Insert all components for job_record using config-only inputs.
        Returns name->id map of the inserted components.
        """
        name_to_id: Dict[str, str] = {}
        for cfg in component_cfgs:
            ct = self._insert_component_row(session, job_record, cfg)
            session.flush()  # ensure ct.id is assigned
            name_to_id[cfg.name] = ct.id

        # wire links after all inserts exist
        for cfg in component_cfgs:
            self._wire_next_links_by_names(
                session,
                source_name=cfg.name,
                source_id=name_to_id[cfg.name],
                next_names=cfg.next,
                name_to_id=name_to_id,
            )
        return name_to_id

    def replace_all_from_configs(
        self,
        session: Session,
        job_record: JobTable,
        component_cfgs: List[Component],
    ) -> Dict[str, str]:
        """
        Replace all components for a job: delete, insert, rewire.
        """
        self.delete_all(session, job_record)
        session.flush()
        session.expire(job_record, ["components"])
        return self.create_all_from_configs(session, job_record, component_cfgs)

    def delete_all(self, session: Session, job_record: JobTable) -> None:
        """
        Delete all components for a job, including link rows and metadata/layout.

        Ordering rules:
          1) delete next-link rows
          2) collect child rows (layout, metadata_) while parents are alive
          3) delete parents (ComponentTable)
          4) delete child rows (LayoutTable, MetaDataTable)
          5) clear/expire relationship so no deleted instances linger in-memory
        """
        comp_rows: List[ComponentTable] = list(job_record.components or [])
        if not comp_rows:
            return

        comp_ids = [c.id for c in comp_rows]

        # 1) delete next-link rows first
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

        # 2) collect child rows WHILE parents are still present
        layouts = [c.layout for c in comp_rows if c.layout is not None]
        metas = [
            c.metadata_ for c in comp_rows if getattr(c, "metadata_", None) is not None
        ]

        # 3) delete parents first to avoid NULLing non-null FKs
        with session.no_autoflush:
            for comp in comp_rows:
                session.delete(comp)
        session.flush()

        # 4) delete child rows (now orphaned or no longer referenced)
        for row in layouts:
            session.delete(row)
        for row in metas:
            session.delete(row)
        session.flush()

        # 5) clear and expire the parent-side relationship to drop deleted instances
        if getattr(job_record, "components", None) is not None:
            job_record.components.clear()
        session.flush()
        session.expire(job_record, ["components"])

    def build_runtime_for_all(self, job_record) -> List:
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
                "metadata": self.dc.dump_metadata(ct.metadata_),
            }
            cls = component_registry[ct.comp_type]
            obj = cls(**data)
            # restore private id so the runtime matches DB row ids
            object.__setattr__(obj, "_id", ct.id)
            runtime_comps.append(obj)

        name_map = {c.name: c for c in runtime_comps}

        # wire up components using comprehension
        for comp in runtime_comps:
            try:
                next_objs = [name_map[n] for n in comp.next]
            except KeyError as e:
                raise ValueError(f"Unknown next‐component name: {e.args[0]!r}")
            comp.next_components = next_objs
            for nxt in next_objs:
                nxt.prev_components.append(comp)

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

    def _insert_component_row(
        self,
        session: Session,
        job_record: JobTable,
        cfg: Component,
    ) -> ComponentTable:
        """
        Creates layout/metadata rows, then the component row.
        """
        layout = self.dc.create_layout_entry(session, cfg.layout.model_dump())
        meta = self.dc.create_metadata_entry(session, cfg.metadata_.model_dump())

        ct = ComponentTable(
            # id is auto via default_factory on the table model
            name=cfg.name,
            description=cfg.description,
            comp_type=cfg.comp_type,
            job=job_record,
            layout=layout,
            metadata_=meta,
        )
        session.add(ct)
        return ct

    def _wire_next_links_by_names(
        self,
        session: Session,
        source_name: str,
        source_id: str,
        next_names: List[str],
        name_to_id: Dict[str, str],
    ) -> None:
        for nxt in next_names:
            dst_id = name_to_id.get(nxt)
            if dst_id is None:
                raise ValueError(
                    f"Component '{source_name}' references unknown next '{nxt}'"
                )
            session.add(
                ComponentNextLink(
                    component_id=source_id,
                    next_id=dst_id,
                )
            )

    def _fetch_component_name_id_map(
        self, session: Session, job_record: JobTable
    ) -> Dict[str, str]:
        stmt = select(ComponentTable).where(ComponentTable.job_id == job_record.id)
        return {row.name: row.id for row in session.exec(stmt)}
