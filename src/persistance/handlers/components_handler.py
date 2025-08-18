from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple, cast

from sqlalchemy.sql.elements import ColumnElement
from sqlmodel import Session, select
from sqlmodel.sql.expression import Select

from src.components.base_component import Component
from src.components.component_registry import component_registry
from src.persistance.errors import PersistLinkageError
from src.persistance.handlers.dataclasses_handler import DataClassHandler
from src.persistance.table_definitions import (
    ComponentNextLink,
    ComponentTable,
    JobTable,
)

BASE_FIELDS: Set[str] = {
    "name",
    "description",
    "comp_type",
    "next",
    "layout",
    "metadata_",
}


def _split_base_and_payload(
    data: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    norm = dict(data)
    if "metadata" in norm and "metadata_" not in norm:
        norm["metadata_"] = norm.pop("metadata")
    if "layout_" in norm and "layout" not in norm:
        norm["layout"] = norm.pop("layout_")

    base = {k: v for k, v in norm.items() if k in BASE_FIELDS}
    payload = {k: v for k, v in norm.items() if k not in BASE_FIELDS}
    return base, payload


class ComponentHandler:
    """
    Handler for CRUD operations on ComponentTable and
    re-building runtime components.
    """

    def __init__(self, dc_handler: DataClassHandler):
        self.dc = dc_handler

    def _name_id_map(self, session: Session, job_record: JobTable) -> Dict[str, str]:
        stmt = select(ComponentTable).where(ComponentTable.job_id == job_record.id)
        return {row.name: row.id for row in session.exec(stmt)}

    def _wire_links(
        self,
        session: Session,
        job_record: JobTable,
        items: List[Tuple[str, List[str]]],
        *,
        name_to_id: Dict[str, str] | None = None,
    ) -> None:
        """
        Wire 'next' links for a list of (source_id, next_names).
        Raises PersistLinkageError if any target name is unknown.
        """
        mapping = name_to_id or self._name_id_map(session, job_record)
        id_to_name = {v: k for k, v in mapping.items()}

        for src_id, next_names in items:
            for nxt in next_names:
                dst_id = mapping.get(nxt)
                if dst_id is None:
                    src_name = id_to_name.get(src_id, "<unknown>")
                    raise PersistLinkageError(
                        f"Component '{src_name}' references unknown next '{nxt}'"
                    )

                session.add(
                    ComponentNextLink(
                        component_id=src_id,
                        next_id=dst_id,
                    )
                )

    def create_component_entry(
        self,
        session: Session,
        job_record: JobTable,
        cfg: Component,
    ) -> ComponentTable:
        """
        Insert a single component row and wire its next links.
        Always strict: missing targets raise PersistLinkageError.
        """
        ct = self._insert_component_row(session, job_record, cfg)
        session.flush()  # ensure ct.id

        self._wire_links(
            session,
            job_record,
            items=[(ct.id, cfg.next)],
            name_to_id=None,
        )
        return ct

    def create_all_from_configs(
        self,
        session: Session,
        job_record: JobTable,
        component_cfgs: List[Component],
    ) -> Dict[str, str]:
        """
        Insert all components for the job, then wire links in one pass.
        Always strict: missing targets raise PersistLinkageError.
        """
        name_to_id: Dict[str, str] = {}
        for cfg in component_cfgs:
            ct = self._insert_component_row(session, job_record, cfg)
            session.flush()
            name_to_id[cfg.name] = ct.id

        items: List[Tuple[str, List[str]]] = [
            (name_to_id[cfg.name], cfg.next) for cfg in component_cfgs
        ]

        self._wire_links(
            session,
            job_record,
            items=items,
            name_to_id=name_to_id,
        )
        return name_to_id

    def replace_all_from_configs(
        self,
        session: Session,
        job_record: JobTable,
        component_cfgs: List[Component],
    ) -> Dict[str, str]:
        self.delete_all(session, job_record)
        session.flush()
        session.expire(job_record, ["components"])
        return self.create_all_from_configs(session, job_record, component_cfgs)

    def delete_all(self, session: Session, job_record: JobTable) -> None:
        comp_rows: List[ComponentTable] = list(job_record.components or [])
        if not comp_rows:
            return

        comp_ids = [c.id for c in comp_rows]

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

        layouts = [c.layout for c in comp_rows if c.layout is not None]
        metas = [
            c.metadata_ for c in comp_rows if getattr(c, "metadata_", None) is not None
        ]

        with session.no_autoflush:
            for comp in comp_rows:
                session.delete(comp)
        session.flush()

        for row in layouts:
            session.delete(row)
        for row in metas:
            session.delete(row)
        session.flush()

        if getattr(job_record, "components", None) is not None:
            job_record.components.clear()
        session.flush()
        session.expire(job_record, ["components"])

    def build_runtime_for_all(self, job_record) -> List[Component]:
        runtime_comps: List = []
        for ct in job_record.components:
            base = {
                "id": ct.id,
                "name": ct.name,
                "description": ct.description,
                "comp_type": ct.comp_type,
                "next": [n.name for n in ct.next_components],
                "layout": self.dc.dump_layout(ct.layout),
                "metadata": self.dc.dump_metadata(ct.metadata_),
            }
            data = {**ct.payload, **base}

            cls = component_registry[ct.comp_type]
            obj = cls(**data)
            object.__setattr__(obj, "_id", ct.id)
            runtime_comps.append(obj)

        name_map = {c.name: c for c in runtime_comps}

        for comp in runtime_comps:
            try:
                next_objs = [name_map[n] for n in comp.next]
            except KeyError as exc:
                raise PersistLinkageError(
                    f"Unknown next-component name: {exc.args[0]!r}"
                ) from exc
            comp.next_components = next_objs
            for nxt in next_objs:
                nxt.prev_components.append(comp)

        return runtime_comps

    # ---------------------------
    # Internals
    # ---------------------------

    def _insert_component_row(
        self,
        session: Session,
        job_record: JobTable,
        cfg: Component,
    ) -> ComponentTable:
        layout = self.dc.create_layout_entry(session, cfg.layout.model_dump())
        meta = self.dc.create_metadata_entry(session, cfg.metadata_.model_dump())

        raw: Dict[str, Any] = cfg.model_dump(by_alias=False, exclude_none=True)
        base_fields, payload_fields = _split_base_and_payload(raw)

        ct = ComponentTable(
            name=base_fields["name"],
            description=base_fields.get("description", ""),
            comp_type=base_fields["comp_type"],
            job=job_record,
            layout=layout,
            metadata_=meta,
            payload=payload_fields,
        )
        session.add(ct)
        return ct
