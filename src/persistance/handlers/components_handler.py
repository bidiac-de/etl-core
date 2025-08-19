from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from sqlmodel import Session, select

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
    data: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
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
                session.add(ComponentNextLink(component_id=src_id, next_id=dst_id))

    def _insert_component_row(
        self,
        session: Session,
        job_record: JobTable,
        cfg: Component,
    ) -> ComponentTable:
        raw: Dict[str, Any] = cfg.model_dump(by_alias=False, exclude_none=True)
        base_fields, payload_fields = _split_base_and_payload(raw)

        ct = ComponentTable(
            name=base_fields["name"],
            description=base_fields.get("description", ""),
            comp_type=base_fields["comp_type"],
            job=job_record,
            payload=payload_fields,
        )
        session.add(ct)
        session.flush()

        self.dc.create_layout_for_component(session, ct, base_fields.get("layout", {}))
        self.dc.create_metadata_for_component(
            session, ct, base_fields.get("metadata_", {})
        )
        return ct

    def create_component_entry(
        self, session: Session, job_record: JobTable, cfg: Component
    ) -> ComponentTable:
        ct = self._insert_component_row(session, job_record, cfg)
        session.flush()
        self._wire_links(session, job_record, items=[(ct.id, cfg.next)])
        return ct

    def create_all_from_configs(
        self, session: Session, job_record: JobTable, component_cfgs: List[Component]
    ) -> Dict[str, str]:
        name_to_id: Dict[str, str] = {}
        for cfg in component_cfgs:
            ct = self._insert_component_row(session, job_record, cfg)
            session.flush()
            name_to_id[cfg.name] = ct.id

        items: List[Tuple[str, List[str]]] = [
            (name_to_id[cfg.name], cfg.next) for cfg in component_cfgs
        ]
        self._wire_links(session, job_record, items=items, name_to_id=name_to_id)
        return name_to_id

    def replace_all_from_configs(
        self, session: Session, job_record: JobTable, component_cfgs: List[Component]
    ) -> Dict[str, str]:
        self.delete_all(session, job_record)
        session.flush()
        session.expire(job_record, ["components"])
        return self.create_all_from_configs(session, job_record, component_cfgs)

    def delete_all(self, session: Session, job_record: JobTable) -> None:
        if not job_record.components:
            return
        with session.no_autoflush:
            job_record.components.clear()  # delete-orphan
        session.flush()
        session.expire(job_record, ["components"])

    def build_runtime_for_all(self, job_record: JobTable) -> List[Component]:
        comps: List[Component] = []
        for ct in job_record.components:
            base = {
                "id": ct.id,
                "name": ct.name,
                "description": ct.description,
                "comp_type": ct.comp_type,
                "next": [n.name for n in ct.next_components],
                "layout": self.dc.dump_layout(ct.layout) if ct.layout else {},
                "metadata": self.dc.dump_metadata(ct.metadata_) if ct.metadata_ else {},
            }
            data = {**ct.payload, **base}
            cls = component_registry[ct.comp_type]
            obj = cls(**data)
            object.__setattr__(obj, "_id", ct.id)
            comps.append(obj)

        name_map = {c.name: c for c in comps}
        for comp in comps:
            next_objs = [name_map[n] for n in comp.next]
            comp.next_components = next_objs
            for nxt in next_objs:
                nxt.prev_components.append(comp)
        return comps
