from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

from sqlmodel import Session

from etl_core.components.base_component import Component
from etl_core.components.component_registry import component_registry
from etl_core.components.wiring.ports import EdgeRef
from etl_core.persistence.handlers.dataclasses_handler import DataClassHandler
from etl_core.persistence.table_definitions import (
    ComponentTable,
    JobTable,
    ComponentLinkTable,
)

BASE_FIELDS: Set[str] = {
    "name",
    "description",
    "comp_type",
    "layout",
    "metadata_",
}

TRANSIENT_FIELDS: Set[str] = {"routes"}


def _split_base_and_payload(
    data: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Normalize aliases (metadata/layout) and split fields that live on ComponentTable
    vs. everything else that remains JSON payload, while dropping transient
    runtime fields.
    """
    norm = dict(data)
    if "metadata" in norm and "metadata_" not in norm:
        norm["metadata_"] = norm.pop("metadata")
    if "layout_" in norm and "layout" not in norm:
        norm["layout"] = norm.pop("layout_")

    base = {k: v for k, v in norm.items() if k in BASE_FIELDS}
    payload = {
        k: v
        for k, v in norm.items()
        if k not in BASE_FIELDS and k not in TRANSIENT_FIELDS
    }
    return base, payload


def _coerce_base_types(base: Dict[str, Any]) -> None:
    """
    Convert JSON-serialized base fields back to their Python types
    that the SQLModel/DB expects. Currently only handles metadata_.timestamp.
    """
    meta = base.get("metadata_")
    if isinstance(meta, dict):
        ts = meta.get("timestamp")
        if isinstance(ts, str):
            # Support both plain ISO 8601 and the 'Z' suffix
            iso = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
            try:
                meta["timestamp"] = datetime.fromisoformat(iso)
            except ValueError:
                # Leave as is, DB layer will surface clear error
                pass


def _to_edgeref(target: Any) -> EdgeRef:
    """Coerce a routes target into an EdgeRef."""
    if isinstance(target, EdgeRef):
        return target
    return EdgeRef(to=str(target))


def _resolve_dst_id(
    name_to_id: Dict[str, str],
    cfg_name: str,
    out_port: str,
    dst_name: str,
) -> str:
    """Resolve target component id or raise a helpful error."""
    try:
        return name_to_id[dst_name]
    except KeyError as exc:
        raise ValueError(
            f"Unknown target {dst_name!r} in routes[{cfg_name}][{out_port!r}]"
        ) from exc


def _iter_routes(cfg: Component) -> List[Tuple[str, List[EdgeRef]]]:
    """
    Yield (out_port, [EdgeRef,...]) pairs with normalization.
    Keep user order per port.
    """
    routes = cfg.routes or {}
    items: List[Tuple[str, List[EdgeRef]]] = []
    for out_port, targets in routes.items():
        refs = [_to_edgeref(t) for t in targets]
        items.append((out_port, refs))
    return items


def _add_link_row(
    session: Session,
    job_id: str,
    src_id: str,
    out_port: str,
    dst_id: str,
    in_port: str,
    position: int,
) -> None:
    """Create one ComponentLinkTable row."""
    link = ComponentLinkTable(
        job_id=job_id,
        src_component_id=src_id,
        src_out_port=out_port,
        dst_component_id=dst_id,
        dst_in_port=in_port,
        position=position,
    )
    session.add(link)


class ComponentHandler:
    """
    CRUD for ComponentTable; rebuild runtime components (routes-only).
    """

    def __init__(self, dc: DataClassHandler) -> None:
        self.dc = dc

    def _insert_component_row(
        self, session: Session, job_record: JobTable, cfg: Component
    ) -> ComponentTable:
        raw: Dict[str, Any] = cfg.model_dump(
            mode="json",
            by_alias=False,
            exclude_none=False,
            exclude_defaults=False,
        )

        base_fields, payload_fields = _split_base_and_payload(raw)

        # Convert types to what the DB layer expects(datetime, paths etc)
        _coerce_base_types(base_fields)

        ct = ComponentTable(
            name=base_fields["name"],
            description=base_fields.get("description", ""),
            comp_type=base_fields["comp_type"],
            job=job_record,
            payload=payload_fields,
        )
        session.add(ct)
        session.flush()

        layout_data = base_fields.get("layout")
        if isinstance(layout_data, dict):
            self.dc.create_layout_for_component(session, ct, layout_data)

        metadata_data = base_fields.get("metadata_")
        if isinstance(metadata_data, dict):
            self.dc.create_metadata_for_component(session, ct, metadata_data)

        return ct

    def _create_links(
        self,
        session: Session,
        job_record: JobTable,
        component_cfgs: List[Component],
        name_to_id: Dict[str, str],
    ) -> None:
        """
        Materialize cfg.routes into ComponentLinkTable rows.
        Delegates to helpers to keep complexity low and messages clear.
        """
        for cfg in component_cfgs:
            src_id = name_to_id[cfg.name]
            for out_port, refs in _iter_routes(cfg):
                for idx, ref in enumerate(refs):
                    dst_id = _resolve_dst_id(name_to_id, cfg.name, out_port, ref.to)
                    in_port = ref.in_port or ""
                    _add_link_row(
                        session=session,
                        job_id=job_record.id,
                        src_id=src_id,
                        out_port=out_port,
                        dst_id=dst_id,
                        in_port=in_port,
                        position=idx,
                    )
        session.flush()

    def create_component_entry(
        self, session: Session, job_record: JobTable, cfg: Component
    ) -> ComponentTable:
        ct = self._insert_component_row(session, job_record, cfg)
        session.flush()
        return ct

    def create_all_from_configs(
        self, session: Session, job_record: JobTable, component_cfgs: List[Component]
    ) -> Dict[str, str]:
        # Insert all components, collect ids
        name_to_id: Dict[str, str] = {}
        for cfg in component_cfgs:
            ct = self._insert_component_row(session, job_record, cfg)
            session.flush()
            name_to_id[cfg.name] = ct.id

        # Insert all links using resolved ids
        self._create_links(session, job_record, component_cfgs, name_to_id)
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
            # deleting components cascades
            job_record.components.clear()
        session.flush()
        session.expire(job_record, ["components"])

    def build_runtime_for_all(self, job_record: JobTable) -> List[Component]:
        """
        Rebuild Component objects from DB rows and re-hydrate
        Component.routes from link rows.
        Do NOT wire next/prev; RuntimeJob wires via `routes`.
        """
        comps: List[Component] = []
        # Build a map id --> name to write "to" by name in EdgeRef
        id_to_name: Dict[str, str] = {ct.id: ct.name for ct in job_record.components}

        for ct in job_record.components:
            base = {
                "id": ct.id,
                "name": ct.name,
                "description": ct.description,
                "comp_type": ct.comp_type,
                "layout": self.dc.dump_layout(ct.layout) if ct.layout else {},
                "metadata_": (
                    self.dc.dump_metadata(ct.metadata_) if ct.metadata_ else {}
                ),
            }
            data = {**ct.payload, **base}

            # rehydrate routes from outgoing_links
            routes: Dict[str, List[EdgeRef]] = {}
            for link in sorted(
                ct.outgoing_links or [],
                key=lambda lin: (lin.src_out_port, lin.position),
            ):
                to_name = id_to_name.get(link.dst_component_id)
                if not to_name:
                    # If dangling (shouldn't happen due to FK), skip safely
                    continue
                # empty string in DB means "unspecified" in config
                in_port = link.dst_in_port or None
                routes.setdefault(link.src_out_port, []).append(
                    EdgeRef(to=to_name, in_port=in_port)
                )

            if routes:
                data["routes"] = routes

            cls = component_registry[ct.comp_type]
            obj = cls(**data)
            object.__setattr__(obj, "_id", ct.id)
            comps.append(obj)
        return comps
