from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlmodel import Session, select

from etl_core.persistence.db import engine, ensure_schema
from etl_core.persistence.table_definitions import ScheduleTable, TriggerType


class ScheduleNotFoundError(Exception):
    pass


class ScheduleHandler:
    def __init__(self, engine_=engine) -> None:
        ensure_schema()
        self.engine = engine_

    @contextmanager
    def _session(self) -> Session:
        with Session(self.engine) as s:
            yield s

    # CRUD
    def create(
        self,
        *,
        name: str,
        job_id: str,
        context: str,
        trigger_type: TriggerType,
        trigger_args: Dict[str, Any],
        is_paused: bool = False,
    ) -> ScheduleTable:
        with self._session() as s:
            row = ScheduleTable(
                name=name,
                job_id=job_id,
                context=context,
                trigger_type=trigger_type,
                trigger_args=dict(trigger_args or {}),
                is_paused=is_paused,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            s.add(row)
            s.commit()
            s.refresh(row)
            return row

    def update(
        self,
        schedule_id: str,
        *,
        name: Optional[str] = None,
        job_id: Optional[str] = None,
        context: Optional[str] = None,
        trigger_type: Optional[TriggerType] = None,
        trigger_args: Optional[Dict[str, Any]] = None,
        is_paused: Optional[bool] = None,
    ) -> ScheduleTable:
        with self._session() as s:
            row = s.get(ScheduleTable, schedule_id)
            if row is None:
                raise ScheduleNotFoundError(schedule_id)
            if name is not None:
                row.name = name
            if job_id is not None:
                row.job_id = job_id
            if context is not None:
                row.context = context
            if trigger_type is not None:
                row.trigger_type = trigger_type
            if trigger_args is not None:
                row.trigger_args = dict(trigger_args)
            if is_paused is not None:
                row.is_paused = is_paused
            row.updated_at = datetime.utcnow()
            s.add(row)
            s.commit()
            s.refresh(row)
            return row

    def list(self) -> List[ScheduleTable]:
        with self._session() as s:
            return list(s.exec(select(ScheduleTable)).all())

    def get(self, schedule_id: str) -> Optional[ScheduleTable]:
        with self._session() as s:
            return s.get(ScheduleTable, schedule_id)

    def delete(self, schedule_id: str) -> None:
        with self._session() as s:
            row = s.get(ScheduleTable, schedule_id)
            if row is None:
                raise ScheduleNotFoundError(schedule_id)
            s.delete(row)
            s.commit()

    def set_paused(self, schedule_id: str, paused: bool) -> ScheduleTable:
        return self.update(schedule_id, is_paused=paused)

    # utilities
    def get_by_name(self, name: str) -> Optional[ScheduleTable]:
        with self._session() as s:
            stmt = select(ScheduleTable).where(ScheduleTable.name == name)
            return s.exec(stmt).first()
