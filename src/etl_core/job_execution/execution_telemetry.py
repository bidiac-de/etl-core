from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re
import threading
from typing import Any, Dict, Optional


_PASSWORD_PATTERNS = [
    re.compile(r"(?i)(password\s*[=:]\s*)([^,\s;]+)"),
    re.compile(r"(?i)(pwd\s*[=:]\s*)([^,\s;]+)"),
    re.compile(r"(?i)(secret\s*[=:]\s*)([^,\s;]+)"),
]


def _redact_line(text: str) -> str:
    redacted = text
    for pattern in _PASSWORD_PATTERNS:
        redacted = pattern.sub(r"\1***", redacted)
    return redacted


@dataclass
class ComponentTelemetry:
    component_id: str
    component_name: str
    status: str = "PENDING"
    rows_received: int = 0
    rows_forwarded: int = 0
    error_count: int = 0
    last_event: Optional[str] = None
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class ExecutionTelemetry:
    execution_id: str
    job_id: str
    job_name: str
    environment: Optional[str]
    status: str = "RUNNING"
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None
    active_attempt: int = 0
    last_error: Optional[str] = None
    rows_received_total: int = 0
    rows_forwarded_total: int = 0
    log_path: Optional[str] = None
    updated_at: datetime = field(default_factory=datetime.now)
    components: Dict[str, ComponentTelemetry] = field(default_factory=dict)

    def recalc_totals(self) -> None:
        self.rows_received_total = sum(c.rows_received for c in self.components.values())
        self.rows_forwarded_total = sum(c.rows_forwarded for c in self.components.values())
        self.updated_at = datetime.now()


class ExecutionTelemetryStore:
    def __init__(self, *, max_entries: int = 200) -> None:
        self._max_entries = max_entries
        self._lock = threading.RLock()
        self._order: list[str] = []
        self._data: Dict[str, ExecutionTelemetry] = {}

    def _prune_locked(self) -> None:
        while len(self._order) > self._max_entries:
            old_id = self._order.pop(0)
            self._data.pop(old_id, None)

    def start_execution(
        self,
        *,
        execution_id: str,
        job_id: str,
        job_name: str,
        environment: Optional[str],
        components: list[tuple[str, str]],
    ) -> None:
        with self._lock:
            telemetry = ExecutionTelemetry(
                execution_id=execution_id,
                job_id=job_id,
                job_name=job_name,
                environment=environment,
            )
            for component_id, component_name in components:
                telemetry.components[component_id] = ComponentTelemetry(
                    component_id=component_id,
                    component_name=component_name,
                )
            telemetry.recalc_totals()
            self._data[execution_id] = telemetry
            self._order.append(execution_id)
            self._prune_locked()

    def set_log_path(self, execution_id: str, log_path: Optional[str]) -> None:
        with self._lock:
            telemetry = self._data.get(execution_id)
            if telemetry is None:
                return
            telemetry.log_path = log_path
            telemetry.updated_at = datetime.now()

    def set_active_attempt(self, execution_id: str, attempt_index: int) -> None:
        with self._lock:
            telemetry = self._data.get(execution_id)
            if telemetry is None:
                return
            telemetry.active_attempt = attempt_index
            telemetry.updated_at = datetime.now()

    def update_component(
        self,
        execution_id: str,
        *,
        component_id: str,
        status: Optional[str] = None,
        rows_received: Optional[int] = None,
        rows_forwarded: Optional[int] = None,
        error_count: Optional[int] = None,
        last_event: Optional[str] = None,
    ) -> None:
        with self._lock:
            telemetry = self._data.get(execution_id)
            if telemetry is None:
                return
            component = telemetry.components.get(component_id)
            if component is None:
                return
            if status is not None:
                component.status = status
            if rows_received is not None:
                component.rows_received = max(int(rows_received), 0)
            if rows_forwarded is not None:
                component.rows_forwarded = max(int(rows_forwarded), 0)
            if error_count is not None:
                component.error_count = max(int(error_count), 0)
            if last_event is not None:
                component.last_event = last_event
            component.updated_at = datetime.now()
            telemetry.recalc_totals()

    def set_status(
        self,
        execution_id: str,
        *,
        status: str,
        error: Optional[str] = None,
        finished: bool = False,
    ) -> None:
        with self._lock:
            telemetry = self._data.get(execution_id)
            if telemetry is None:
                return
            telemetry.status = status
            telemetry.last_error = error
            if finished:
                telemetry.finished_at = datetime.now()
            telemetry.updated_at = datetime.now()

    def snapshot(self, execution_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            telemetry = self._data.get(execution_id)
            if telemetry is None:
                return None
            components = []
            for component in telemetry.components.values():
                components.append(
                    {
                        "component_id": component.component_id,
                        "component_name": component.component_name,
                        "status": component.status,
                        "rows_received": component.rows_received,
                        "rows_forwarded": component.rows_forwarded,
                        "error_count": component.error_count,
                        "last_event": component.last_event,
                        "updated_at": component.updated_at,
                    }
                )
            return {
                "execution_id": telemetry.execution_id,
                "job_id": telemetry.job_id,
                "job_name": telemetry.job_name,
                "environment": telemetry.environment,
                "status": telemetry.status,
                "started_at": telemetry.started_at,
                "finished_at": telemetry.finished_at,
                "active_attempt": telemetry.active_attempt,
                "last_error": telemetry.last_error,
                "rows_received_total": telemetry.rows_received_total,
                "rows_forwarded_total": telemetry.rows_forwarded_total,
                "log_path": telemetry.log_path,
                "updated_at": telemetry.updated_at,
                "components": components,
            }

    def tail_logs(self, execution_id: str, *, tail: int = 200) -> Dict[str, Any]:
        safe_tail = max(1, min(int(tail), 2000))
        with self._lock:
            telemetry = self._data.get(execution_id)
            if telemetry is None:
                return {"lines": [], "log_path": None, "redacted": True}
            log_path_raw = telemetry.log_path

        if not log_path_raw:
            return {"lines": [], "log_path": None, "redacted": True}

        log_path = Path(log_path_raw)
        if not log_path.exists() or not log_path.is_file():
            return {"lines": [], "log_path": str(log_path), "redacted": True}

        lines: deque[str] = deque(maxlen=safe_tail)
        with log_path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                lines.append(_redact_line(line.rstrip("\n")))
        return {"lines": list(lines), "log_path": str(log_path), "redacted": True}


_telemetry_lock = threading.Lock()
_telemetry_singleton: Optional[ExecutionTelemetryStore] = None


def execution_telemetry_store() -> ExecutionTelemetryStore:
    global _telemetry_singleton
    if _telemetry_singleton is None:
        with _telemetry_lock:
            if _telemetry_singleton is None:
                _telemetry_singleton = ExecutionTelemetryStore()
    return _telemetry_singleton

