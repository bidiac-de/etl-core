from __future__ import annotations

from typing import Any, Dict


def _serialize_datetime(value: Any, *, as_iso: bool) -> Any:
    if value is None:
        return None
    return value.isoformat() if as_iso else value


def serialize_execution_row(row: Any, *, as_iso: bool = False) -> Dict[str, Any]:
    return {
        "id": row.id,
        "job_id": row.job_id,
        "environment": row.environment,
        "status": row.status,
        "error": row.error,
        "started_at": _serialize_datetime(row.started_at, as_iso=as_iso),
        "finished_at": _serialize_datetime(row.finished_at, as_iso=as_iso),
    }


def serialize_attempt_row(row: Any, *, as_iso: bool = False) -> Dict[str, Any]:
    return {
        "id": row.id,
        "execution_id": row.execution_id,
        "attempt_index": row.attempt_index,
        "status": row.status,
        "error": row.error,
        "started_at": _serialize_datetime(row.started_at, as_iso=as_iso),
        "finished_at": _serialize_datetime(row.finished_at, as_iso=as_iso),
    }
