from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from etl_core.context.environment import Environment
from etl_core.persistence.table_definitions import TriggerType


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as e:
            raise ValueError("Invalid ISO datetime string") from e
    raise ValueError("Datetime must be datetime or ISO string")


def _ensure_allowed_keys(args: Dict[str, Any], allowed: Iterable[str]) -> None:
    allowed_set = set(allowed)
    unknown = [k for k in args.keys() if k not in allowed_set]
    if unknown:
        raise ValueError(f"Unsupported trigger_args keys: {sorted(unknown)}")


class ScheduleConfig(BaseModel):
    """
    Validated schedule configuration mirroring ScheduleTable fields, with
    trigger_args validated based on trigger_type.
    """

    name: str
    job_id: str
    context: str = Field(description="Execution environment gate: DEV/TEST/PROD")
    trigger_type: TriggerType
    trigger_args: Dict[str, Any] = Field(default_factory=dict)
    paused: bool = False

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    @field_validator("name", "job_id", mode="before")
    @classmethod
    def _non_empty(cls, v: Any) -> str:
        if not _is_non_empty_string(v):
            raise ValueError("must be a non-empty string")
        return str(v).strip()

    @field_validator("context", mode="before")
    @classmethod
    def _validate_context(cls, v: Any) -> str:
        if not _is_non_empty_string(v):
            raise ValueError("context must be a non-empty string")
        val = str(v).strip().upper()
        try:
            Environment(val)
        except Exception as e:
            valid = ", ".join([e.value for e in Environment]) if hasattr(Environment, "__iter__") else "DEV, TEST, PROD"
            raise ValueError(f"context must be one of DEV/TEST/PROD; got {val!r}") from e
        return val

    @staticmethod
    def _validate_interval(args: Dict[str, Any]) -> Dict[str, Any]:
        allowed = [
            "weeks",
            "days",
            "hours",
            "minutes",
            "seconds",
            "start_date",
            "end_date",
            "timezone",
            "jitter",
        ]
        _ensure_allowed_keys(args, allowed)
        # at least one duration key must be present
        if not any(k in args for k in ["weeks", "days", "hours", "minutes", "seconds"]):
            raise ValueError(
                "interval trigger requires one of weeks/days/hours/minutes/seconds"
            )
        for k in ["weeks", "days", "hours", "minutes", "seconds", "jitter"]:
            if k in args:
                v = args[k]
                if not isinstance(v, int) or v <= 0:
                    raise ValueError(f"{k} must be a positive integer")
        for k in ["start_date", "end_date"]:
            if k in args and args[k] is not None:
                args[k] = _parse_dt(args[k])
        return args

    @staticmethod
    def _validate_cron(args: Dict[str, Any]) -> Dict[str, Any]:
        allowed = [
            "year",
            "month",
            "day",
            "week",
            "day_of_week",
            "hour",
            "minute",
            "second",
            "start_date",
            "end_date",
            "timezone",
            "jitter",
        ]
        _ensure_allowed_keys(args, allowed)
        # should specify at least one time field
        if not any(k in args for k in [
            "year",
            "month",
            "day",
            "week",
            "day_of_week",
            "hour",
            "minute",
            "second",
        ]):
            raise ValueError("cron trigger requires at least one scheduling field")

        # Light type checks: allow int/str/list[int|str]
        def _ok(v: Any) -> bool:
            if isinstance(v, (int, str)):
                return True
            if isinstance(v, list) and all(isinstance(x, (int, str)) for x in v):
                return True
            return False

        for k, v in list(args.items()):
            if k in ("start_date", "end_date"):
                args[k] = _parse_dt(v)
                continue
            if not _ok(v):
                raise ValueError(
                    f"cron field {k!r} must be int/str or list[int|str], got {type(v)}"
                )
        return args

    @staticmethod
    def _validate_date(args: Dict[str, Any]) -> Dict[str, Any]:
        # allow alias 'date' -> 'run_date'
        if "date" in args and "run_date" not in args:
            args = dict(args)
            args["run_date"] = args.pop("date")
        allowed = ["run_date", "timezone"]
        _ensure_allowed_keys(args, allowed)
        if "run_date" not in args:
            raise ValueError("date trigger requires 'run_date'")
        args["run_date"] = _parse_dt(args["run_date"])
        return args

    @model_validator(mode="after")
    def _validate_trigger(self) -> "ScheduleConfig":
        ta = dict(self.trigger_args or {})
        if self.trigger_type == TriggerType.INTERVAL:
            self.trigger_args = self._validate_interval(ta)
        elif self.trigger_type == TriggerType.CRON:
            self.trigger_args = self._validate_cron(ta)
        elif self.trigger_type == TriggerType.DATE:
            self.trigger_args = self._validate_date(ta)
        else:  # pragma: no cover - enum guards this
            raise ValueError(f"Unsupported trigger type: {self.trigger_type}")
        return self


class SchedulePatchConfig(BaseModel):
    """
    Optional fields for update validation. If both trigger_type and
    trigger_args are provided, validate their combination. Otherwise,
    per-field checks only (full validation should merge with the
    existing row and use ScheduleConfig).
    """

    name: Optional[str] = None
    job_id: Optional[str] = None
    context: Optional[str] = None
    trigger_type: Optional[TriggerType] = None
    trigger_args: Optional[Dict[str, Any]] = None
    paused: Optional[bool] = None

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    @field_validator("name", "job_id", mode="before")
    @classmethod
    def _non_empty_opt(cls, v: Any) -> Any:
        if v is None:
            return v
        if not _is_non_empty_string(v):
            raise ValueError("must be a non-empty string")
        return str(v).strip()

    @field_validator("context", mode="before")
    @classmethod
    def _validate_context_opt(cls, v: Any) -> Any:
        if v is None:
            return v
        if not _is_non_empty_string(v):
            raise ValueError("context must be a non-empty string")
        val = str(v).strip().upper()
        try:
            Environment(val)
        except Exception as e:
            raise ValueError(f"context must be one of DEV/TEST/PROD; got {val!r}") from e
        return val

    @model_validator(mode="after")
    def _maybe_validate_trigger(self) -> "SchedulePatchConfig":
        if self.trigger_type is not None and self.trigger_args is not None:
            # validate the combination by constructing a minimal ScheduleConfig
            try:
                ScheduleConfig(
                    name=self.name or "_placeholder_",
                    job_id=self.job_id or "_placeholder_",
                    context=self.context or Environment.DEV.value,
                    trigger_type=self.trigger_type,
                    trigger_args=self.trigger_args,
                    paused=self.paused or False,
                )
            except ValidationError as e:  # re-raise as ValueError for uniformity
                raise ValueError(str(e))
        return self

