from __future__ import annotations

from datetime import datetime

import pytest

from etl_core.persistence.configs.schedule_config import (
    ScheduleConfig,
    SchedulePatchConfig,
)
from etl_core.persistence.table_definitions import TriggerType


@pytest.fixture(autouse=True)
def _disable_validate_assignment(monkeypatch: pytest.MonkeyPatch) -> None:
    # Prevent recursive re-validation loops during tests
    orig = ScheduleConfig.model_config
    new_cfg = {**orig}
    new_cfg["validate_assignment"] = False
    monkeypatch.setattr(ScheduleConfig, "model_config", new_cfg, raising=False)


def test_interval_parses_dates_and_validates_positive_integers() -> None:
    args = {
        "minutes": 5,
        "start_date": "2025-01-01T00:00:00",
        "end_date": "2025-01-02T00:00:00",
        "jitter": 1,
    }
    out = ScheduleConfig._validate_interval(dict(args))
    assert isinstance(out["start_date"], datetime)
    assert isinstance(out["end_date"], datetime)
    assert out["minutes"] == 5
    assert out["jitter"] == 1


def test_interval_requires_any_duration_key() -> None:
    with pytest.raises(ValueError):
        ScheduleConfig._validate_interval({"timezone": "UTC"})


def test_cron_accepts_list_and_parses_dates() -> None:
    args = {
        "day_of_week": [1, "fri"],
        "hour": 9,
        "start_date": "2025-01-01T10:00:00",
        "end_date": "2025-01-03T10:00:00",
    }
    out = ScheduleConfig._validate_cron(dict(args))
    assert isinstance(out["start_date"], datetime)
    assert isinstance(out["end_date"], datetime)
    assert out["day_of_week"] == [1, "fri"]
    assert out["hour"] == 9


def test_cron_invalid_list_element_type_raises() -> None:
    with pytest.raises(ValueError):
        ScheduleConfig._validate_cron({"minute": [0.5]})


def test_schedule_config_validate_trigger_interval() -> None:
    cfg = ScheduleConfig(
        name="n",
        job_id="j",
        context="DEV",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 10},
        paused=False,
    )
    assert cfg.trigger_args["seconds"] == 10


def test_schedule_config_validate_trigger_cron() -> None:
    cfg = ScheduleConfig(
        name="n",
        job_id="j",
        context="TEST",
        trigger_type=TriggerType.CRON,
        trigger_args={"minute": 0, "hour": 1},
        paused=False,
    )
    assert cfg.trigger_args["minute"] == 0
    assert cfg.trigger_args["hour"] == 1


def test_schedule_config_validate_trigger_date() -> None:
    cfg = ScheduleConfig(
        name="n",
        job_id="j",
        context="PROD",
        trigger_type=TriggerType.DATE,
        trigger_args={"run_date": "2025-02-01T12:00:00"},
        paused=True,
    )
    assert isinstance(cfg.trigger_args["run_date"], datetime)


def test_patch_validators_none_passthrough() -> None:
    assert SchedulePatchConfig._non_empty_opt(None) is None
    assert SchedulePatchConfig._validate_context_opt(None) is None


def test_patch_validate_trigger_combo_invalid_raises() -> None:
    with pytest.raises(ValueError):
        SchedulePatchConfig(
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"timezone": "UTC"},
        )


def test_patch_validate_trigger_combo_ok() -> None:
    patch = SchedulePatchConfig(
        trigger_type=TriggerType.CRON,
        trigger_args={"hour": 7, "minute": 0},
    )
    assert patch.trigger_type == TriggerType.CRON
    assert patch.trigger_args == {"hour": 7, "minute": 0}
