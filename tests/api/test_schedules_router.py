from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from etl_core.api.routers import schedules as S


class FakeRow:
    def __init__(self, id_: str = "1", name: str = "n"):
        self.id = id_
        self.name = name
        self.job_id = "j"
        self.environment = "DEV"
        self.trigger_type = "cron"  # type: ignore[assignment]
        self.trigger_args = {"minute": "*/5"}
        self.is_paused = False


class FakeHandler:
    def __init__(self):
        self._rows = {"1": FakeRow("1")}

    def list(self):
        return list(self._rows.values())

    def get(self, schedule_id: str):
        return self._rows.get(schedule_id)


def test_list_and_get_schedule(monkeypatch):
    monkeypatch.setattr(S, "_schedule_handler_singleton", lambda: FakeHandler())

    out = S.list_schedules()
    assert out and out[0].id == "1"

    one = S.get_schedule("1")
    assert one.id == "1" and one.name == "n"

    with pytest.raises(HTTPException) as ei:
        S.get_schedule("missing")
    assert ei.value.status_code == 404


class FakeCmd:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def execute(self):
        return FakeRow("X")


def test_create_update_delete_pause_resume(monkeypatch):
    monkeypatch.setattr(S, "CreateScheduleCommand", lambda **kw: FakeCmd(**kw))
    monkeypatch.setattr(S, "UpdateScheduleCommand", lambda **kw: FakeCmd(**kw))
    monkeypatch.setattr(S, "GetScheduleCommand", lambda **kw: FakeCmd(**kw))
    monkeypatch.setattr(
        S,
        "DeleteScheduleCommand",
        lambda schedule_id: SimpleNamespace(execute=lambda: None),
    )
    monkeypatch.setattr(
        S, "PauseScheduleCommand", lambda schedule_id: FakeCmd(id=schedule_id)
    )
    monkeypatch.setattr(
        S, "ResumeScheduleCommand", lambda schedule_id: FakeCmd(id=schedule_id)
    )

    new_id = S.create_schedule(
        S.ScheduleIn(
            name="n",
            job_id="j",
            environment="DEV",
            trigger_type="cron",
            trigger_args={"minute": "*/5"},
        )
    )  # type: ignore[arg-type]
    assert new_id == "X"

    upd_id = S.update_schedule("Z", S.SchedulePatch(name="nn"))
    assert upd_id == "X"

    msg = S.delete_schedule("Z")
    assert "deleted" in msg["message"]

    paused = S.pause_schedule("Z")
    assert paused.id == "X"

    resumed = S.resume_schedule("Z")
    assert resumed.id == "X"


def test_create_schedule_invalid_trigger_args_returns_400():
    with pytest.raises(HTTPException) as ei:
        S.create_schedule(
            S.ScheduleIn(
                name="n",
                job_id="j",
                environment="DEV",
                trigger_type="cron",
                trigger_args={},
            )
        )  # type: ignore[arg-type]
    assert ei.value.status_code == 400
    assert ei.value.detail.get("code") == "SCHEDULE_INVALID_TRIGGER_ARGS"


def test_update_schedule_invalid_patch_returns_400(monkeypatch):
    monkeypatch.setattr(S, "GetScheduleCommand", lambda **kw: FakeCmd(**kw))
    monkeypatch.setattr(S, "UpdateScheduleCommand", lambda **kw: FakeCmd(**kw))

    with pytest.raises(HTTPException) as ei:
        S.update_schedule("Z", S.SchedulePatch(trigger_args={"unknown": "value"}))
    assert ei.value.status_code == 400
    assert ei.value.detail.get("code") == "SCHEDULE_INVALID_PATCH"
