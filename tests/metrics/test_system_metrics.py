from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

import etl_core.metrics.system_metrics as S


def _patch_collect(
    monkeypatch: pytest.MonkeyPatch,
    cpu: float,
    mem: float,
    disk_used: int,
    disk_total: int,
) -> None:
    monkeypatch.setattr(S.psutil, "cpu_percent", lambda interval=1: cpu)
    monkeypatch.setattr(
        S.psutil, "virtual_memory", lambda: SimpleNamespace(percent=mem)
    )
    monkeypatch.setattr(
        S.shutil,
        "disk_usage",
        lambda path: (disk_total, disk_used, disk_total - disk_used),
    )


def test_system_metrics_properties_and_repr() -> None:
    ts = datetime(2025, 1, 1, 12, 0, 0)
    m = S.SystemMetrics(
        timestamp=ts, cpu_usage=10.0, memory_usage=20.0, disk_usage=30.0
    )

    assert isinstance(m.id, str) and len(m.id) >= 32
    assert m.timestamp is ts
    assert m.cpu_usage == 10.0
    assert m.memory_usage == 20.0
    assert m.disk_usage == 30.0

    r = repr(m)
    assert "SystemMetrics(" in r
    assert "cpu_usage=10.00%" in r
    assert "memory_usage=20.00%" in r
    assert "disk_usage=30.00%" in r


@pytest.mark.parametrize(
    ("value", "ok"),
    [(datetime(2025, 1, 2, 0, 0, 0), True), ("not-a-dt", False)],
)
def test_timestamp_setter_validation(value: Any, ok: bool) -> None:
    m = S.SystemMetrics(datetime(2025, 1, 1, 0, 0, 0), 1.0, 1.0, 1.0)
    if ok:
        m.timestamp = value  # type: ignore[assignment]
        assert m.timestamp is value
    else:
        with pytest.raises(ValueError):
            m.timestamp = value  # type: ignore[assignment]


@pytest.mark.parametrize(
    ("setter", "good", "bad_type", "bad_low", "bad_high"),
    [
        ("cpu_usage", 50.0, "x", -1.0, 101.0),
        ("memory_usage", 40.0, None, -0.1, 100.1),
        ("disk_usage", 60.0, object(), -5.0, 1000.0),
    ],
)
def test_numeric_setters_validation(
    setter: str,
    good: float,
    bad_type: Any,
    bad_low: float,
    bad_high: float,
) -> None:
    m = S.SystemMetrics(datetime(2025, 1, 1, 0, 0, 0), 1.0, 1.0, 1.0)

    with pytest.raises(ValueError):
        setattr(m, setter, bad_type)

    with pytest.raises(ValueError):
        setattr(m, setter, bad_low)

    with pytest.raises(ValueError):
        setattr(m, setter, bad_high)

    setattr(m, setter, good)
    assert getattr(m, setter) == good


def test_handler_captures_initial_and_new_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_collect(monkeypatch, cpu=12.34, mem=45.67, disk_used=55, disk_total=100)
    h = S.SystemMetricsHandler()
    first = h.system_metrics[0]
    assert isinstance(first, S.SystemMetrics)
    assert first.cpu_usage == 12.34
    assert first.memory_usage == 45.67
    assert round(first.disk_usage, 2) == 55.0

    _patch_collect(monkeypatch, cpu=1.0, mem=2.0, disk_used=1, disk_total=2)
    new = h.new_metrics_entry()
    assert new is h.system_metrics[-1]
    assert new.cpu_usage == 1.0
    assert new.memory_usage == 2.0
    assert round(new.disk_usage, 2) == 50.0

    latest = h.get_latest_system_metrics()
    assert latest is new


def test_handler_get_latest_none_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_collect(monkeypatch, cpu=0.0, mem=0.0, disk_used=0, disk_total=1)
    h = S.SystemMetricsHandler()
    h.system_metrics.clear()
    assert h.get_latest_system_metrics() is None
