# tests/components/files/test_csv_job_topology.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.etl_core.components.file_components.csv.csv_component import Delimiter
from src.etl_core.components.runtime_state import RuntimeState
from src.etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import get_component_by_name, runtime_job_from_config


def _make_input_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
            {"id": "3", "name": "Charlie"},
        ]
    ).to_csv(path, index=False)


@pytest.mark.asyncio
async def test_csv_fan_out_bulk(tmp_path: Path) -> None:
    """
    Fan-out Topologie:
        read_csv(bulk) --> [write1(bulk), write2(bulk)]
    Erwartung:
      - Job SUCCESS
      - beide Targets enthalten die gleichen Daten
      - Metrics pro Writer: lines_forwarded == 3
    """
    inp = tmp_path / "in.csv"
    out1 = tmp_path / "out1.csv"
    out2 = tmp_path / "out2.csv"
    _make_input_csv(inp)

    handler = JobExecutionHandler()
    config = {
        "name": "CSV_FanOut_Bulk",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {"user_id": 1, "timestamp": datetime.now()},
        "components": [
            {
                "name": "read",
                "comp_type": "read_csv",
                "description": "read csv bulk",
                "strategy": "bulk",
                "next": ["write1", "write2"],
                "filepath": inp,
                "separator": Delimiter.COMMA,
                "metadata": {"user_id": 1, "timestamp": datetime.now()},
            },
            {
                "name": "write1",
                "comp_type": "write_csv",
                "description": "write csv bulk #1",
                "strategy": "bulk",
                "next": [],
                "filepath": out1,
                "separator": Delimiter.COMMA,
                "metadata": {"user_id": 1, "timestamp": datetime.now()},
            },
            {
                "name": "write2",
                "comp_type": "write_csv",
                "description": "write csv bulk #2",
                "strategy": "bulk",
                "next": [],
                "filepath": out2,
                "separator": Delimiter.COMMA,
                "metadata": {"user_id": 1, "timestamp": datetime.now()},
            },
        ],
    }

    rj = runtime_job_from_config(config)
    execution = handler.execute_job(rj)
    attempt = execution.attempts[0]
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    read = get_component_by_name(rj, "read")
    w1 = get_component_by_name(rj, "write1")
    w2 = get_component_by_name(rj, "write2")

    m_read = mh.get_comp_metrics(execution.id, attempt.id, read.id)
    m_w1 = mh.get_comp_metrics(execution.id, attempt.id, w1.id)
    m_w2 = mh.get_comp_metrics(execution.id, attempt.id, w2.id)

    assert m_read.status == RuntimeState.SUCCESS
    assert m_read.lines_received == 3
    assert m_w1.status == RuntimeState.SUCCESS
    assert m_w1.lines_forwarded == 3
    assert m_w2.status == RuntimeState.SUCCESS
    assert m_w2.lines_forwarded == 3

    for out in (out1, out2):
        df = pd.read_csv(out, dtype=str)
        assert list(df.columns) == ["id", "name"]
        assert list(df["id"]) == ["1", "2", "3"]
        assert list(df["name"]) == ["Alice", "Bob", "Charlie"]


@pytest.mark.asyncio
async def test_csv_skip_children_when_read_fails(tmp_path: Path) -> None:
    """
    Skip-/Cancel-Logik: wenn read_csv fehlschlägt
    (fehlender Input), werden alle Child-Komponenten CANCELLED.
    """
    missing = tmp_path / "not_there.csv"
    out1 = tmp_path / "out1.csv"
    out2 = tmp_path / "out2.csv"

    handler = JobExecutionHandler()
    config = {
        "name": "CSV_Skip_Children_On_Fail",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {"user_id": 1, "timestamp": datetime.now()},
        "components": [
            {
                "name": "read",
                "comp_type": "read_csv",
                "description": "read csv bulk (will fail)",
                "strategy": "bulk",
                "next": ["write1", "write2"],
                "filepath": missing,  # existiert nicht -> FAIL
                "separator": Delimiter.COMMA,
                "metadata": {"user_id": 1, "timestamp": datetime.now()},
            },
            {
                "name": "write1",
                "comp_type": "write_csv",
                "description": "write csv bulk #1",
                "strategy": "bulk",
                "next": [],
                "filepath": out1,
                "separator": Delimiter.COMMA,
                "metadata": {"user_id": 1, "timestamp": datetime.now()},
            },
            {
                "name": "write2",
                "comp_type": "write_csv",
                "description": "write csv bulk #2",
                "strategy": "bulk",
                "next": [],
                "filepath": out2,
                "separator": Delimiter.COMMA,
                "metadata": {"user_id": 1, "timestamp": datetime.now()},
            },
        ],
    }

    rj = runtime_job_from_config(config)
    execution = handler.execute_job(rj)
    attempt = execution.attempts[0]
    mh = handler.job_info.metrics_handler

    read = get_component_by_name(rj, "read")
    w1 = get_component_by_name(rj, "write1")
    w2 = get_component_by_name(rj, "write2")

    m_read = mh.get_comp_metrics(execution.id, attempt.id, read.id)
    m_w1 = mh.get_comp_metrics(execution.id, attempt.id, w1.id)
    m_w2 = mh.get_comp_metrics(execution.id, attempt.id, w2.id)

    # Job schlägt fehl, Root FAIL -> Kinder CANCELLED
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert m_read.status == RuntimeState.FAILED
    assert m_w1.status == RuntimeState.CANCELLED
    assert m_w2.status == RuntimeState.CANCELLED


@pytest.mark.asyncio
async def test_csv_linear_row_pipeline(tmp_path: Path) -> None:
    """
    Lineare Pipeline (Row-Strategie):
        read_csv(row) -> write_csv(row)
    """
    inp = tmp_path / "in.csv"
    out = tmp_path / "out_row.csv"
    _make_input_csv(inp)

    handler = JobExecutionHandler()
    config = {
        "name": "CSV_Linear_Row",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {"user_id": 1, "timestamp": datetime.now()},
        "components": [
            {
                "name": "read",
                "comp_type": "read_csv",
                "description": "read csv row",
                "strategy": "row",
                "next": ["write"],
                "filepath": inp,
                "separator": Delimiter.COMMA,
                "metadata": {"user_id": 1, "timestamp": datetime.now()},
            },
            {
                "name": "write",
                "comp_type": "write_csv",
                "description": "write csv row",
                "strategy": "row",
                "next": [],
                "filepath": out,
                "separator": Delimiter.COMMA,
                "metadata": {"user_id": 1, "timestamp": datetime.now()},
            },
        ],
    }

    rj = runtime_job_from_config(config)
    execution = handler.execute_job(rj)
    attempt = execution.attempts[0]
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    read = get_component_by_name(rj, "read")
    write = get_component_by_name(rj, "write")

    m_read = mh.get_comp_metrics(execution.id, attempt.id, read.id)
    m_write = mh.get_comp_metrics(execution.id, attempt.id, write.id)

    assert m_read.lines_received == 3
    assert m_write.lines_forwarded == 3
    assert m_read.status == RuntimeState.SUCCESS
    assert m_write.status == RuntimeState.SUCCESS

    content = out.read_text().splitlines()
    assert content[0] == "id,name"
    assert content[1] == "1,Alice"
    assert content[2] == "2,Bob"
    assert content[3] == "3,Charlie"