from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET

import pytest

import etl_core.components.file_components.xml.read_xml  # noqa: F401
import etl_core.components.file_components.xml.write_xml  # noqa: F401

from etl_core.components.runtime_state import RuntimeState
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import runtime_job_from_config
from tests.config_helpers import render_job_cfg_with_filepaths


ROW_JOB = Path(__file__).parent / "job_config_xml_row.json"
BULK_JOB = Path(__file__).parent / "job_config_xml_bulk.json"
BIG_JOB = Path(__file__).parent / "job_config_xml_bigdata.json"


@pytest.fixture()
def exec_handler() -> JobExecutionHandler:
    return JobExecutionHandler()


def _write_xml_rows(
    path: Path, rows: list[dict[str, str]], root_tag="rows", record_tag="row"
) -> None:
    root = ET.Element(root_tag)
    for r in rows:
        rec_el = ET.SubElement(root, record_tag)
        for k, v in r.items():
            child = ET.SubElement(rec_el, k)
            child.text = str(v)
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def _read_xml_rows(path: Path, record_tag="row") -> list[dict[str, str]]:
    tree = ET.parse(path)
    root = tree.getroot()
    out: list[dict[str, str]] = []
    for el in root.findall(f"./{record_tag}"):
        row = {child.tag: (child.text or "") for child in el}
        out.append(row)
    return out


def test_execute_xml_row_job(tmp_path: Path, exec_handler: JobExecutionHandler) -> None:
    in_fp = tmp_path / "in.xml"
    out_fp = tmp_path / "out.xml"
    _write_xml_rows(in_fp, [{"id": "1", "name": "Nina"}])

    cfg = render_job_cfg_with_filepaths(
        ROW_JOB, {"read_xml": in_fp, "write_xml": out_fp}
    )
    job = runtime_job_from_config(cfg)

    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    rows = _read_xml_rows(out_fp)
    assert rows == [{"id": "1", "name": "Nina"}]


def test_execute_xml_bulk_job(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    in_fp = tmp_path / "in_bulk.xml"
    out_fp = tmp_path / "out_bulk.xml"
    _write_xml_rows(in_fp, [{"id": "2", "name": "Omar"}, {"id": "3", "name": "Lina"}])

    cfg = render_job_cfg_with_filepaths(
        BULK_JOB, {"read_xml": in_fp, "write_xml": out_fp}
    )
    job = runtime_job_from_config(cfg)

    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    rows = sorted(_read_xml_rows(out_fp), key=lambda r: r["id"])
    assert [r["name"] for r in rows] == ["Omar", "Lina"]


def test_execute_xml_bigdata_job(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    in_fp = tmp_path / "in_big.xml"
    out_fp = tmp_path / "out_big.xml"
    _write_xml_rows(in_fp, [{"id": "4", "name": "Max"}, {"id": "5", "name": "Gina"}])

    cfg = render_job_cfg_with_filepaths(
        BIG_JOB, {"read_xml": in_fp, "write_xml": out_fp}
    )
    job = runtime_job_from_config(cfg)

    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    rows = sorted(_read_xml_rows(out_fp), key=lambda r: r["id"])
    assert [r["name"] for r in rows] == ["Max", "Gina"]
