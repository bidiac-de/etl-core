# tests/receivers/test_xml_receiver.py
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pytest

from src.receivers.files.xml_receiver import XMLReceiver
from src.metrics.component_metrics import ComponentMetrics

@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(datetime.now(), timedelta(0), 0, 0, 0)

def test_xmlreceiver_row_roundtrip(tmp_path: Path, metrics: ComponentMetrics):
    r = XMLReceiver(root_tag="records", record_tag="record")
    fp = tmp_path / "row.xml"
    row_in = {"id": 1, "name": "Alice"}
    r.write_row(row_in, fp, metrics=metrics)
    row_out = r.read_row(fp, metrics=metrics)
    assert row_out["id"] == "1" or row_out["id"] == 1  # text vs int
    assert row_out["name"] == "Alice"

def test_xmlreceiver_bulk_roundtrip(tmp_path: Path, metrics: ComponentMetrics):
    r = XMLReceiver()
    fp = tmp_path / "bulk.xml"
    df_in = pd.DataFrame([{"id": 10, "name": "Bob"}, {"id": 11, "name": "Cara"}])
    r.write_bulk(df_in, fp, metrics=metrics)
    df_out = r.read_bulk(fp, metrics=metrics)
    assert len(df_out) == 2
    assert set(df_out["name"]) == {"Bob", "Cara"}

def test_xmlreceiver_bigdata_iterparse(tmp_path: Path, metrics: ComponentMetrics):
    r = XMLReceiver()
    fp = tmp_path / "stream.xml"
    fp.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
        <records>
          <record><id>100</id><name>Eve</name></record>
          <record><id>101</id><name>Frank</name></record>
        </records>
        """,
        encoding="utf-8",
    )
    gen = r.read_bigdata(fp, metrics=metrics)
    rows = list(gen)
    assert len(rows) == 2
    assert {x["name"] for x in rows} == {"Eve", "Frank"}
