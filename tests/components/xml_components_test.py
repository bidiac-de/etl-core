# tests/components/test_xml_components_execute.py
import json
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pytest

from src.components.file_components.xml.read_xml_component import ReadXML
from src.components.file_components.xml.write_xml_component import WriteXML
from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics import ComponentMetrics
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy

@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    def row_exec(self, component, inputs, **kwargs):
        return component.process_row(inputs, metrics=getattr(component, "metrics", None))
    def bulk_exec(self, component, inputs, **kwargs):
        return component.process_bulk(inputs, metrics=getattr(component, "metrics", None))
    monkeypatch.setattr(RowExecutionStrategy, "execute", row_exec, raising=True)
    monkeypatch.setattr(BulkExecutionStrategy, "execute", bulk_exec, raising=True)

@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(datetime.now(), timedelta(0), 0, 0, 0)

@pytest.fixture
def schema_definition():
    return [ColumnDefinition("id", DataType.INTEGER), ColumnDefinition("name", DataType.STRING)]

def test_readxml_execute_bulk(tmp_path: Path, schema_definition, metrics: ComponentMetrics):
    fp = tmp_path / "in.xml"
    fp.write_text("""<?xml version="1.0"?>
    <records>
      <record><id>1</id><name>Alice</name></record>
      <record><id>2</id><name>Bob</name></record>
    </records>""", encoding="utf-8")
    comp = ReadXML(name="Read XML", description="bulk", comp_type="read_xml",
                   strategy_type="bulk", filepath=fp, schema_definition=schema_definition)
    comp.metrics = metrics
    df = comp.execute(data=None, metrics=metrics)
    assert len(df) == 2

def test_writexml_execute_bulk_and_readback(tmp_path: Path, schema_definition, metrics: ComponentMetrics):
    out_fp = tmp_path / "out.xml"
    out_fp.touch()
    writer = WriteXML(name="Write XML", description="bulk", comp_type="write_xml",
                      strategy_type="bulk", filepath=out_fp, schema_definition=schema_definition)
    writer.metrics = metrics
    df = pd.DataFrame([{"id": 10, "name": "Charlie"}, {"id": 11, "name": "Diana"}])
    writer.execute(data=df, metrics=metrics)

    reader = ReadXML(name="ReadBack", description="bulk", comp_type="read_xml",
                     strategy_type="bulk", filepath=out_fp, schema_definition=schema_definition)
    reader.metrics = metrics
    read_df = reader.execute(data=None, metrics=metrics)
    assert list(read_df.sort_values("id")["name"]) == ["Charlie", "Diana"]
