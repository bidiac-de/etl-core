from pathlib import Path
from typing import Any, Dict, Generator

import pandas as pd
import xml.etree.ElementTree as ET

from metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver
from src.receivers.files.xml_helper import load_xml_records, dump_xml_records, elem_to_dict
import dask
import dask.dataframe as dd
from uuid import uuid4



class XMLReceiver(ReadFileReceiver, WriteFileReceiver):
    root_tag: str = "records"
    record_tag: str = "record"

    def read_row(self, filepath: Path, metrics: ComponentMetrics) -> Dict[str, Any]:
        record_tag = getattr(self, "record_tag", "record")
        records = load_xml_records(filepath, record_tag=record_tag)
        return records[0] if records else {}

    def read_bulk(self, filepath: Path, metrics: ComponentMetrics) -> pd.DataFrame:
        record_tag = getattr(self, "record_tag", "record")
        records = load_xml_records(filepath, record_tag=record_tag)
        return pd.DataFrame(records)

    def read_bigdata(self, filepath: Path, metrics: ComponentMetrics) -> Generator[Dict[str, Any], None, None]:
        """Streaming read via iterparse → yields record dicts (generator)."""
        record_tag = getattr(self, "record_tag", "record")
        context = ET.iterparse(str(filepath), events=("end",))
        for _, elem in context:
            if elem.tag == record_tag:
                yield elem_to_dict(elem)  # use helper
                elem.clear()  # free memory

    def write_row(self, row: Dict[str, Any], filepath: Path, metrics: ComponentMetrics):
        root_tag = getattr(self, "root_tag", "records")
        record_tag = getattr(self, "record_tag", "record")
        dump_xml_records(filepath, [row], root_tag=root_tag, record_tag=record_tag)

    def write_bulk(self, df: pd.DataFrame, filepath: Path, metrics: ComponentMetrics):
        root_tag = getattr(self, "root_tag", "records")
        record_tag = getattr(self, "record_tag", "record")
        records = df.to_dict(orient="records")
        dump_xml_records(filepath, records, root_tag=root_tag, record_tag=record_tag)

    def write_bigdata(self, ddf: dd.DataFrame, filepath: Path, metrics: ComponentMetrics):
        """Write a Dask DataFrame as partitioned XML files (folder with part-*.xml)."""
        filepath.mkdir(parents=True, exist_ok=True)

        root_tag = getattr(self, "root_tag", "records")
        record_tag = getattr(self, "record_tag", "record")

        def _write_partition(pdf: pd.DataFrame, out_dir: str, root_tag: str, record_tag: str) -> str:
            out_path = Path(out_dir) / f"part-{uuid4().hex}.xml"
            records = pdf.to_dict(orient="records")
            dump_xml_records(out_path, records, root_tag=root_tag, record_tag=record_tag)
            return str(out_path)

        delayed_tasks = [
            dask.delayed(_write_partition)(part.compute(), str(filepath), root_tag, record_tag)
            for part in ddf.to_delayed()
        ]
        dask.compute(*delayed_tasks)

