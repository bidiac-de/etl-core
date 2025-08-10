# src/receivers/files/xml_receiver.py
from pathlib import Path
from typing import Dict, Any, List, AsyncIterator
import asyncio
import xml.etree.ElementTree as ET

import pandas as pd
import dask.dataframe as dd

from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


class XMLReceiver(ReadFileReceiver, WriteFileReceiver):
    """Stateless XML receiver (pandas bulk; simple dask wrapper for bigdata)."""

    async def read_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            root_tag: str = "rows",
            record_tag: str = "row",
    ) -> AsyncIterator[Dict[str, Any]]:

        def _read_all() -> List[Dict[str, Any]]:
            root = ET.parse(filepath).getroot()
            return [
                {child.tag: (child.text or "") for child in rec}
                for rec in root.findall(record_tag)
            ]

        records = await asyncio.to_thread(_read_all)
        for rec in records:
            yield rec

    async def read_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            root_tag: str = "rows",
            record_tag: str = "row",
    ) -> pd.DataFrame:

        def _read_df() -> pd.DataFrame:
            root = ET.parse(filepath).getroot()
            rows = [
                {child.tag: (child.text or "") for child in rec}
                for rec in root.findall(record_tag)
            ]
            if not rows:
                return pd.DataFrame()

            all_keys: set[str] = set()
            for r in rows:
                all_keys.update(r.keys())
            return pd.DataFrame(rows, columns=list(all_keys))

        return await asyncio.to_thread(_read_df)

    async def read_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            root_tag: str = "rows",
            record_tag: str = "row",
            npartitions: int = 2,
    ) -> dd.DataFrame:
        pdf = await self.read_bulk(filepath, metrics, root_tag=root_tag, record_tag=record_tag)
        return dd.from_pandas(pdf, npartitions=max(1, npartitions))

    async def write_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            row: Dict[str, Any],
            root_tag: str = "rows",
            record_tag: str = "row",
            indent: int = 2,
    ):
        def _write():
            try:
                root = ET.parse(filepath).getroot()
            except FileNotFoundError:
                root = ET.Element(root_tag)
            except ET.ParseError:
                root = ET.Element(root_tag)

            rec = ET.SubElement(root, record_tag)
            for k, v in row.items():
                c = ET.SubElement(rec, str(k))
                c.text = "" if v is None else str(v)
            ET.ElementTree(root).write(filepath, encoding="utf-8", xml_declaration=True)

        await asyncio.to_thread(_write)

    async def write_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            data: List[Dict[str, Any]],
            root_tag: str = "rows",
            record_tag: str = "row",
            indent: int = 2,
    ):
        def _write():
            root = ET.Element(root_tag)
            for row in data:
                rec = ET.SubElement(root, record_tag)
                for k, v in row.items():
                    c = ET.SubElement(rec, str(k))
                    c.text = "" if v is None else str(v)
            ET.ElementTree(root).write(filepath, encoding="utf-8", xml_declaration=True)

        await asyncio.to_thread(_write)

    async def write_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            *,
            data: dd.DataFrame,
            record_tag: str = "row",
    ):
        def _write_partition(pdf: pd.DataFrame, part_idx: int):
            root = ET.Element("rows")
            for _, row in pdf.iterrows():
                rec = ET.SubElement(root, record_tag)
                for k, v in row.to_dict().items():
                    c = ET.SubElement(rec, str(k))
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        c.text = ""
                    else:
                        c.text = str(v)
            out = Path(filepath) / f"part-{part_idx:05d}.xml"
            ET.ElementTree(root).write(out, encoding="utf-8", xml_declaration=True)

        def _write_all():
            Path(filepath).mkdir(parents=True, exist_ok=True)
            for i in range(data.npartitions):
                pdf = data.get_partition(i).compute()
                _write_partition(pdf, i)

        await asyncio.to_thread(_write_all)
