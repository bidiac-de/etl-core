from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.excel import excel_receiver as Rcv  # module to patch
from etl_core.receivers.files.excel.excel_receiver import ExcelReceiver


@pytest.fixture
def metrics() -> ComponentMetrics:
    from datetime import datetime, timedelta

    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


@pytest.mark.asyncio
async def test_read_row_wraps_error(
    tmp_path: Path, metrics: ComponentMetrics, monkeypatch: pytest.MonkeyPatch
) -> None:
    fp = tmp_path / "ok.xlsx"
    pd.DataFrame({"A": [1]}).to_excel(fp, index=False)

    def boom(*_args: Any, **_kwargs: Any):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(Rcv, "read_excel_rows", boom, raising=True)

    recv = ExcelReceiver()
    with pytest.raises(
        Rcv.FileReceiverError, match="Failed to open excel for row-read:"
    ):
        async for _ in recv.read_row(fp, metrics):
            pass



@pytest.mark.asyncio
async def test_read_bigdata_open_raises(
    tmp_path: Path, metrics: ComponentMetrics, monkeypatch: pytest.MonkeyPatch
) -> None:
    fp = tmp_path / "ok.xlsx"
    pd.DataFrame({"a": [1]}).to_excel(fp, index=False)

    def boom_open(*_a: Any, **_k: Any):
        raise ValueError("nope")

    monkeypatch.setattr(Rcv, "read_excel_bigdata", boom_open, raising=True)

    recv = ExcelReceiver()
    with pytest.raises(Rcv.FileReceiverError, match="Failed to read excel bigdata:"):
        await recv.read_bigdata(fp, metrics)


@pytest.mark.asyncio
async def test_read_bigdata_count_fallback_to_zero(
    tmp_path: Path, metrics: ComponentMetrics, monkeypatch: pytest.MonkeyPatch
) -> None:
    fp = tmp_path / "ok.xlsx"
    pd.DataFrame({"a": [1, 2, 3]}).to_excel(fp, index=False)

    class _DDFFailCompute:
        def map_partitions(self, _func):
            class _Sum:
                def sum(self):
                    class _Comp:
                        def compute(self_inner):
                            raise RuntimeError("compute failed")

                    return _Comp()

            return _Sum()

    monkeypatch.setattr(
        Rcv, "read_excel_bigdata", lambda *_: _DDFFailCompute(), raising=True
    )

    recv = ExcelReceiver()
    ddf = await recv.read_bigdata(fp, metrics)
    assert metrics.lines_received == 0
    assert hasattr(ddf, "map_partitions")


@pytest.mark.asyncio
async def test_write_row_wraps_error(tmp_path: Path, metrics: ComponentMetrics) -> None:
    fp = tmp_path / "out.xls"
    recv = ExcelReceiver()
    with pytest.raises(Rcv.FileReceiverError, match="Failed to write excel row:"):
        await recv.write_row(fp, metrics, row={"a": 1})
    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 0


@pytest.mark.asyncio
async def test_write_bulk_wraps_error(
    tmp_path: Path, metrics: ComponentMetrics
) -> None:
    fp = tmp_path / "out.xls"
    df = pd.DataFrame({"a": [1, 2]})
    recv = ExcelReceiver()
    with pytest.raises(Rcv.FileReceiverError, match="Failed to write excel bulk:"):
        await recv.write_bulk(fp, metrics, data=df)
    assert metrics.lines_received == len(df)
    assert metrics.lines_forwarded == 0


class _FakeDDF:
    """
    Tiny ddf stub controllable via flags to hit branches in write_bigdata.
    """

    def __init__(
        self, persist_ok: bool, row_count: int, unpersist_raises: bool = False
    ) -> None:
        self._persist_ok = persist_ok
        self._row_count = row_count
        self._unpersist_raises = unpersist_raises

    def persist(self):
        if not self._persist_ok:
            raise RuntimeError("persist failed")
        return self

    def map_partitions(self, _func):
        class _Sum:
            def sum(self_inner):
                class _Comp:
                    def compute(self2):
                        return self._row_count

                return _Comp()

        return _Sum()

    def unpersist(self):
        if self._unpersist_raises:
            raise RuntimeError("unpersist failed")
        return None


@pytest.mark.asyncio
async def test_write_bigdata_persist_fails_then_succeeds_to_write(
    tmp_path: Path, metrics: ComponentMetrics, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Cover lines 133–134 (persist exception swallowed) by providing a ddf whose
    persist() raises; wrapper must still continue and compute count (0) and write.
    """
    fp = tmp_path / "ok.xlsx"

    ddf = _FakeDDF(persist_ok=False, row_count=3)

    monkeypatch.setattr(Rcv, "write_excel_bigdata", lambda *_: None, raising=True)

    recv = ExcelReceiver()
    await recv.write_bigdata(fp, metrics, data=ddf)
    assert metrics.lines_forwarded == 3


@pytest.mark.asyncio
async def test_write_bigdata_row_limit_exceeded_raises(
    tmp_path: Path, metrics: ComponentMetrics, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Cover lines 143–146: exceeding Excel's max rows triggers FileReceiverError.
    """
    fp = tmp_path / "too_many.xlsx"

    ddf = _FakeDDF(persist_ok=True, row_count=1_048_576 + 1)

    monkeypatch.setattr(Rcv, "write_excel_bigdata", lambda *_: None, raising=True)

    recv = ExcelReceiver()
    with pytest.raises(Rcv.FileReceiverError, match="exceeds Excel sheet limit"):
        await recv.write_bigdata(fp, metrics, data=ddf)


@pytest.mark.asyncio
async def test_write_bigdata_outer_pipeline_error_and_unpersist_finally(
    tmp_path: Path, metrics: ComponentMetrics, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Cover outer except (150–153) by making write_excel_bigdata raise.
    Also cover finally-unpersist exception swallow (155–159).
    """
    fp = tmp_path / "pipeline.xlsx"

    ddf = _FakeDDF(persist_ok=True, row_count=10, unpersist_raises=True)

    def boom_write(*_a: Any, **_k: Any):
        raise RuntimeError("write failed")

    monkeypatch.setattr(Rcv, "write_excel_bigdata", boom_write, raising=True)

    recv = ExcelReceiver()
    with pytest.raises(
        Rcv.FileReceiverError, match="Failed during excel write pipeline:"
    ):
        await recv.write_bigdata(fp, metrics, data=ddf)
