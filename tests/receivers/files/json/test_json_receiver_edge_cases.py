import json
from pathlib import Path
import pandas as pd
import dask.dataframe as dd
import pytest
from unittest.mock import patch

import etl_core.receivers.files.json.json_receiver as jr
from etl_core.receivers.files.json.json_receiver import JSONReceiver
from etl_core.receivers.files.file_helper import FileReceiverError
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from datetime import datetime, timedelta


@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


class TestJSONReceiverErrorHandling:
    """Test error handling in JSON receiver."""

    @pytest.mark.asyncio
    async def test_write_row_type_validation(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test that write_row validates input type."""
        receiver = JSONReceiver()
        file_path = tmp_path / "test.json"

        with pytest.raises(TypeError, match="Row mode expects a dict payload"):
            await receiver.write_row(file_path, metrics, "not a dict")

    @pytest.mark.asyncio
    async def test_write_row_flat_paths_rejection(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test that write_row rejects flat paths."""
        receiver = JSONReceiver()
        file_path = tmp_path / "test.json"

        with pytest.raises(FileReceiverError, match="Row mode expects a nested dict"):
            await receiver.write_row(file_path, metrics, {"user.name": "John"})

    @pytest.mark.asyncio
    async def test_read_bulk_file_not_found(
        self, metrics: ComponentMetrics, tmp_path: Path
    ):
        """Test read_bulk with missing file."""
        receiver = JSONReceiver()
        missing_file = tmp_path / "missing.json"

        with pytest.raises(FileNotFoundError):
            await receiver.read_bulk(missing_file, metrics)

    @pytest.mark.asyncio
    async def test_read_row_file_not_found(
        self, metrics: ComponentMetrics, tmp_path: Path
    ):
        """Test read_row with missing file."""
        receiver = JSONReceiver()
        missing_file = tmp_path / "missing.jsonl"

        with pytest.raises(FileNotFoundError):
            async for _ in receiver.read_row(missing_file, metrics):
                pass

    @pytest.mark.asyncio
    async def test_read_bigdata_file_not_found(
        self, metrics: ComponentMetrics, tmp_path: Path
    ):
        """Test read_bigdata with missing file."""
        receiver = JSONReceiver()
        missing_file = tmp_path / "missing.jsonl"

        with pytest.raises(FileNotFoundError):
            await receiver.read_bigdata(missing_file, metrics)


class TestJSONReceiverMalformedData:
    """Test handling of malformed JSON data."""

    @pytest.mark.asyncio
    async def test_read_bulk_malformed_json(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test read_bulk with malformed JSON."""
        receiver = JSONReceiver()
        malformed_file = tmp_path / "malformed.json"
        malformed_file.write_text('{"incomplete": json}')

        with pytest.raises(FileReceiverError, match="Failed to read JSON to Pandas"):
            await receiver.read_bulk(malformed_file, metrics)

        assert metrics.error_count == 1

    @pytest.mark.asyncio
    async def test_read_row_ndjson_with_errors(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test read_row with NDJSON containing malformed lines."""
        receiver = JSONReceiver()
        ndjson_file = tmp_path / "malformed.jsonl"
        lines = ['{"valid": "line1"}', "invalid json line", '{"valid": "line2"}']
        ndjson_file.write_text("\n".join(lines))

        collected = []
        async for record in receiver.read_row(ndjson_file, metrics):
            collected.append(record)

        assert len(collected) == 2
        assert collected[0] == {"valid": "line1"}
        assert collected[1] == {"valid": "line2"}
        assert metrics.error_count == 1

    @pytest.mark.asyncio
    async def test_read_bulk_ndjson_with_errors(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test read_bulk with NDJSON containing malformed lines."""
        receiver = JSONReceiver()
        ndjson_file = tmp_path / "malformed.jsonl"
        lines = ['{"valid": "line1"}', "invalid json line", '{"valid": "line2"}']
        ndjson_file.write_text("\n".join(lines))

        df = await receiver.read_bulk(ndjson_file, metrics)

        assert len(df) == 2
        assert metrics.error_count == 1
        assert metrics.lines_forwarded == 2


class TestJSONReceiverWriteOperations:
    """Test write operations and their error handling."""

    @pytest.mark.asyncio
    async def test_write_bulk_error_handling(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test write_bulk error handling."""
        receiver = JSONReceiver()
        file_path = tmp_path / "test.json"

        data = pd.DataFrame([{"id": 1, "data": "test"}])

        with patch(
            "etl_core.receivers.files.json.json_receiver.dump_records_auto"
        ) as mock_dump:
            mock_dump.side_effect = RuntimeError("Write failed")

            with pytest.raises(FileReceiverError, match="Failed to write JSON bulk"):
                await receiver.write_bulk(file_path, metrics, data)

            assert metrics.error_count == 1

    @pytest.mark.asyncio
    async def test_write_row_error_handling(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test write_row error handling."""
        receiver = JSONReceiver()

        ndjson_file = tmp_path / "test.ndjson"

        with patch(
            "etl_core.receivers.files.json.json_receiver.append_ndjson_record"
        ) as mock_append:
            mock_append.side_effect = RuntimeError("Write failed")

            with pytest.raises(FileReceiverError, match="Failed to write JSON row"):
                await receiver.write_row(ndjson_file, metrics, {"id": 1})

            assert metrics.error_count == 1

    @pytest.mark.asyncio
    async def test_write_bigdata_error_handling(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test write_bigdata error handling."""
        import dask.dataframe as dd

        receiver = JSONReceiver()
        file_path = tmp_path / "test.json"

        data = pd.DataFrame([{"id": 1}])
        ddf = dd.from_pandas(data, npartitions=1)

        with patch(
            "etl_core.receivers.files.json.json_receiver.dask.compute"
        ) as mock_compute:
            mock_compute.side_effect = RuntimeError("Compute failed")

            with pytest.raises(FileReceiverError, match="Failed to write JSON bigdata"):
                await receiver.write_bigdata(file_path, metrics, ddf)

            assert metrics.error_count >= 1

    @pytest.mark.asyncio
    async def test_write_bigdata_outer_exception(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test write_bigdata outer exception handling."""
        import dask.dataframe as dd

        receiver = JSONReceiver()
        file_path = tmp_path / "test.json"

        data = pd.DataFrame([{"id": 1}])
        ddf = dd.from_pandas(data, npartitions=1)

        with patch.object(ddf, "to_delayed") as mock_delayed:
            mock_delayed.side_effect = RuntimeError("Delayed failed")

            with pytest.raises(FileReceiverError, match="Failed to write JSON bigdata"):
                await receiver.write_bigdata(file_path, metrics, ddf)

            assert metrics.error_count == 1


class TestJSONReceiverBigDataOperations:
    """Test bigdata operations and edge cases."""

    @pytest.mark.asyncio
    async def test_read_bigdata_json_to_ndjson_conversion_error(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test read_bigdata with JSON to NDJSON conversion errors."""
        receiver = JSONReceiver()
        json_file = tmp_path / "test.json"
        json_file.write_text('{"malformed": json}')

        with pytest.raises(FileReceiverError, match="Failed to read JSON bigdata"):
            await receiver.read_bigdata(json_file, metrics)

        assert metrics.error_count == 1

    @pytest.mark.asyncio
    async def test_read_bigdata_compute_error_handling(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test read_bigdata with compute errors."""
        receiver = JSONReceiver()
        ndjson_file = tmp_path / "test.jsonl"
        ndjson_file.write_text('{"id": 1}\n{"id": 2}')

        with patch(
            "etl_core.receivers.files.json.json_receiver.dd.read_json"
        ) as mock_read:
            mock_read.side_effect = RuntimeError("Read failed")

            with pytest.raises(FileReceiverError, match="Failed to read JSON bigdata"):
                await receiver.read_bigdata(ndjson_file, metrics)

            assert metrics.error_count == 1

    @pytest.mark.asyncio
    async def test_read_bigdata_directory_input(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test read_bigdata with directory input."""
        import dask.dataframe as dd

        receiver = JSONReceiver()
        data_dir = tmp_path / "data_dir"
        data_dir.mkdir()

        file1 = data_dir / "part1.jsonl"
        file2 = data_dir / "part2.jsonl"
        file1.write_text('{"id": 1}\n{"id": 2}')
        file2.write_text('{"id": 3}\n{"id": 4}')

        ddf = await receiver.read_bigdata(data_dir, metrics)

        assert isinstance(ddf, dd.DataFrame)
        df = ddf.compute()
        assert len(df) == 4
        assert metrics.lines_forwarded == 4

    @pytest.mark.asyncio
    async def test_write_bigdata_gzip_output(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test write_bigdata with gzip output."""
        import dask.dataframe as dd

        receiver = JSONReceiver()
        file_path = tmp_path / "output.jsonl.gz"

        data = pd.DataFrame([{"id": 1}, {"id": 2}])
        ddf = dd.from_pandas(data, npartitions=1)

        await receiver.write_bigdata(file_path, metrics, ddf)

        output_dir = file_path.parent / f"{file_path.stem}_parts"
        assert output_dir.exists()

        parts = list(output_dir.glob("part-*.jsonl.gz"))
        assert len(parts) == 1

        import gzip

        with gzip.open(parts[0], "rt") as f:
            content = f.read()
            assert '{"id": 1}' in content
            assert '{"id": 2}' in content


class TestJSONReceiverSpecialCases:
    """Test special cases and edge conditions."""

    @pytest.mark.asyncio
    async def test_read_bulk_empty_dataframe(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test read_bulk with empty data."""
        receiver = JSONReceiver()
        empty_file = tmp_path / "empty.json"
        empty_file.write_text("[]")

        df = await receiver.read_bulk(empty_file, metrics)

        assert len(df) == 0
        assert metrics.lines_forwarded == 0

    @pytest.mark.asyncio
    async def test_read_row_empty_file(self, tmp_path: Path, metrics: ComponentMetrics):
        """Test read_row with empty file."""
        receiver = JSONReceiver()
        empty_file = tmp_path / "empty.jsonl"
        empty_file.write_text("")

        collected = []
        async for record in receiver.read_row(empty_file, metrics):
            collected.append(record)

        assert len(collected) == 0
        assert metrics.lines_forwarded == 0

    @pytest.mark.asyncio
    async def test_write_row_ndjson_append(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test write_row with NDJSON file (append mode)."""
        receiver = JSONReceiver()
        ndjson_file = tmp_path / "test.ndjson"

        await receiver.write_row(ndjson_file, metrics, {"id": 1})
        await receiver.write_row(ndjson_file, metrics, {"id": 2})

        lines = ndjson_file.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"id": 1}
        assert json.loads(lines[1]) == {"id": 2}

    @pytest.mark.asyncio
    async def test_write_row_json_array_append(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test write_row with JSON file (array append mode)."""
        receiver = JSONReceiver()
        json_file = tmp_path / "test.json"

        await receiver.write_row(json_file, metrics, {"id": 1})
        await receiver.write_row(json_file, metrics, {"id": 2})

        data = json.loads(json_file.read_text())
        assert data == [{"id": 1}, {"id": 2}]

    @pytest.mark.asyncio
    async def test_read_bulk_single_object(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test read_bulk with single JSON object."""
        receiver = JSONReceiver()
        single_obj_file = tmp_path / "single.json"
        single_obj_file.write_text('{"id": 1, "name": "test"}')

        df = await receiver.read_bulk(single_obj_file, metrics)

        assert len(df) == 1
        assert df.iloc[0]["id"] == 1
        assert df.iloc[0]["name"] == "test"

    @pytest.mark.asyncio
    async def test_read_row_single_object(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test read_row with single JSON object."""
        receiver = JSONReceiver()
        single_obj_file = tmp_path / "single.json"
        single_obj_file.write_text('{"id": 1, "name": "test"}')

        collected = []
        async for record in receiver.read_row(single_obj_file, metrics):
            collected.append(record)

        assert len(collected) == 1
        assert collected[0] == {"id": 1, "name": "test"}

    @pytest.mark.asyncio
    async def test_metrics_tracking_accuracy(
        self, tmp_path: Path, metrics: ComponentMetrics
    ):
        """Test that metrics are tracked accurately."""
        receiver = JSONReceiver()
        json_file = tmp_path / "test.json"

        data = pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
        await receiver.write_bulk(json_file, metrics, data)

        assert metrics.lines_received == 3
        assert metrics.lines_forwarded == 3

        read_metrics = ComponentMetrics(
            started_at=datetime.now(),
            processing_time=timedelta(0),
            error_count=0,
            lines_received=0,
            lines_forwarded=0,
        )

        df = await receiver.read_bulk(json_file, read_metrics)

        assert len(df) == 3
        assert read_metrics.lines_forwarded == 3
        assert read_metrics.error_count == 0


def test__atomic_overwrite_replaces_content(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("old", encoding="utf-8")

    def writer(tmp: Path) -> None:
        tmp.write_text("new", encoding="utf-8")

    jr._atomic_overwrite(target, writer)  # type: ignore[attr-defined]
    assert target.read_text(encoding="utf-8") == "new"


def test__cleanup_temp_dirs_removes_registered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temp_dir = tmp_path / "t"
    temp_dir.mkdir(parents=True, exist_ok=True)
    (temp_dir / "x.txt").write_text("x", encoding="utf-8")

    monkeypatch.setattr(jr, "_TEMP_DIRS", set(), raising=True)
    jr._register_temp_dir(temp_dir)  # type: ignore[attr-defined]
    assert temp_dir in jr._TEMP_DIRS  # type: ignore[attr-defined]

    jr._cleanup_temp_dirs()  # type: ignore[attr-defined]
    assert not temp_dir.exists()
    assert not jr._TEMP_DIRS  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_read_bigdata_converts_json_array_to_ndjson(tmp_path: Path) -> None:
    src = tmp_path / "array.json"
    src.write_text(
        json.dumps(
            [{"id": 10, "name": "A"}, {"id": 11, "name": "B"}],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    r = JSONReceiver()
    df = (
        (await r.read_bigdata(src, metrics=_mk_metrics()))
        .compute()
        .sort_values("id")
        .reset_index(drop=True)
    )

    expected = (
        pd.DataFrame([{"id": 10, "name": "A"}, {"id": 11, "name": "B"}])
        .sort_values("id")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(df, expected, check_dtype=False)


@pytest.mark.asyncio
async def test_write_bigdata_to_dir_without_suffix(tmp_path: Path) -> None:
    out_dir = tmp_path / "out_dir"
    out_dir.mkdir(parents=True, exist_ok=True)

    pdf = pd.DataFrame([{"id": 1, "v": "x"}, {"id": 2, "v": "y"}])
    ddf = dd.from_pandas(pdf, npartitions=2)

    r = JSONReceiver()
    metrics = _mk_metrics()
    await r.write_bigdata(out_dir, metrics, ddf)

    parts = sorted(out_dir.glob("part-*.jsonl"))
    assert parts, "expected partition files"
    seen = []
    for p in parts:
        for line in p.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s:
                seen.append(json.loads(s))
    seen = sorted(seen, key=lambda x: x["id"])
    assert seen == [{"id": 1, "v": "x"}, {"id": 2, "v": "y"}]
    assert metrics.lines_received == 2
    assert metrics.lines_forwarded == 2


def _mk_metrics():
    from datetime import datetime, timedelta

    from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics

    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


@pytest.mark.asyncio
async def test_write_bulk_json_and_gz(
    tmp_path: Path, metrics: ComponentMetrics
) -> None:
    path_json = tmp_path / "bulk.json"
    df = pd.DataFrame([{"id": 1}, {"id": 2}])

    r = JSONReceiver()
    await r.write_bulk(path_json, metrics, df)

    assert json.loads(path_json.read_text(encoding="utf-8")) == [
        {"id": 1},
        {"id": 2},
    ]
    path_gz = tmp_path / "bulk.json.gz"
    await r.write_bulk(path_gz, metrics, df)

    import gzip

    with gzip.open(path_gz, "rt", encoding="utf-8") as f:
        assert json.load(f) == [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_write_bigdata_error_branch_increments_metrics(
    tmp_path: Path, metrics: ComponentMetrics
) -> None:
    """
    Force a single-part write failure. write_bigdata creates delayed tasks that
    call _write_part_ndjson, so patch that symbol to raise. This should increment
    metrics.error_count and raise FileReceiverError.
    """
    out = tmp_path / "out_dir"
    pdf = pd.DataFrame([{"id": 1}, {"id": 2}])
    ddf = dd.from_pandas(pdf, npartitions=1)

    def boom_part(_pdf: pd.DataFrame, _path: str) -> int:
        raise RuntimeError("boom")

    orig = jr._write_part_ndjson
    jr._write_part_ndjson = boom_part
    try:
        r = JSONReceiver()
        with pytest.raises(FileReceiverError, match="Failed to write JSON bigdata"):
            await r.write_bigdata(out, metrics, ddf)

        assert metrics.error_count >= 1
        assert not list(out.glob("part-*")), "no parts should be written after failure"
    finally:
        jr._write_part_ndjson = orig


@pytest.mark.asyncio
async def test_write_row_switches_between_json_and_ndjson(
    tmp_path: Path, metrics: ComponentMetrics
) -> None:
    """
    Ensure row writer appends to .jsonl and .json (array) correctly.
    """
    r = JSONReceiver()

    p_l = tmp_path / "rows.jsonl"
    await r.write_row(p_l, metrics, {"a": 1})
    await r.write_row(p_l, metrics, {"a": 2})
    rows = [json.loads(s) for s in p_l.read_text(encoding="utf-8").splitlines()]
    assert rows == [{"a": 1}, {"a": 2}]

    p_j = tmp_path / "rows.json"
    await r.write_row(p_j, metrics, {"b": 3})
    await r.write_row(p_j, metrics, {"b": 4})
    assert json.loads(p_j.read_text(encoding="utf-8")) == [{"b": 3}, {"b": 4}]
