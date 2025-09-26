import json
from pathlib import Path
import pandas as pd
import pytest
from unittest.mock import patch

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

        # Create a DataFrame that might cause issues
        data = pd.DataFrame([{"id": 1, "data": "test"}])

        # Mock the dump_records_auto to raise an exception
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

        # Test with NDJSON file to trigger append_ndjson_record
        ndjson_file = tmp_path / "test.ndjson"

        # Mock append_ndjson_record to raise an exception
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

        # Mock dask.compute to raise an exception
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

        # Mock to_delayed to raise an exception
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

        # Mock dd.read_json to raise an exception
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

        # Create multiple JSONL files
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

        # Write some data
        data = pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
        await receiver.write_bulk(json_file, metrics, data)

        assert metrics.lines_received == 3
        assert metrics.lines_forwarded == 3

        # Read the data back
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
