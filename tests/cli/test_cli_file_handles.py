"""
Tests for verifying that file handles are properly closed when loading config files.
"""

from __future__ import annotations

import builtins
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, IO, List
from unittest.mock import Mock, patch

from typer.testing import CliRunner

from etl_core.api.cli.cli_app import app

runner = CliRunner()


def _write_json(tmp_path: Path, name: str, payload: Dict[str, Any]) -> Path:
    p = tmp_path / name
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


@contextmanager
def _track_open(patch_target: str) -> Generator[List[IO[Any]], None, None]:
    opened_files: List[IO[Any]] = []
    original_open = builtins.open

    def tracking_open(*args: Any, **kwargs: Any) -> IO[Any]:
        fh = original_open(*args, **kwargs)
        opened_files.append(fh)
        return fh

    with patch(patch_target, side_effect=tracking_open):
        yield opened_files


def _assert_all_closed(opened_files: List[IO[Any]]) -> None:
    for fh in opened_files:
        assert fh.closed, "File handle was not properly closed"


class TestJobsFileHandles:
    def test_create_job_closes_file_handle(self, tmp_path: Path, monkeypatch) -> None:
        cfg = {"name": "test-job", "components": []}
        config_path = _write_json(tmp_path, "job.json", cfg)

        jobs_mock = Mock()
        jobs_mock.create.return_value = "new-job-id"
        monkeypatch.setattr(
            "etl_core.api.cli.commands.jobs.pick_clients",
            lambda: (jobs_mock, None, None),
        )

        with _track_open("etl_core.api.cli.commands.jobs.open") as opened_files:
            result = runner.invoke(app, ["jobs", "create", str(config_path)])

        assert result.exit_code == 0
        assert "Created job new-job-id" in result.stdout
        _assert_all_closed(opened_files)

    def test_update_job_closes_file_handle(self, tmp_path: Path, monkeypatch) -> None:
        cfg = {"name": "updated-job", "components": []}
        config_path = _write_json(tmp_path, "job_update.json", cfg)

        jobs_mock = Mock()
        jobs_mock.update.return_value = "updated-job-id"
        monkeypatch.setattr(
            "etl_core.api.cli.commands.jobs.pick_clients",
            lambda: (jobs_mock, None, None),
        )

        with _track_open("etl_core.api.cli.commands.jobs.open") as opened_files:
            result = runner.invoke(
                app, ["jobs", "update", "existing-id", str(config_path)]
            )

        assert result.exit_code == 0
        assert "Updated job updated-job-id" in result.stdout
        _assert_all_closed(opened_files)

    def test_create_job_closes_file_on_json_error(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        invalid_json = tmp_path / "invalid.json"
        invalid_json.write_text("{invalid json", encoding="utf-8")

        jobs_mock = Mock()
        monkeypatch.setattr(
            "etl_core.api.cli.commands.jobs.pick_clients",
            lambda: (jobs_mock, None, None),
        )

        with _track_open("etl_core.api.cli.commands.jobs.open") as opened_files:
            result = runner.invoke(app, ["jobs", "create", str(invalid_json)])

        assert result.exit_code != 0
        _assert_all_closed(opened_files)


class TestContextsFileHandles:
    def test_create_context_closes_file_handle(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        cfg = {"name": "test-ctx", "environment": "TEST", "parameters": {}}
        config_path = _write_json(tmp_path, "context.json", cfg)

        ctxs_mock = Mock()
        ctxs_mock.create_context.return_value = {"ok": True}
        monkeypatch.setattr(
            "etl_core.api.cli.commands.contexts.pick_clients",
            lambda: (None, None, ctxs_mock),
        )

        with _track_open("etl_core.api.cli.commands.contexts.open") as opened_files:
            result = runner.invoke(
                app, ["contexts", "create-context", str(config_path)]
            )

        assert result.exit_code == 0
        _assert_all_closed(opened_files)

    def test_create_credentials_closes_file_handle(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        cfg = {
            "name": "test-creds",
            "user": "u",
            "host": "h",
            "port": 5432,
            "database": "d",
            "password": "secret",
        }
        config_path = _write_json(tmp_path, "creds.json", cfg)

        ctxs_mock = Mock()
        ctxs_mock.create_credentials.return_value = {"ok": True}
        monkeypatch.setattr(
            "etl_core.api.cli.commands.contexts.pick_clients",
            lambda: (None, None, ctxs_mock),
        )

        with _track_open("etl_core.api.cli.commands.contexts.open") as opened_files:
            result = runner.invoke(
                app, ["contexts", "create-credentials", str(config_path)]
            )

        assert result.exit_code == 0
        _assert_all_closed(opened_files)

    def test_create_context_mapping_closes_file_handle(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        cfg = {
            "name": "test-mapping",
            "environment": "TEST",
            "credentials_ids": {"TEST": "creds-id"},
        }
        config_path = _write_json(tmp_path, "mapping.json", cfg)

        ctxs_mock = Mock()
        ctxs_mock.create_context_mapping.return_value = {"ok": True}
        monkeypatch.setattr(
            "etl_core.api.cli.commands.contexts.pick_clients",
            lambda: (None, None, ctxs_mock),
        )

        with _track_open("etl_core.api.cli.commands.contexts.open") as opened_files:
            result = runner.invoke(
                app, ["contexts", "create-context-mapping", str(config_path)]
            )

        assert result.exit_code == 0
        _assert_all_closed(opened_files)

    def test_create_context_closes_file_on_error(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        invalid_json = tmp_path / "invalid_context.json"
        invalid_json.write_text("{not valid json", encoding="utf-8")

        ctxs_mock = Mock()
        monkeypatch.setattr(
            "etl_core.api.cli.commands.contexts.pick_clients",
            lambda: (None, None, ctxs_mock),
        )

        with _track_open("etl_core.api.cli.commands.contexts.open") as opened_files:
            result = runner.invoke(
                app, ["contexts", "create-context", str(invalid_json)]
            )

        assert result.exit_code != 0
        _assert_all_closed(opened_files)


class TestFileHandleEdgeCases:
    def test_empty_json_file_closes_handle(self, tmp_path: Path, monkeypatch) -> None:
        empty_obj = tmp_path / "empty.json"
        empty_obj.write_text("{}", encoding="utf-8")

        jobs_mock = Mock()
        jobs_mock.create.return_value = "new-id"
        monkeypatch.setattr(
            "etl_core.api.cli.commands.jobs.pick_clients",
            lambda: (jobs_mock, None, None),
        )

        with _track_open("etl_core.api.cli.commands.jobs.open") as opened_files:
            _ = runner.invoke(app, ["jobs", "create", str(empty_obj)])

        _assert_all_closed(opened_files)

    def test_large_json_file_closes_handle(self, tmp_path: Path, monkeypatch) -> None:
        large_cfg = {
            "name": "large-job",
            "components": [{"name": f"comp-{i}"} for i in range(100)],
        }
        config_path = _write_json(tmp_path, "large_job.json", large_cfg)

        jobs_mock = Mock()
        jobs_mock.create.return_value = "large-job-id"
        monkeypatch.setattr(
            "etl_core.api.cli.commands.jobs.pick_clients",
            lambda: (jobs_mock, None, None),
        )

        with _track_open("etl_core.api.cli.commands.jobs.open") as opened_files:
            result = runner.invoke(app, ["jobs", "create", str(config_path)])

        assert result.exit_code == 0
        _assert_all_closed(opened_files)

    def test_unicode_json_file_closes_handle(self, tmp_path: Path, monkeypatch) -> None:
        unicode_cfg = {
            "name": "unicode-job-日本語",
            "description": "Descripción con caracteres especiales: äöü ñ 中文",
            "components": [],
        }
        config_path = _write_json(tmp_path, "unicode_job.json", unicode_cfg)

        jobs_mock = Mock()
        jobs_mock.create.return_value = "unicode-job-id"
        monkeypatch.setattr(
            "etl_core.api.cli.commands.jobs.pick_clients",
            lambda: (jobs_mock, None, None),
        )

        with _track_open("etl_core.api.cli.commands.jobs.open") as opened_files:
            result = runner.invoke(app, ["jobs", "create", str(config_path)])

        assert result.exit_code == 0
        _assert_all_closed(opened_files)
