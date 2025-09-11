from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from typer.testing import CliRunner

from etl_core.api.cli.main import app


class TestCLIErrorHandling:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_jobs_create_file_not_found(self, runner):
        result = runner.invoke(app, ["jobs", "create", "nope.json"])
        assert result.exit_code != 0
        assert result.exception is not None
        assert "No such file or directory" in str(result.exception)

    def test_jobs_create_invalid_json(self, runner):
        bad = Path("tests/components/data/json/testdata_bad.json")
        result = runner.invoke(app, ["jobs", "create", str(bad)])
        assert result.exit_code != 0
        assert "Expecting property name" in str(result.exception)

    def test_jobs_create_empty_json(self, runner):
        tmp = Path("empty.json")
        tmp.write_text("{}")
        try:
            result = runner.invoke(app, ["jobs", "create", str(tmp)])
            assert result.exit_code == 0
        finally:
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(tmp)

    def test_jobs_update_file_not_found(self, runner):
        result = runner.invoke(app, ["jobs", "update", "job-id", "missing.json"])
        assert result.exit_code != 0
        assert "No such file or directory" in str(result.exception)

    def test_jobs_update_invalid_json(self, runner):
        bad = Path("tests/components/data/json/testdata_bad.json")
        result = runner.invoke(app, ["jobs", "update", "job-id", str(bad)])
        assert result.exit_code != 0
        assert "Expecting property name" in str(result.exception)

    def test_http_connection_error(self, runner):
        tmp = Path("cfg.json")
        tmp.write_text('{"name": "x"}')
        try:
            with patch("etl_core.api.cli.adapters.requests") as mreq:
                mreq.post.side_effect = Exception("Connection failed")
                result = runner.invoke(
                    app,
                    [
                        "jobs",
                        "create",
                        str(tmp),
                        "--remote",
                        "--base-url",
                        "http://invalid:9999",
                    ],
                )
                assert result.exit_code != 0
                assert "Connection failed" in str(result.exception)
        finally:
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(tmp)

    def test_http_error(self, runner):
        tmp = Path("cfg.json")
        tmp.write_text('{"name": "x"}')
        try:
            with patch("etl_core.api.cli.adapters.requests") as mreq:
                resp = Mock()
                resp.status_code = 500
                resp.raise_for_status.side_effect = Exception("Internal Server Error")
                mreq.post.return_value = resp
                result = runner.invoke(
                    app,
                    [
                        "jobs",
                        "create",
                        str(tmp),
                        "--remote",
                        "--base-url",
                        "http://localhost:8000",
                    ],
                )
                assert result.exit_code != 0
                assert "Internal Server Error" in str(result.exception)
        finally:
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(tmp)

    def test_missing_required_args(self, runner):
        assert runner.invoke(app, ["jobs", "create"]).exit_code != 0
        assert runner.invoke(app, ["jobs", "get"]).exit_code != 0
        assert runner.invoke(app, ["jobs", "update"]).exit_code != 0
        assert runner.invoke(app, ["jobs", "update", "job-id"]).exit_code != 0
        assert runner.invoke(app, ["jobs", "delete"]).exit_code != 0
        assert runner.invoke(app, ["execution", "start"]).exit_code != 0

    def test_remote_flag_without_base_url(self, runner):
        tmp = Path("cfg.json")
        tmp.write_text('{"name": "x"}')
        try:
            with patch("etl_core.api.cli.adapters.requests") as mreq:
                resp = Mock()
                resp.json.return_value = {"id": "test-job-id-123"}
                resp.raise_for_status.return_value = None
                mreq.post.return_value = resp
                result = runner.invoke(app, ["jobs", "create", str(tmp), "--remote"])
                assert result.exit_code == 0
                mreq.post.assert_called_once_with(
                    "http://127.0.0.1:8000/jobs", json={"name": "x"}, timeout=30
                )
        finally:
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(tmp)

    def test_unicode_in_config(self, runner):
        cfg = {"name": "tëst_job_ümlaut", "description": "Üñíčødë ✔", "components": []}
        tmp = Path("unicode.json")
        tmp.write_text(json.dumps(cfg), encoding="utf-8")
        try:
            with patch("etl_core.api.cli.adapters._jh_singleton") as jh:
                jh.return_value.create_job_entry.return_value = Mock(id="unicode-123")
                res = runner.invoke(app, ["jobs", "create", str(tmp)])
                assert res.exit_code == 0
                assert "Created job unicode-123" in res.stdout
        finally:
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(tmp)
