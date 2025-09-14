from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

from typer.testing import CliRunner

from etl_core.api.cli.main import app


class TestCLIErrorHandling:
    @staticmethod
    def runner():
        return CliRunner()

    def test_jobs_create_file_not_found(self):
        runner = self.runner()
        result = runner.invoke(app, ["jobs", "create", "nope.json"])
        assert result.exit_code != 0
        assert result.exception is not None
        assert "No such file or directory" in str(result.exception)

    def test_jobs_create_invalid_json(self):
        runner = self.runner()
        bad = Path("tests/components/data/json/testdata_bad.json")
        result = runner.invoke(app, ["jobs", "create", str(bad)])
        assert result.exit_code != 0
        assert "Expecting property name" in str(result.exception)

    def test_jobs_create_empty_json(self):
        runner = self.runner()
        tmp = Path("empty.json")
        tmp.write_text("{}")
        try:
            result = runner.invoke(app, ["jobs", "create", str(tmp)])
            assert result.exit_code == 0
        finally:
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(tmp)

    def test_jobs_update_file_not_found(self):
        runner = self.runner()
        result = runner.invoke(app, ["jobs", "update", "job-id", "missing.json"])
        assert result.exit_code != 0
        assert "No such file or directory" in str(result.exception)

    def test_jobs_update_invalid_json(self):
        runner = self.runner()
        bad = Path("tests/components/data/json/testdata_bad.json")
        result = runner.invoke(app, ["jobs", "update", "job-id", str(bad)])
        assert result.exit_code != 0
        assert "Expecting property name" in str(result.exception)

    def test_missing_required_args(self):
        runner = self.runner()
        assert runner.invoke(app, ["jobs", "create"]).exit_code != 0
        assert runner.invoke(app, ["jobs", "get"]).exit_code != 0
        assert runner.invoke(app, ["jobs", "update"]).exit_code != 0
        assert runner.invoke(app, ["jobs", "update", "job-id"]).exit_code != 0
        assert runner.invoke(app, ["jobs", "delete"]).exit_code != 0
        assert runner.invoke(app, ["execution", "start"]).exit_code != 0

    def test_unicode_in_config(self):
        runner = self.runner()
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
