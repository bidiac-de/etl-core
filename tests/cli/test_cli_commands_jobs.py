import json
from unittest.mock import Mock
from typer.testing import CliRunner

from etl_core.api.cli.cli_app import app
from etl_core.persistence.errors import PersistNotFoundError


runner = CliRunner()


class TestJobsCLIExtra:
    def test_jobs_get_not_found(self, monkeypatch):
        jobs = Mock()
        jobs.get.side_effect = PersistNotFoundError("x")
        monkeypatch.setattr("etl_core.api.cli.commands.jobs.pick_clients", lambda: (jobs, None, None))
        res = runner.invoke(app, ["jobs", "get", "missing-id"])
        assert res.exit_code == 1
        assert "not found" in res.stdout

    def test_jobs_update_success_and_not_found(self, temp_config_file, monkeypatch):
        jobs = Mock()
        jobs.update.return_value = "updated-id"
        monkeypatch.setattr("etl_core.api.cli.commands.jobs.pick_clients", lambda: (jobs, None, None))
        # success
        res = runner.invoke(app, ["jobs", "update", "jid", str(temp_config_file)])
        assert res.exit_code == 0
        assert "Updated job updated-id" in res.stdout

        # not found
        jobs.update.side_effect = PersistNotFoundError("x")
        res = runner.invoke(app, ["jobs", "update", "jid", str(temp_config_file)])
        assert res.exit_code == 1
        assert "not found" in res.stdout

    def test_jobs_delete_success_and_not_found(self, monkeypatch):
        jobs = Mock()
        monkeypatch.setattr("etl_core.api.cli.commands.jobs.pick_clients", lambda: (jobs, None, None))
        # success
        res = runner.invoke(app, ["jobs", "delete", "jid"])
        assert res.exit_code == 0
        assert "Deleted job jid" in res.stdout

        # not found
        jobs.delete.side_effect = PersistNotFoundError("x")
        res = runner.invoke(app, ["jobs", "delete", "jid"])
        assert res.exit_code == 1
        assert "not found" in res.stdout

    def test_jobs_list_outputs_json(self, monkeypatch):
        jobs = Mock()
        jobs.list_brief.return_value = [{"id": "j1", "name": "n"}]
        monkeypatch.setattr("etl_core.api.cli.commands.jobs.pick_clients", lambda: (jobs, None, None))
        res = runner.invoke(app, ["jobs", "list"])
        assert res.exit_code == 0
        data = json.loads(res.stdout)
        assert data[0]["id"] == "j1"
