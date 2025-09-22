from __future__ import annotations

import typer

from etl_core.api.helpers import autodiscover_components

from etl_core.api.cli.commands.contexts import contexts_app
from etl_core.api.cli.commands.execution import execution_app
from etl_core.api.cli.commands.jobs import jobs_app
from etl_core.api.cli.commands.schedules import schedules_app

app = typer.Typer(
    help="ETL control CLI to manage jobs, contexts/credentials and executions."
)
app.add_typer(jobs_app, name="jobs")
app.add_typer(execution_app, name="execution")
app.add_typer(contexts_app, name="contexts")
app.add_typer(schedules_app, name="schedules")


def run() -> None:
    autodiscover_components("etl_core.components")
    app()


if __name__ == "__main__":
    run()
