from __future__ import annotations

import typer

from .commands.contexts import contexts_app
from .commands.execution import execution_app
from .commands.jobs import jobs_app

app = typer.Typer(
    help="ETL control CLI to manage jobs, contexts/credentials and executions."
)
app.add_typer(jobs_app, name="jobs")
app.add_typer(execution_app, name="execution")
app.add_typer(contexts_app, name="contexts")


def run() -> None:
    app()


if __name__ == "__main__":
    run()
