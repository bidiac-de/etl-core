import typer
import json
import requests

app = typer.Typer()


@app.command()
def create_job(path: str):
    cfg = json.load(open(path))
    resp = requests.post("http://127.0.0.1:8000/jobs", json=cfg)
    typer.echo(f"Created job {resp.json()['id']}")


@app.command()
def get_job(job_id: str):
    resp = requests.get(f"http://127.0.0.1:8000/jobs/{job_id}")
    if resp.status_code == 404:
        typer.echo(f"Job with ID {job_id} not found")
    else:
        typer.echo(json.dumps(resp.json(), indent=2))


@app.command()
def update_job(job_id: str, path: str):
    cfg = json.load(open(path))
    resp = requests.put(f"http://127.0.0.1:8000/jobs/{job_id}", json=cfg)
    if resp.status_code == 404:
        typer.echo(f"Job with ID {job_id} not found")
    else:
        typer.echo(f"Updated job {job_id}: {resp.json()}")


@app.command()
def delete_job(job_id: str):
    resp = requests.delete(f"http://127.0.0.1:8000/jobs/{job_id}")
    if resp.status_code == 404:
        typer.echo(f"Job with ID {job_id} not found")
    else:
        typer.echo(f"Deleted job {job_id}: {resp.json()}")


@app.command()
def start_execution(job_id: str):
    resp = requests.post(f"http://127.0.0.1:8000/execution/{job_id}")
    if resp.status_code == 404:
        typer.echo(f"Job with ID {job_id} not found")
    else:
        typer.echo(f"Started execution for job {job_id}: {resp.json()}")


@app.command()
def get_metrics(job_id: str):
    resp = requests.get(f"http://127.0.0.1:8000/ws/jobs/metrics/{job_id}")
    if resp.status_code == 404:
        typer.echo(f"running job with ID {job_id} not found")
    else:
        typer.echo(f"Metrics for job {job_id}: {resp.json()}")


@app.command()
def get_job_schema():
    resp = requests.get("http://127.0.0.1:8000/schemas/job")
    if resp.status_code == 404:
        typer.echo("Job schema not found")
    else:
        typer.echo(f"Job schema: {json.dumps(resp.json(), indent=2)}")


@app.command()
def list_component_types():
    resp = requests.get("http://127.0.0.1:8000/schemas/component_types")
    if resp.status_code == 404:
        typer.echo("Component types not found")
    else:
        typer.echo(f"Component types: {json.dumps(resp.json(), indent=2)}")


@app.command()
def get_component_schema(comp_type: str):
    resp = requests.get(f"http://127.0.01:8000/schemas/{comp_type}")
    if resp.status_code == 404:
        typer.echo(f"Component schema for {comp_type} not found")
    else:
        typer.echo(
            f"Component schema for {comp_type}: {json.dumps(resp.json(), indent=2)}"
        )
