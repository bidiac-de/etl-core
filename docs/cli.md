# etl-core CLI — Quick Reference

Below is a compact, task-oriented overview of all available CLI commands, their flags, and important gotchas when using them.

> Installed command: **`etl`** (entry point defined in `pyproject.toml`).
> Command groups: **`jobs`**, **`execution`**, **`contexts`**.

---

## Global startup behavior

- The CLI auto-discovers components on start (`autodiscover_components("etl_core.components")`).

---

## Jobs

Create, inspect, update, delete, or list jobs.

### Commands

- **Create**
  `etl jobs create PATH/TO/job.json`
  Prints: `Created job <job_id>`.

- **Get**
  `etl jobs get JOB_ID`
  Prints full job as JSON (adds `"id"` to the payload). Exit code **1** if not found.

- **Update**
  `etl jobs update JOB_ID PATH/TO/job.json`
  Prints: `Updated job <job_id>`. Exit code **1** if not found.

- **Delete**
  `etl jobs delete JOB_ID`
  Prints: `Deleted job <job_id>`. Exit code **1** if not found.

- **List**
  `etl jobs list`
  Prints a brief list of jobs as JSON.

## Execution

Start executions and browse their history/attempts.

### Commands

- **Start**
  `etl execution start JOB_ID [--environment ENV]`
  `ENV` is optional (`TEST`, `DEV`, `PROD`, etc.; case-insensitive). Prints JSON like:
  `{ "job_id": "...", "status": "started", "execution_id": "...", "max_attempts": ..., "environment": "ENV" }`
  Exit code **1** if the job doesn’t exist.

- **List**
  `etl execution list [--job-id JOB_ID] [--status STATUS] [--environment ENV] [--started-after ISO] [--started-before ISO] [--sort-by started_at|finished_at|status] [--order asc|desc] [--limit 1..200] [--offset N]`
  Prints: `{ "data": [ { execution... }, ... ] }` (no `meta` block). Datetime filters use ISO-8601.

- **Get**
  `etl execution get EXECUTION_ID`
  Prints: `{ "execution": {...}, "attempts": [ ... ] }`. Exit code **1** if not found.

- **Attempts**
  `etl execution attempts EXECUTION_ID`
  Prints an array of attempts. Exit code **1** if not found.

### Keep in mind
- Timestamps are ISO strings; `finished_at` may be `null` for running/failed executions without end time.

---

## Contexts & Credentials

Manage secure parameter providers, credentials, and context-to-credentials mappings.

### Commands

- **Create Context**
  `etl contexts create-context PATH/TO/context.json [--keyring-service SERVICE]`
  Prints: `{ "id": "...", "kind": "context", "environment": "...", "parameters_registered": N }`.

- **Create Credentials**
  `etl contexts create-credentials PATH/TO/credentials.json [--keyring-service SERVICE]`
  Prints: `{ "id": "...", "kind": "credentials", "environment": null, "parameters_registered": 1 }`.

- **Create Context-Mapping**
  `etl contexts create-context-mapping PATH/TO/mapping.json`
  Creates a **CredentialsMappingContext** that ties environments to credential IDs; fails if any referenced credentials are unknown. Prints a summary JSON. Exit code **1** on missing IDs.

- **List Providers**
  `etl contexts list`
  Prints a combined list of all provider IDs with kinds, e.g.
  `[{"id": "...","kind": "context"}, {"id": "...","kind": "credentials"}]`.

- **Get Provider**
  `etl contexts get PROVIDER_ID`
  Looks up either a context or a credentials entry by ID. Exit code **1** if not found.

- **Delete Provider**
  `etl contexts delete PROVIDER_ID`
  Best-effort deletion on both context and credentials stores. Prints a confirmation; exit code **1** if neither exists.

### Keep in mind
- **Secrets storage**: By default, secrets are written to a keyring service named `sep-sose-2025/default`. Override per command with `--keyring-service`. Non-secure parameters are persisted in plain storage; secure parameter **keys** are tracked, values remain in keyring.
- **IDs** are generated UUIDs and are the single source of truth across contexts, credentials, and mappings. Keep them around for wiring jobs.

---

## Typical workflow

1) Register credentials → 2) Create a mapping context (bind ENV→credentials) **or** create a plain context → 3) Create a job (referencing the context/mapping by ID) → 4) Start an execution → 5) Inspect executions/attempts.

---

## Errors & exit codes

- Most `get`/`update`/`delete`/`start` commands exit with code **1** if the target resource is not found; the error message is printed to stdout before exit. Adjust shell scripts/CI to check the exit code.

---

## Examples

```bash
# Create credentials and remember the printed ID
etl contexts create-credentials creds_mongo.json

# Bind environments to credential IDs (e.g., TEST/PROD)
etl contexts create-context-mapping mapping.json

# Create a job config (references the context/mapping by ID)
etl jobs create jobs/my_job.json

# Kick off an execution in TEST
etl execution start <JOB_ID> --environment TEST

# List latest executions for a job
etl execution list --job-id <JOB_ID> --limit 20

# Inspect one execution and its attempts
etl execution get <EXEC_ID>
etl execution attempts <EXEC_ID>
```
