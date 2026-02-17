# ETL Core

ETL Core is the execution engine behind ETL Studio. It loads jobs from persisted JSON configuration, validates component wiring/schemas, and executes pipelines with row/bulk/bigdata strategies.

## Scope

- Repository: `/Users/conradhofstede/Projects/UNI/SEP/ETL/etl-core`
- UI repository: `/Users/conradhofstede/Projects/UNI/SEP/ETL/etl-studio`
- Studio and Core are deployed as a strict pair via `contract_version`.

## Core/Studio API Contract (Current)

Studio expects these endpoints:

- `GET /setup/capabilities`
- `POST /setup/validate`
- `GET /configs/component_types`
- `GET /configs/{comp_type}/form`
- `GET /configs/{comp_type}/full`
- `GET /configs/job`
- `GET/POST/PUT/DELETE /jobs/*`
- `POST /execution/{job_id}`
- `GET /contexts/`
- `POST /contexts/credentials`
- `POST /contexts/credentials-mapping-context`

Key contract guarantees:

- `GET /setup/capabilities` returns `contract_version`, environments, rule operators, data types, and setup-validation metadata.
- `POST /setup/validate` accepts `{"key":"..."}` and returns `{"valid": bool}`.
- `GET /configs/{comp_type}/form` includes mandatory `x-ui` and `x-class` metadata for dynamic Studio rendering.
- Errors use a canonical envelope:
  - `{"error": {"code": str, "message": str, "details": [], "context": {}}}`

Detailed spec: `docs/studio_core_contract.md`.

## Local Start (macOS)

```bash
cd /Users/conradhofstede/Projects/UNI/SEP/ETL/etl-core
mkdir -p data logs
cp -n .env_demo .env
PYTHONPATH=src /Users/conradhofstede/Projects/UNI/SEP/ETL/etl-core/.conda/bin/uvicorn etl_core.main:app --host 127.0.0.1 --port 8000 --reload
```

Or helper:

```bash
cd /Users/conradhofstede/Projects/UNI/SEP/ETL/etl-core
./scripts/demo/start_core.sh
```

OpenAPI: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

## Demo Stack Helpers

In this repo:

- `scripts/demo/start_core.sh`
- `scripts/demo/docker_up.sh`
- `scripts/demo/docker_seed.sh`
- `scripts/demo/create_three_jobs.sh`
- `scripts/demo/verify_three_jobs.sh`
- `scripts/demo/create_complex_story_job.sh`
- `scripts/demo/verify_complex_story_job.sh`
- `scripts/demo/docker_down.sh`

In Studio repo:

- `/Users/conradhofstede/Projects/UNI/SEP/ETL/etl-studio/scripts/demo/start_studio.sh`
- `/Users/conradhofstede/Projects/UNI/SEP/ETL/etl-studio/scripts/demo/prepare_setup_db.sh`

Full walkthrough: `docs/mac_demo_runbook.md`.

## Testing

Run all core tests:

```bash
cd /Users/conradhofstede/Projects/UNI/SEP/ETL/etl-core
PYTHONPATH=src /Users/conradhofstede/Projects/UNI/SEP/ETL/etl-core/.conda/bin/pytest -q
```

## Documentation

- `docs/documentation.md` (index + Nuclino page map)
- `docs/studio_core_contract.md` (strict interlock contract)
- `docs/mac_demo_runbook.md` (end-to-end local demo)
- `docs/cli.md` (CLI reference)
- `docs/postgresql_components.md`
- `docs/mariadb_components.md`
- `docs/sql_connection_handler.md`
- `docs/filter_system.md`

## License

AGPL. See `LICENSE`.
