import os
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import event
from sqlmodel import SQLModel, create_engine
from sqlmodel.pool import StaticPool

from etl_core.persistence.table_definitions import (
    JobTable,
    ComponentTable,
    MetaDataTable,
    LayoutTable,
    ComponentLinkTable,
    CredentialsTable,
    ContextTable,
    ContextParameterTable,
    ContextCredentialsMapTable,
    ScheduleTable,
)

# Ensure model classes are imported
_, _, _, _, _ = JobTable, ComponentTable, MetaDataTable, LayoutTable, ComponentLinkTable
_, _, _, _ = (
    CredentialsTable,
    ContextTable,
    ContextParameterTable,
    ContextCredentialsMapTable,
)
_ = ScheduleTable

load_dotenv()
db_path = os.getenv("DB_PATH")

if db_path:
    dir_name = os.path.dirname(db_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

url = db_path and f"sqlite:///{db_path}" or "sqlite:///:memory:"
engine = create_engine(
    url,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)


# allow foreign keys in SQLite and improve concurrency for file-backed DBs
@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn: Any, _: Any) -> None:
    cur = dbapi_conn.cursor()
    try:
        cur.execute("PRAGMA foreign_keys=ON")
        # Enable WAL mode for better concurrency across processes
        cur.execute("PRAGMA journal_mode=WAL")
        # Trade a little durability for speed; safe for many apps
        cur.execute("PRAGMA synchronous=NORMAL")
    finally:
        cur.close()


SQLModel.metadata.create_all(engine)


# --- Lightweight migrations for backward compatibility ---
def _column_exists(table: str, column: str) -> bool:
    try:
        with engine.connect() as conn:
            rows = conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
            return any(r[1] == column for r in rows)  # (cid, name, type, ...)
    except Exception:
        return False


def _migrate_schedules_add_job_id() -> None:
    """
    Earlier versions stored schedule -> job via 'job_name'. New schema uses 'job_id'.
    Add 'job_id' column if missing and backfill values by joining on job.name.
    """
    if _column_exists("scheduletable", "job_id"):
        return
    with engine.begin() as conn:
        # 1) add column as nullable (SQLite restriction for ALTER TABLE)
        conn.exec_driver_sql("ALTER TABLE scheduletable ADD COLUMN job_id VARCHAR")
        # 2) backfill values by matching on job name
        conn.exec_driver_sql(
            """
            UPDATE scheduletable
            SET job_id = (
                SELECT jobtable.id
                FROM jobtable
                WHERE jobtable.name = scheduletable.job_name
                LIMIT 1
            )
            """
        )
        # 3) add an index for faster lookups (optional)
        conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_scheduletable_job_id "
            "ON scheduletable (job_id)"
        )


# run lightweight migrations at import time so consumers see the latest shape
_migrate_schedules_add_job_id()
