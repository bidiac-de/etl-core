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


@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn: Any, _: Any) -> None:
    cur = dbapi_conn.cursor()
    try:
        cur.execute("PRAGMA foreign_keys=ON")
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
    finally:
        cur.close()


SQLModel.metadata.create_all(engine)


def _column_exists(table: str, column: str) -> bool:
    try:
        with engine.connect() as conn:
            rows = conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
            return any(r[1] == column for r in rows)
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
        conn.exec_driver_sql("ALTER TABLE scheduletable ADD COLUMN job_id VARCHAR")
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
        conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_scheduletable_job_id "
            "ON scheduletable (job_id)"
        )


_migrate_schedules_add_job_id()
