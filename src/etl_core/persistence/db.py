import os
import re
import threading
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import event, inspect
from sqlalchemy.exc import SQLAlchemyError
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

# Force imports so tables get registered
_, _, _, _, _ = JobTable, ComponentTable, MetaDataTable, LayoutTable, ComponentLinkTable
_, _, _, _ = (
    CredentialsTable,
    ContextTable,
    ContextParameterTable,
    ContextCredentialsMapTable,
)
_ = ScheduleTable

# --- Database setup ---
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


_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False

# --- Known tables cache ---
_KNOWN_TABLES = {t.lower() for t in SQLModel.metadata.tables.keys()}

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _column_exists(table: str, column: str) -> bool:
    """
    Return True if `column` exists in `table`.

    Security:
      - Table validated against known table names from SQLModel.metadata.
      - No SQL string construction (uses SQLAlchemy Inspector.get_columns).
    Robustness:
      - Validate `column` is a simple identifier to fail fast on junk input.
      - Case-insensitive column comparison to avoid casing mismatches.
    """
    if not isinstance(table, str) or not isinstance(column, str):
        return False

    table_l = table.strip().lower()
    column_l = column.strip().lower()

    if not table_l or not column_l:
        return False

    if table_l not in _KNOWN_TABLES:
        return False

    if not _IDENTIFIER_RE.match(column):
        return False

    try:
        inspector = inspect(engine)
        cols = inspector.get_columns(table_l)
        return any((c.get("name") or "").lower() == column_l for c in cols)
    except SQLAlchemyError:
        return False


def _migrate_schedules_add_job_id() -> None:
    """Ensure schedules reference jobs by id instead of name.

    Triggered via ``ensure_schema`` so the schema is up-to-date before any
    handlers rely on it. If ``job_id`` already exists the function exits without
    touching the table. When ``job_name`` is present we backfill values by
    matching on the legacy name column; the SQL statements simply no-op when no
    schedules require migration.
    """
    if _column_exists("scheduletable", "job_id"):
        return

    has_job_name = _column_exists("scheduletable", "job_name")

    with engine.begin() as conn:
        conn.exec_driver_sql("ALTER TABLE scheduletable ADD COLUMN job_id VARCHAR")
        if has_job_name:
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


def ensure_schema() -> None:
    """Create core tables and run lightweight migrations on first use."""

    global _SCHEMA_READY, _KNOWN_TABLES
    if _SCHEMA_READY:
        return

    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return

        SQLModel.metadata.create_all(engine)
        _migrate_schedules_add_job_id()

        # Refresh known table cache in case metadata gained new tables.
        _KNOWN_TABLES = {t.lower() for t in SQLModel.metadata.tables.keys()}

        _SCHEMA_READY = True
