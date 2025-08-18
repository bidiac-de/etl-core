import os
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import event
from sqlmodel import SQLModel, create_engine
from sqlmodel.pool import StaticPool

from src.persistance.table_definitions import (
    JobTable,
    ComponentTable,
    MetaDataTable,
    LayoutTable,
)

# Ensure model classes are imported
_, _, _, _ = JobTable, ComponentTable, MetaDataTable, LayoutTable

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


# allow foreign keys in SQLite
@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn: Any, _: Any) -> None:
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA foreign_keys=ON")
    cur.close()


# SQLModel.metadata.drop_all(engine)
SQLModel.metadata.create_all(engine)
