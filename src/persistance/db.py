import os
from dotenv import load_dotenv
from sqlmodel import SQLModel, create_engine
from sqlmodel.pool import StaticPool
from src.persistance.table_definitions import (
    JobTable,
    ComponentTable,
    MetaDataTable,
    LayoutTable,
)

# ensure the table definitions are loaded
_, _, _, _ = JobTable, ComponentTable, MetaDataTable, LayoutTable

load_dotenv()
db_path = os.getenv("DB_PATH")

if db_path:
    dir_name = os.path.dirname(db_path)
    if dir_name:
        # create the directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

url = db_path and f"sqlite:///{db_path}" or "sqlite:///:memory:"
engine = create_engine(
    url,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
SQLModel.metadata.create_all(engine)
