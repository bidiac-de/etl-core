from enum import Enum


class DatabaseOperation(Enum):
    """Core database operations for ETL processes."""

    INSERT = "insert"  # Pure insert (can fail on duplicates)
    UPSERT = "upsert"  # Insert or update on conflict
    TRUNCATE = "truncate"  # Clear table before operation
    UPDATE = "update"  # Pure update operation
