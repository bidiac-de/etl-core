from enum import Enum


class IfExistsStrategy(Enum):
    """Strategies for handling existing tables during write operations."""

    FAIL = "fail"  # Raise error if table exists
    REPLACE = "replace"  # Drop and recreate table
    APPEND = "append"  # Add data to existing table (default)
    TRUNCATE = "truncate"  # Empty table then insert


# String constants for database-specific strategies
class MariaDBIfExistsStrategy:
    """MariaDB-specific strategies for handling existing tables."""

    IGNORE = "ignore"  # MariaDB: IGNORE bei Duplikaten
    ON_DUPLICATE_UPDATE = "on_duplicate_update"  # MariaDB: ON DUPLICATE KEY UPDATE


class PostgreSQLIfExistsStrategy:
    """PostgreSQL-specific strategies for handling existing tables."""

    ON_CONFLICT_DO_NOTHING = (
        "on_conflict_do_nothing"  # PostgreSQL: ON CONFLICT DO NOTHING
    )
    ON_CONFLICT_UPDATE = "on_conflict_update"  # PostgreSQL: ON CONFLICT DO UPDATE
