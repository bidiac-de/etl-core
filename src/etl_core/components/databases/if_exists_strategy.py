from enum import Enum


class IfExistsStrategy(Enum):
    """Strategies for handling existing tables during write operations."""
    FAIL = "fail"           # Raise error if table exists
    REPLACE = "replace"     # Drop and recreate table
    APPEND = "append"       # Add data to existing table (default)
    TRUNCATE = "truncate"   # Empty table then insert
