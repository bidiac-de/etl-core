from pydantic import Field

from etl_core.components.databases.sql_database import SQLDatabaseComponent


class PostgreSQLComponent(SQLDatabaseComponent):
    """Base class for PostgreSQL components with common functionality."""

    ICON = "devicon-postgresql-plain"

    charset: str = Field(default="utf8", description="Character set for PostgreSQL")
    collation: str = Field(
        default="en_US.UTF-8", description="Collation for PostgreSQL"
    )

    def _apply_session_variables(self, conn):
        """Apply PostgreSQL-specific session variables to a live connection."""
        if self.charset:
            conn.execute(f"SET client_encoding = '{self.charset}'")
        if self.collation:
            conn.execute(f"SET lc_collate = '{self.collation}'")

    def _build_objects(self):
        """Build PostgreSQL-specific objects after validation."""
        return self
