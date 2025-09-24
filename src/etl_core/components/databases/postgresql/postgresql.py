from pydantic import Field

from etl_core.components.databases.sql_database import SQLDatabaseComponent


class PostgreSQLComponent(SQLDatabaseComponent):
    """Base class for PostgreSQL components with common functionality."""

    ICON = "devicon-postgresql-plain"

    charset: str = Field(default="utf8", description="Character set for PostgreSQL")
    collation: str = Field(
        default="en_US.UTF-8", description="Collation for PostgreSQL"
    )

    def _setup_session_variables(self):
        """Setup PostgreSQL-specific session variables."""
        if not self._connection_handler or not self.charset:
            return

        try:
            with self._connection_handler.lease() as conn:
                if self.charset:
                    conn.execute(f"SET client_encoding = '{self.charset}'")
                if self.collation:
                    conn.execute(f"SET lc_collate = '{self.collation}'")
                conn.commit()
        except Exception as e:
            print(f"Warning: Could not set PostgreSQL session variables: {e}")

    def _build_objects(self):
        """Build PostgreSQL-specific objects after validation."""
        return self
