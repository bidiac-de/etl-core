from pydantic import Field

from etl_core.components.databases.sql_database import SQLDatabaseComponent


class SQLServerComponent(SQLDatabaseComponent):
    """Base class for SQL Server components with common functionality."""

    charset: str = Field(default="utf8", description="Character set for SQL Server")
    collation: str = Field(
        default="SQL_Latin1_General_CP1_CI_AS", description="Collation for SQL Server"
    )

    def _setup_session_variables(self):
        """Setup SQL Server-specific session variables."""
        if not self._connection_handler or not self.charset:
            return

        try:
            with self._connection_handler.lease() as conn:
                # Set common SQL Server session variables
                conn.execute("SET ANSI_NULLS ON")
                conn.execute("SET ANSI_WARNINGS ON")
                conn.execute("SET QUOTED_IDENTIFIER ON")
                conn.execute("SET NOCOUNT ON")

                if self.charset:
                    conn.execute(f"SET LANGUAGE {self.charset}")
                if self.collation:
                    conn.execute(f"SET COLLATION {self.collation}")

                conn.commit()
        except Exception as e:
            print(f"Warning: Could not set SQL Server session variables: {e}")

    def _build_objects(self):
        """Build SQL Server-specific objects after validation."""
        return self
