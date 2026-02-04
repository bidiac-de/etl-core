from pydantic import Field

from etl_core.components.databases.sql_database import SQLDatabaseComponent


class MariaDBComponent(SQLDatabaseComponent):
    """Base class for MariaDB components with common functionality."""

    charset: str = Field(default="utf8mb4", description="Character set for MariaDB")
    collation: str = Field(
        default="utf8mb4_unicode_ci", description="Collation for MariaDB"
    )
    ICON = "devicon-mariadb-plain"

    def _apply_session_variables(self, conn):
        """Apply MariaDB-specific session variables to a live connection."""
        if self.charset:
            conn.execute(f"SET NAMES {self.charset}")
        if self.collation:
            conn.execute(f"SET collation_connection = {self.collation}")

    def _build_objects(self):
        """Build MariaDB-specific objects after validation."""
        return self
