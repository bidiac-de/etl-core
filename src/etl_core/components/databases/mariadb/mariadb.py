from pydantic import Field

from etl_core.components.databases.sql_database import SQLDatabaseComponent


class MariaDBComponent(SQLDatabaseComponent):
    """Base class for MariaDB components with common functionality."""

    charset: str = Field(default="utf8mb4", description="Character set for MariaDB")
    collation: str = Field(
        default="utf8mb4_unicode_ci", description="Collation for MariaDB"
    )
    ICON = "ti ti-brand-mysql"

    def _setup_session_variables(self):
        """Setup MariaDB-specific session variables."""
        if not self._connection_handler or not self.charset:
            return

        try:
            with self._connection_handler.lease() as conn:
                if self.charset:
                    conn.execute(f"SET NAMES {self.charset}")
                if self.collation:
                    conn.execute(f"SET collation_connection = {self.collation}")
                conn.commit()
        except Exception as e:
            print(f"Warning: Could not set MariaDB session variables: {e}")

    def _build_objects(self):
        """Build MariaDB-specific objects after validation."""
        super()._build_objects()
        # Set session variables after connection is established
        self._setup_session_variables()
        return self
