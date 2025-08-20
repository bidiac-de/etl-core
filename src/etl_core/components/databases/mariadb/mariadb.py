from pydantic import Field

from src.etl_core.components.databases.sql_database import SQLDatabaseComponent


class MariaDBComponent(SQLDatabaseComponent):
    """Base class for MariaDB components with common functionality."""

    charset: str = Field(default="utf8mb4", description="Character set for MariaDB")
    collation: str = Field(
        default="utf8mb4_unicode_ci", description="Collation for MariaDB"
    )

    def _create_receiver(self):
        """Create the MariaDB receiver."""
        from etl_core.receivers.databases.mariadb.mariadb_receiver import (
            MariaDBReceiver,
        )

        return MariaDBReceiver(self._connection_handler)
