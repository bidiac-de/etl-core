from pydantic import Field

from etl_core.components.databases.database import DatabaseComponent


class MariaDBComponent(DatabaseComponent):
    """Base class for MariaDB components with common functionality."""

    # MariaDB-specific fields
    charset: str = Field(default="utf8mb4", description="Character set for MariaDB")
    collation: str = Field(
        default="utf8mb4_unicode_ci", description="Collation for MariaDB"
    )

    def _create_receiver(self):
        """Create the MariaDB receiver."""
        # Import here to avoid circular import
        from etl_core.receivers.data_operations_receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver

        return MariaDBReceiver(self._connection_handler)

    def _setup_connection(self):
        """Setup the MariaDB connection with credentials and specific settings."""
        super()._setup_connection()

        # Additional MariaDB-specific connection setup can go here
        # For example, setting charset and collation
        if self._connection_handler:
            try:
                # Set MariaDB-specific session variables using the connection handler
                with self._connection_handler.lease() as conn:
                    conn.execute(f"SET NAMES {self.charset}")
                    conn.execute(f"SET collation_connection = {self.collation}")
                    conn.commit()
            except Exception as e:
                # Log warning but don't fail
                print(f"Warning: Could not set MariaDB session variables: {e}")
