from pydantic import Field

from etl_core.components.databases.sql_database import SQLDatabaseComponent, IfExistsStrategy


class PostgreSQLIfExistsStrategy(IfExistsStrategy):
    """PostgreSQL-specific strategies for handling existing tables."""
    ON_CONFLICT_DO_NOTHING = "on_conflict_do_nothing"  # PostgreSQL: ON CONFLICT DO NOTHING
    ON_CONFLICT_UPDATE = "on_conflict_update"           # PostgreSQL: ON CONFLICT DO UPDATE


class PostgreSQLComponent(SQLDatabaseComponent):
    """Base class for PostgreSQL components with common functionality."""

    charset: str = Field(default="utf8", description="Character set for PostgreSQL")
    collation: str = Field(
        default="en_US.UTF-8", description="Collation for PostgreSQL"
    )
    
    # PostgreSQL-spezifische if_exists Strategie mit intelligentem Standard
    if_exists: PostgreSQLIfExistsStrategy = Field(
        default=PostgreSQLIfExistsStrategy.APPEND, 
        description="PostgreSQL-specific behavior for existing tables (APPEND by default, but ON_CONFLICT strategies available)"
    )

    def _build_upsert_query(self, table: str, columns: list, if_exists: PostgreSQLIfExistsStrategy, **kwargs) -> str:
        """
        Build PostgreSQL-specific UPSERT query based on if_exists strategy.
        
        Args:
            table: Target table name
            columns: List of column names
            if_exists: PostgreSQL-specific strategy for handling existing data
            **kwargs: Additional parameters (e.g., conflict_columns, update_columns for ON CONFLICT)
            
        Returns:
            SQL query string
        """
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        base_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        if if_exists == PostgreSQLIfExistsStrategy.ON_CONFLICT_DO_NOTHING:
            # PostgreSQL: ON CONFLICT DO NOTHING
            conflict_columns = kwargs.get('conflict_columns', ['id'])  # Default to 'id' column
            conflict_clause = ", ".join(conflict_columns)
            return f"{base_query} ON CONFLICT ({conflict_clause}) DO NOTHING"
        elif if_exists == PostgreSQLIfExistsStrategy.ON_CONFLICT_UPDATE:
            # PostgreSQL: ON CONFLICT DO UPDATE
            conflict_columns = kwargs.get('conflict_columns', ['id'])  # Default to 'id' column
            update_columns = kwargs.get('update_columns', columns)
            conflict_clause = ", ".join(conflict_columns)
            update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            return f"{base_query} ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}"
        else:
            # Fall back to base implementation for standard strategies
            return super()._build_upsert_query(table, columns, if_exists, **kwargs)

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
        super()._build_objects()
        # Set session variables after connection is established
        self._setup_session_variables()
        return self


