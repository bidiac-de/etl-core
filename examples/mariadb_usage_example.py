"""
Example usage of MariaDB ETL components.

This example demonstrates how to use the MariaDB read and write components
with different execution strategies.
"""

import asyncio
from src.components.databases.mariadb import MariaDBRead, MariaDBWrite
from src.context.context import Context
from src.context.credentials import Credentials
from src.metrics.component_metrics.component_metrics import ComponentMetrics


async def example_mariadb_etl():
    """Example of MariaDB ETL operations."""

    # Create context and credentials
    context = Context()
    credentials = Credentials(
        credentials_id=1,
        user="etl_user",
        password="secure_password",
        database="etl_database",
    )
    context.add_credentials(credentials)

    # Create MariaDB Read component
    read_component = MariaDBRead(
        name="read_users",
        description="Read users from MariaDB",
        comp_type="database",
        host="localhost",
        port=3306,
        database="etl_database",
        table="users",
        query="SELECT id, name, email FROM users WHERE active = 1",
        params={},
        strategy_type="bulk",  # Can be "row", "bulk", or "bigdata"
        schema=None,  # Will be set automatically
    )

    # Create MariaDB Write component
    write_component = MariaDBWrite(
        name="write_processed_users",
        description="Write processed users to MariaDB",
        comp_type="database",
        host="localhost",
        port=3306,
        database="etl_database",
        table="processed_users",
        strategy_type="bulk",
        batch_size=1000,
        on_duplicate_key_update=["name", "email"],
        schema=None,  # Will be set automatically
    )

    # Set context for both components
    read_component.context = context
    write_component.context = context

    # Create mock metrics
    metrics = ComponentMetrics()

    print("=== MariaDB ETL Example ===")
    print(f"Read Component: {read_component.name}")
    print(f"Write Component: {write_component.name}")
    print(f"Strategy: {read_component.strategy_type}")

    # Example of using the components
    try:
        # Read data using bulk strategy
        print("\n--- Reading data ---")
        df = await read_component.process_bulk(None, metrics)
        print(f"Read {len(df)} rows")
        print(f"Columns: {list(df.columns)}")

        # Write data using bulk strategy
        print("\n--- Writing data ---")
        await write_component.process_bulk(df.to_dict("records"), metrics)
        print("Data written successfully")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Cleanup connections
        if hasattr(read_component, "_connection_handler"):
            read_component._connection_handler.close()
        if hasattr(write_component, "_connection_handler"):
            write_component._connection_handler.close()


if __name__ == "__main__":
    # Run the example
    asyncio.run(example_mariadb_etl())
