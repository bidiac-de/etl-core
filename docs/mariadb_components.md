# MariaDB ETL Components

This document describes the MariaDB ETL components that provide read and write capabilities for MariaDB databases.

## Overview

The MariaDB components follow the same pattern as other ETL components in the system:
- **Inheritance hierarchy**: `Component` → `DatabaseComponent` → `MariaDBComponent` → `MariaDBRead`/`MariaDBWrite`
- **Strategy support**: All three execution strategies (row, bulk, bigdata) are supported
- **Async operations**: All operations are asynchronous and support streaming
- **Credential management**: Secure credential handling through the context system
- **Clean architecture**: Separate folders for components and receivers

## Component Hierarchy

```
Component (base_component.py)
├── DatabaseComponent (database.py)
│   └── MariaDBComponent (mariadb/mariadb.py)
│       ├── MariaDBRead (mariadb/mariadb_read.py)
│       └── MariaDBWrite (mariadb/mariadb_write.py)
```

## Receiver Hierarchy

```
Receiver (base_receiver.py)
├── ReadDatabaseReceiver (mariadb/read_database_receiver.py) - Abstract
├── WriteDatabaseReceiver (mariadb/write_database_receiver.py) - Abstract
└── MariaDBReceiver (mariadb/mariadb_receiver.py) - Concrete implementation
```

## File Structure

```
src/
├── components/databases/
│   ├── __init__.py                    # Exports all database components
│   ├── database.py                    # Base DatabaseComponent
│   └── mariadb/                       # MariaDB-specific component folder
│       ├── __init__.py               # Exports MariaDB components
│       ├── mariadb.py                # MariaDBComponent (base)
│       ├── mariadb_read.py           # MariaDBRead
│       └── mariadb_write.py          # MariaDBWrite
└── receivers/databases/
    ├── __init__.py                    # Exports all database receivers
    └── mariadb/                       # MariaDB-specific receiver folder
        ├── __init__.py               # Exports MariaDB receivers
        ├── mariadb_receiver.py       # MariaDBReceiver (concrete)
        ├── read_database_receiver.py # ReadDatabaseReceiver (abstract)
        └── write_database_receiver.py # WriteDatabaseReceiver (abstract)
```

## Components

### MariaDBRead

Reads data from MariaDB tables using SQL queries.

**Key Features:**
- Supports parameterized queries
- Configurable execution strategy
- Streaming row-by-row processing
- Bulk DataFrame operations
- Big data support with Dask

**Configuration:**
```python
read_component = MariaDBRead(
    name="read_users",
    description="Read users from MariaDB",
    comp_type="database",
    host="localhost",
    port=3306,
    database="etl_database",
    table="users",
    query="SELECT id, name, email FROM users WHERE active = 1",
    params={"active": 1},
    strategy_type="bulk"  # "row", "bulk", or "bigdata"
)
```

### MariaDBWrite

Writes data to MariaDB tables with support for various insert strategies.

**Key Features:**
- Row-by-row insertion
- Bulk insertion with configurable batch sizes
- Big data support with Dask DataFrames
- ON DUPLICATE KEY UPDATE support
- Automatic connection management

**Configuration:**
```python
write_component = MariaDBWrite(
    name="write_users",
    description="Write users to MariaDB",
    comp_type="database",
    host="localhost",
    port=3306,
    database="etl_database",
    table="users",
    strategy_type="bulk",
    batch_size=1000,
    on_duplicate_key_update=["name", "email"]
)
```

## Execution Strategies

### Row Strategy
- Processes data row-by-row
- Good for small datasets or when memory is limited
- Supports streaming operations

### Bulk Strategy
- Processes data in batches
- Good for medium-sized datasets
- Returns pandas DataFrames

### Big Data Strategy
- Uses Dask DataFrames for large datasets
- Supports partitioning and parallel processing
- Good for datasets that don't fit in memory

## Usage Examples

### Basic Read Operation
```python
# Create read component
read_comp = MariaDBRead(
    name="read_data",
    comp_type="database",
    host="localhost",
    port=3306,
    database="test_db",
    table="test_table",
    query="SELECT * FROM test_table WHERE status = :status",
    params={"status": "active"},
    strategy_type="bulk"
)

# Set context and credentials
read_comp.context = context

# Read data
df = await read_comp.process_bulk(None, metrics)
```

### Basic Write Operation
```python
# Create write component
write_comp = MariaDBWrite(
    name="write_data",
    comp_type="database",
    host="localhost",
    port=3306,
    database="test_db",
    table="output_table",
    strategy_type="bulk"
)

# Set context and credentials
write_comp.context = context

# Write data
await write_comp.process_bulk(data, metrics)
```

### Streaming Row Processing
```python
# Use row strategy for streaming
read_comp.strategy_type = "row"

async for row in read_comp.process_row(None, metrics):
    # Process each row individually
    processed_row = await process_row(row)
    
    # Write processed row
    await write_comp.process_row(processed_row, metrics)
```

## Credential Management

The components use the context system for secure credential management:

```python
from src.context.context import Context
from src.context.credentials import Credentials

# Create context
context = Context()

# Add credentials
credentials = Credentials(
    credentials_id=1,
    user="etl_user",
    password="secure_password",
    database="etl_database"
)
context.add_credentials(credentials)

# Set context on components
read_comp.context = context
write_comp.context = context
```

## Connection Management

Connections are automatically managed by the components:
- Created when components are instantiated
- Updated when credentials are set
- Automatically closed when components are destroyed
- Support for MariaDB-specific settings (charset, collation)

## Import Paths

### Components
```python
# From outside the module
from src.components.databases.mariadb import MariaDBRead, MariaDBWrite

# Within the databases module
from .mariadb import MariaDBRead, MariaDBWrite
```

### Receivers
```python
# From outside the module
from src.receivers.databases.mariadb import MariaDBReceiver

# Within the databases module
from .mariadb import MariaDBReceiver
```

## Error Handling

The components include comprehensive error handling:
- Connection validation
- Query execution error handling
- Credential validation
- Automatic cleanup on errors

## Performance Considerations

- **Row strategy**: Best for small datasets, streaming operations
- **Bulk strategy**: Best for medium datasets, memory-efficient
- **Big data strategy**: Best for large datasets, supports parallel processing
- **Batch sizes**: Configurable for write operations
- **Connection pooling**: Automatic connection management

## Security Features

- Encrypted credential storage
- Parameterized queries (prevents SQL injection)
- Secure connection handling
- Automatic connection cleanup

## Architecture Benefits

The new folder structure provides several benefits:
- **Clear separation**: MariaDB-specific code is isolated
- **Easy maintenance**: Simple to add new database types
- **Consistent pattern**: Follows the same structure as file components
- **Clean imports**: Relative imports within modules
- **Scalable**: Easy to add PostgreSQL, MongoDB, etc. in the future
