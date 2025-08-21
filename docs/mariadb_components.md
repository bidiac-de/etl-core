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
├── SQLReceiver (sql_receiver.py) - Abstract base for SQL databases
└── MariaDBReceiver (mariadb/mariadb_receiver.py) - Concrete MariaDB implementation
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
    ├── sql_receiver.py               # Abstract SQLReceiver base
    └── mariadb/                       # MariaDB-specific receiver folder
        ├── __init__.py               # Exports MariaDB receivers
        └── mariadb_receiver.py       # MariaDBReceiver (concrete)
```

## Components

### MariaDBComponent (Base Class)

Base class for MariaDB components with MariaDB-specific configuration:

```python
class MariaDBComponent(SQLDatabaseComponent):
    charset: str = Field(default="utf8mb4", description="Character set for MariaDB")
    collation: str = Field(default="utf8mb4_unicode_ci", description="Collation for MariaDB")
```

**Key Features:**
- Inherits from `SQLDatabaseComponent`
- Configurable character set and collation
- MariaDB-specific optimizations

### MariaDBRead

Reads data from MariaDB tables using SQL queries.

**Key Features:**
- Supports parameterized queries with SQLAlchemy text()
- Configurable execution strategy (row, bulk, bigdata)
- Streaming row-by-row processing
- Bulk DataFrame operations
- Big data support with Dask DataFrames

**Configuration:**
```python
read_component = MariaDBRead(
    name="read_users",
    description="Read users from MariaDB",
    comp_type="database",
    credentials_id=1,
    entity_name="users",
    query="SELECT id, name, email FROM users WHERE active = :active",
    params={"active": 1},
    strategy_type="bulk"  # "row", "bulk", or "bigdata"
)
```

**Required Fields:**
- `credentials_id`: ID of credentials in context
- `entity_name`: Target table name
- `query`: SQL query to execute
- `params`: Query parameters (optional)

**Optional Fields:**
- `strategy_type`: Execution strategy (default: "bulk")
- `charset`: Character set (default: "utf8mb4")
- `collation`: Collation (default: "utf8mb4_unicode_ci")

### MariaDBWrite

Writes data to MariaDB tables with support for various insert strategies.

**Key Features:**
- Row-by-row insertion with custom queries
- Bulk insertion using pandas.to_sql()
- Big data support with Dask DataFrames
- Automatic INSERT query generation
- Custom query support for complex operations

**Configuration:**
```python
write_component = MariaDBWrite(
    name="write_users",
    description="Write users to MariaDB",
    comp_type="database",
    credentials_id=1,
    entity_name="users",
    strategy_type="bulk",
    query="INSERT INTO users (name, email) VALUES (:name, :email) ON DUPLICATE KEY UPDATE email = :email"
)
```

**Required Fields:**
- `credentials_id`: ID of credentials in context
- `entity_name`: Target table name

**Optional Fields:**
- `query`: Custom INSERT/UPDATE query (optional)
- `strategy_type`: Execution strategy (default: "bulk")
- `charset`: Character set (default: "utf8mb4")
- `collation`: Collation (default: "utf8mb4_unicode_ci")

## Execution Strategies

### Row Strategy
- **MariaDBRead**: Streams individual rows as dictionaries
- **MariaDBWrite**: Executes custom query or generates INSERT for each row
- **Best for**: Small datasets, real-time processing, custom row logic
- **Memory usage**: Low (one row at a time)

### Bulk Strategy
- **MariaDBRead**: Returns pandas DataFrame with query results
- **MariaDBWrite**: Uses pandas.to_sql() for efficient bulk insertion
- **Best for**: Medium-sized datasets, pandas operations
- **Memory usage**: Medium (chunked processing)

### Big Data Strategy
- **MariaDBRead**: Returns Dask DataFrame with query results
- **MariaDBWrite**: Converts Dask to pandas, then uses to_sql()
- **Best for**: Large datasets, distributed processing
- **Memory usage**: Low (lazy evaluation)

## MariaDBReceiver Implementation

The `MariaDBReceiver` handles the actual database operations:

### Read Operations

**read_row():**
- Executes query using SQLAlchemy text()
- Yields rows as dictionaries
- Supports parameterized queries
- Uses connection pooling

**read_bulk():**
- Returns pandas DataFrame
- Efficient for medium datasets
- Supports custom queries and parameters

**read_bigdata():**
- Returns Dask DataFrame
- Converts pandas result to Dask
- Single partition by default

### Write Operations

**write_row():**
- Supports custom INSERT queries
- Auto-generates INSERT if no query provided
- Commits after each row
- Returns affected rows count

**write_bulk():**
- Uses pandas.to_sql() for efficiency
- Supports custom queries for each row
- Batch processing with configurable sizes
- Automatic transaction management

**write_bigdata():**
- Converts Dask DataFrame to pandas
- Processes in chunks
- Supports both custom and auto-generated queries

## Usage Examples

### Basic Read Operation
```python
from etl_core.components.databases.mariadb.mariadb_read import MariaDBRead

# Create read component
read_comp = MariaDBRead(
    name="read_data",
    comp_type="database",
    credentials_id=1,
    entity_name="users",
    query="SELECT * FROM users WHERE status = :status",
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
from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite

# Create write component
write_comp = MariaDBWrite(
    name="write_data",
    comp_type="database",
    credentials_id=1,
    entity_name="users",
    strategy_type="bulk"
)

# Set context and credentials
write_comp.context = context

# Write data
result = await write_comp.process_bulk(data, metrics)
```

### Custom Query Write Operation
```python
# Create write component with custom query
write_comp = MariaDBWrite(
    name="upsert_users",
    comp_type="database",
    credentials_id=1,
    entity_name="users",
    query="""
        INSERT INTO users (id, name, email) 
        VALUES (:id, :name, :email) 
        ON DUPLICATE KEY UPDATE 
        name = VALUES(name), email = VALUES(email)
    """,
    strategy_type="bulk"
)

# Write with upsert logic
result = await write_comp.process_bulk(data, metrics)
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
from etl_core.context.context import Context
from etl_core.context.credentials import Credentials

# Create context
context = Context()

# Add credentials
credentials = Credentials(
    credentials_id=1,
    user="etl_user",
    password="secure_password",
    database="etl_database",
    host="localhost",
    port=3306
)
context.add_credentials(credentials)

# Set context on components
read_comp.context = context
write_comp.context = context
```

## Connection Management

Connections are automatically managed by the `SQLConnectionHandler`:
- Created when components are instantiated
- Updated when credentials are set
- Automatic connection pooling
- Support for MariaDB-specific settings (charset, collation)
- Proper cleanup and transaction management

## Import Paths

### Components
```python
# From outside the module
from etl_core.components.databases.mariadb.mariadb_read import MariaDBRead
from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite

# Within the databases module
from .mariadb.mariadb_read import MariaDBRead
from .mariadb.mariadb_write import MariaDBWrite
```

### Receivers
```python
# From outside the module
from etl_core.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver

# Within the databases module
from .mariadb.mariadb_receiver import MariaDBReceiver
```

## Error Handling

The components include comprehensive error handling:
- Connection validation through context system
- Query execution error handling with SQLAlchemy
- Credential validation
- Automatic cleanup on errors
- Transaction rollback on failures

## Performance Considerations

### Strategy Selection
- **Row strategy**: Best for small datasets, streaming operations
- **Bulk strategy**: Best for medium datasets, uses pandas.to_sql()
- **Big data strategy**: Best for large datasets, converts to pandas for efficiency

### Optimization Features
- **Connection pooling**: Automatic connection management
- **Batch processing**: Configurable batch sizes for bulk operations
- **Parameterized queries**: Prevents SQL injection and improves performance
- **Transaction management**: Automatic commit/rollback

### MariaDB-Specific Optimizations
- **Character set**: Configurable charset and collation
- **Batch inserts**: Efficient bulk insertion
- **Connection settings**: MariaDB-optimized connection parameters

## Security Features

- **Encrypted credential storage** through context system
- **Parameterized queries** (prevents SQL injection)
- **Secure connection handling** with SSL support
- **Automatic connection cleanup**
- **Transaction isolation**

## Edge Cases and Special Behaviors

### 1. Empty DataFrames
- **Read**: Returns empty DataFrame (no rows to process)
- **Write**: Skips processing, returns empty DataFrame
- **No errors thrown**

### 2. Custom Queries vs. Auto-Generated
- **Custom query provided**: Uses exact query with parameters
- **No custom query**: Auto-generates INSERT INTO statement
- **Fallback behavior**: Always available for write operations

### 3. Dask DataFrame Handling
- **Read**: Converts to Dask with single partition
- **Write**: Computes to pandas before processing
- **Memory considerations**: Large DataFrames are materialized

### 4. Connection Failures
- **Automatic retry**: Connection handler manages retries
- **Graceful degradation**: Clear error messages
- **Resource cleanup**: Connections properly closed

## Best Practices

### 1. Query Design
- Use parameterized queries for dynamic data
- Keep queries simple and focused
- Use appropriate indexes on target tables
- Test queries before production use

### 2. Strategy Selection
- Use row strategy for streaming and small datasets
- Use bulk strategy for medium datasets
- Use bigdata strategy for large datasets that fit in memory
- Consider memory usage vs. performance

### 3. Performance Optimization
- Use appropriate batch sizes for bulk operations
- Monitor connection pool usage
- Use transactions for multiple related operations
- Consider query complexity vs. data size

### 4. Error Prevention
- Validate data types before writing
- Handle missing columns gracefully
- Test with sample data before production
- Monitor database performance metrics

## Troubleshooting

### Common Issues

**1. Connection errors:**
- Verify credentials in context
- Check network connectivity
- Verify database server status
- Check firewall settings

**2. Query execution errors:**
- Validate SQL syntax
- Check table/column existence
- Verify parameter types
- Test query in database client

**3. Performance issues:**
- Switch to appropriate strategy
- Optimize batch sizes
- Check database indexes
- Monitor connection pool usage

**4. Memory issues:**
- Reduce batch sizes
- Use row strategy for large datasets
- Monitor DataFrame sizes
- Consider chunked processing

### Debugging Tips

1. **Enable logging**: Check component and receiver logs
2. **Test queries**: Verify SQL syntax in database client
3. **Monitor metrics**: Track processing times and row counts
4. **Check connections**: Verify connection pool status
5. **Validate data**: Test with small datasets first

## Conclusion

The MariaDB components provide a robust and efficient solution for database operations in ETL pipelines. By understanding the component hierarchy, execution strategies, and best practices, you can create reliable and performant database operations.

The architecture separates concerns between components (configuration) and receivers (implementation), making it easy to maintain and extend. The support for multiple execution strategies allows you to choose the optimal approach for your specific use case.

For advanced usage and custom implementations, refer to the `MariaDBReceiver` and `SQLConnectionHandler` modules for detailed implementation details.
