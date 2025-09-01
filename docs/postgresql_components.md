# PostgreSQL ETL Components

This document describes the PostgreSQL ETL components that provide read and write capabilities for PostgreSQL databases.

## Overview

The PostgreSQL components follow the same pattern as other ETL components in the system:
- **Inheritance hierarchy**: `Component` → `DatabaseComponent` → `SQLDatabaseComponent` → `PostgreSQLComponent` → `PostgreSQLRead`/`PostgreSQLWrite`
- **Strategy support**: All three execution strategies (row, bulk, bigdata) are supported
- **Async operations**: All operations are asynchronous and support streaming
- **Credential management**: Secure credential handling through the context system
- **Clean architecture**: Separate folders for components and receivers

## Component Hierarchy

```
Component (base_component.py)
├── DatabaseComponent (database.py)
│   └── SQLDatabaseComponent (sql_database.py)
│       └── PostgreSQLComponent (postgresql/postgresql.py)
│           ├── PostgreSQLRead (postgresql/postgresql_read.py)
│           └── PostgreSQLWrite (postgresql/postgresql_write.py)
```

## Receiver Hierarchy

```
Receiver (base_receiver.py)
├── ReadDatabaseReceiver (read_database_receiver.py) - Abstract base
├── WriteDatabaseReceiver (write_database_receiver.py) - Abstract base
├── SQLReceiver (sql_receiver.py) - Abstract base for SQL databases
└── PostgreSQLReceiver (postgresql/postgresql_receiver.py) - Concrete PostgreSQL implementation
```

## File Structure

```
src/
├── components/databases/
│   ├── __init__.py                    # Exports all database components
│   ├── database.py                    # Base DatabaseComponent
│   ├── sql_database.py               # Base SQLDatabaseComponent
│   └── postgresql/                    # PostgreSQL-specific component folder
│       ├── __init__.py               # Exports PostgreSQL components
│       ├── postgresql.py             # PostgreSQLComponent (base)
│       ├── postgresql_read.py        # PostgreSQLRead
│       └── postgresql_write.py       # PostgreSQLWrite
└── receivers/databases/
    ├── __init__.py                    # Exports all database receivers
    ├── read_database_receiver.py     # Abstract ReadDatabaseReceiver
    ├── write_database_receiver.py    # Abstract WriteDatabaseReceiver
    ├── sql_receiver.py              # Abstract SQLReceiver base
    └── postgresql/                    # PostgreSQL-specific receiver folder
        ├── __init__.py               # Exports PostgreSQL receivers
        └── postgresql_receiver.py    # PostgreSQLReceiver (concrete)
```

## Components

### PostgreSQLComponent (Base Class)

Base class for PostgreSQL components with PostgreSQL-specific configuration:

```python
class PostgreSQLComponent(SQLDatabaseComponent):
    charset: str = Field(default="utf8", description="Character set for PostgreSQL")
    collation: str = Field(default="en_US.UTF-8", description="Collation for PostgreSQL")
```

**Key Features:**
- Inherits from `SQLDatabaseComponent`
- Configurable character set and collation
- PostgreSQL-specific optimizations
- Automatic database type detection

### PostgreSQLRead

Reads data from PostgreSQL tables using SQL queries.

**Key Features:**
- Supports parameterized queries with SQLAlchemy text()
- Configurable execution strategy (row, bulk, bigdata)
- Streaming row-by-row processing
- Bulk DataFrame operations
- Big data support with Dask DataFrames

**Configuration:**
```python
read_component = PostgreSQLRead(
    name="read_users",
    description="Read users from PostgreSQL",
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
- `charset`: Character set (default: "utf8")
- `collation`: Collation (default: "en_US.UTF-8")

### PostgreSQLWrite

Writes data to PostgreSQL tables with support for various insert strategies.

**Key Features:**
- Row-by-row insertion with custom queries
- Bulk insertion using pandas.to_sql()
- Big data support with Dask DataFrames
- Automatic INSERT query generation
- Custom query support for complex operations
- PostgreSQL-specific optimizations

**Configuration:**
```python
write_component = PostgreSQLWrite(
    name="write_users",
    description="Write users to PostgreSQL",
    comp_type="database",
    credentials_id=1,
    entity_name="users",
    strategy_type="bulk",
    query="INSERT INTO users (name, email) VALUES (:name, :email) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name"
)
```

**Required Fields:**
- `credentials_id`: ID of credentials in context
- `entity_name`: Target table name

**Optional Fields:**
- `query`: Custom INSERT/UPDATE query (optional)
- `strategy_type`: Execution strategy (default: "bulk")
- `charset`: Character set (default: "utf8")
- `collation`: Collation (default: "en_US.UTF-8")

## Execution Strategies

### Row Strategy
- **PostgreSQLRead**: Streams individual rows as dictionaries
- **PostgreSQLWrite**: Executes custom query or generates INSERT for each row
- **Best for**: Small datasets, real-time processing, custom row logic
- **Memory usage**: Low (one row at a time)

### Bulk Strategy
- **PostgreSQLRead**: Returns pandas DataFrame with query results
- **PostgreSQLWrite**: Uses pandas.to_sql() for efficient bulk insertion
- **Best for**: Medium-sized datasets, pandas operations
- **Memory usage**: Medium (chunked processing)

### Big Data Strategy
- **PostgreSQLRead**: Returns Dask DataFrame with query results
- **PostgreSQLWrite**: Converts Dask to pandas, then uses to_sql()
- **Best for**: Large datasets, distributed processing
- **Memory usage**: Low (lazy evaluation)

## PostgreSQLReceiver Implementation

The `PostgreSQLReceiver` handles the actual database operations:

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
- PostgreSQL-optimized chunk size (1000)
- Supports custom queries for each row
- Batch processing with configurable sizes
- Automatic transaction management

**write_bigdata():**
- Converts Dask DataFrame to pandas
- Processes in chunks
- Supports both custom and auto-generated queries
- PostgreSQL-optimized performance

## PostgreSQL-Specific Features

### 1. Character Set and Collation
- **Default charset**: `utf8` (PostgreSQL standard)
- **Default collation**: `en_US.UTF-8`
- **Session variables**: `client_encoding` and `lc_collate`

### 2. Connection Handling
- **Driver**: `postgresql+psycopg2`
- **Connection pooling**: Automatic management
- **Transaction handling**: ACID compliance

### 3. Query Optimizations
- **Parameterized queries**: SQL injection prevention
- **Batch processing**: Optimized chunk sizes
- **Connection reuse**: Efficient resource management

### 4. PostgreSQL-Specific SQL
- **ON CONFLICT**: Native upsert support
- **RETURNING**: Get inserted/updated data
- **JSON operators**: Native JSON support
- **Array types**: PostgreSQL array handling

## Usage Examples

### Basic Read Operation
```python
from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead

# Create read component
read_comp = PostgreSQLRead(
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
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite

# Create write component
write_comp = PostgreSQLWrite(
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

### Custom Query Write Operation with Upsert
```python
# Create write component with PostgreSQL-specific upsert
write_comp = PostgreSQLWrite(
    name="upsert_users",
    comp_type="database",
    credentials_id=1,
    entity_name="users",
    query="""
        INSERT INTO users (id, name, email)
        VALUES (:id, :name, :email)
        ON CONFLICT (email) DO UPDATE SET
        name = EXCLUDED.name, updated_at = NOW()
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
    port=5432  # PostgreSQL default port
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
- Support for PostgreSQL-specific settings (charset, collation)
- Proper cleanup and transaction management

## Import Paths

### Components
```python
# From outside the module
from etl_core.components.databases.postgresql.postgresql_read import PostgreSQLRead
from etl_core.components.databases.postgresql.postgresql_write import PostgreSQLWrite

# Within the databases module
from .postgresql.postgresql_read import PostgreSQLRead
from .postgresql.postgresql_write import PostgreSQLWrite
```

### Receivers
```python
# From outside the module
from etl_core.receivers.databases.postgresql.postgresql_receiver import PostgreSQLReceiver

# Within the databases module
from .postgresql.postgresql_receiver import PostgreSQLReceiver
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

### PostgreSQL-Specific Optimizations
- **Character set**: Configurable charset and collation
- **Batch inserts**: Efficient bulk insertion with chunk size 1000
- **Connection settings**: PostgreSQL-optimized connection parameters
- **Native upsert**: ON CONFLICT support for efficient updates

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

### 5. PostgreSQL-Specific Considerations
- **Port**: Default port 5432 (vs. MariaDB 3306)
- **Session variables**: Different syntax for charset/collation
- **Upsert syntax**: ON CONFLICT instead of ON DUPLICATE KEY
- **Data types**: PostgreSQL-specific type handling

## Best Practices

### 1. Query Design
- Use parameterized queries for dynamic data
- Keep queries simple and focused
- Use appropriate indexes on target tables
- Test queries before production use
- Leverage PostgreSQL-specific features (JSON, arrays, etc.)

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
- Leverage PostgreSQL's native upsert capabilities

### 4. Error Prevention
- Validate data types before writing
- Handle missing columns gracefully
- Test with sample data before production
- Monitor database performance metrics
- Use appropriate PostgreSQL data types

## Troubleshooting

### Common Issues

**1. Connection errors:**
- Verify credentials in context
- Check network connectivity
- Verify database server status
- Check firewall settings
- Verify PostgreSQL is running on port 5432

**2. Query execution errors:**
- Validate SQL syntax (PostgreSQL-specific)
- Check table/column existence
- Verify parameter types
- Test query in psql client
- Check for PostgreSQL-specific syntax requirements

**3. Performance issues:**
- Switch to appropriate strategy
- Optimize batch sizes
- Check database indexes
- Monitor connection pool usage
- Use PostgreSQL-specific optimizations

**4. Memory issues:**
- Reduce batch sizes
- Use row strategy for large datasets
- Monitor DataFrame sizes
- Consider chunked processing

### Debugging Tips

1. **Enable logging**: Check component and receiver logs
2. **Test queries**: Verify SQL syntax in psql client
3. **Monitor metrics**: Track processing times and row counts
4. **Check connections**: Verify connection pool status
5. **Validate data**: Test with small datasets first
6. **PostgreSQL logs**: Check PostgreSQL server logs for errors

## PostgreSQL vs. MariaDB Differences

### 1. Connection Details
- **Port**: PostgreSQL (5432) vs. MariaDB (3306)
- **Driver**: psycopg2 vs. mysqlconnector
- **URL format**: postgresql+psycopg2 vs. mysql+mysqlconnector

### 2. Session Variables
- **Charset**: `client_encoding` vs. `SET NAMES`
- **Collation**: `lc_collate` vs. `collation_connection`

### 3. SQL Syntax
- **Upsert**: `ON CONFLICT` vs. `ON DUPLICATE KEY UPDATE`
- **Returning**: Native support vs. `SELECT LAST_INSERT_ID()`
- **Data types**: Rich type system vs. MySQL types

### 4. Performance Characteristics
- **Bulk operations**: Optimized chunk sizes (1000)
- **Connection handling**: Similar pooling strategies
- **Transaction management**: ACID compliance in both

## Conclusion

The PostgreSQL components provide a robust and efficient solution for database operations in ETL pipelines. By understanding the component hierarchy, execution strategies, and PostgreSQL-specific features, you can create reliable and performant database operations.

The architecture separates concerns between components (configuration) and receivers (implementation), making it easy to maintain and extend. The support for multiple execution strategies allows you to choose the optimal approach for your specific use case.

PostgreSQL-specific features like native upsert support, rich data types, and optimized bulk operations make it an excellent choice for complex ETL workflows.

For advanced usage and custom implementations, refer to the `PostgreSQLReceiver` and `SQLConnectionHandler` modules for detailed implementation details.
