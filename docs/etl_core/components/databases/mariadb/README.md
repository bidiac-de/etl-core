# MariaDB ETL Components

This document describes the MariaDB ETL components that provide read and write capabilities for MariaDB databases.

## Overview

The MariaDB components follow the same pattern as other ETL components in the system:
- **Inheritance hierarchy**: `Component` → `DatabaseComponent` → `SQLDatabaseComponent` → `MariaDBComponent` → `MariaDBRead`/`MariaDBWrite`
- **Strategy support**: All three execution strategies (row, bulk, bigdata) are supported
- **Async operations**: All operations are asynchronous and support streaming
- **Credential management**: Secure credential handling through the context system
- **Clean architecture**: Separate folders for components and receivers

## Component Hierarchy

```
Component (base_component.py)
├── DatabaseComponent (database.py)
│   └── SQLDatabaseComponent (sql_database.py)
│       └── MariaDBComponent (mariadb/mariadb.py)
│           ├── MariaDBRead (mariadb/mariadb_read.py)
│           └── MariaDBWrite (mariadb/mariadb_write.py)
```

## Receiver Hierarchy

```
Receiver (base_receiver.py)
├── ReadDatabaseReceiver (read_database_receiver.py) - Abstract base
├── WriteDatabaseReceiver (write_database_receiver.py) - Abstract base
├── SQLReceiver (sql_receiver.py) - Abstract base for SQL databases
└── MariaDBReceiver (mariadb/mariadb_receiver.py) - Concrete MariaDB implementation
```

## File Structure

```
src/
├── components/databases/
│   ├── __init__.py                    # Exports all database components
│   ├── database.py                    # Base DatabaseComponent
│   ├── sql_database.py               # Base SQLDatabaseComponent
│   └── mariadb/                       # MariaDB-specific component folder
│       ├── __init__.py               # Exports MariaDB components
│       ├── mariadb.py                # MariaDBComponent (base)
│       ├── mariadb_read.py           # MariaDBRead
│       └── mariadb_write.py          # MariaDBWrite
└── receivers/databases/
    ├── __init__.py                    # Exports all database receivers
    ├── read_database_receiver.py     # Abstract ReadDatabaseReceiver
    ├── write_database_receiver.py    # Abstract WriteDatabaseReceiver
    ├── sql_receiver.py              # Abstract SQLReceiver base
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
- Enhanced performance over MySQL
- Full Unicode support (UTF8MB4)

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
- `strategy_type`: Execution strategy ("row", "bulk", "bigdata")
- `charset`: Character encoding (default: "utf8mb4")
- `collation`: Collation setting (default: "utf8mb4_unicode_ci")
- `pool_size`: Connection pool size
- `pool_timeout`: Connection timeout

**Advanced Query Features:**
- **JSON Support**: Native JSON data type handling
- **Window Functions**: Advanced analytical functions
- **Common Table Expressions**: CTE support
- **Recursive Queries**: Recursive CTE support
- **Storage Engines**: Multiple storage engine support

### MariaDBWrite

Writes data to MariaDB tables with various strategies.

**Key Features:**
- Supports insert, update, delete operations
- Configurable execution strategy (row, bulk, bigdata)
- Transaction support with rollback capability
- Batch processing for performance
- Schema validation and type conversion

**Configuration:**
```python
write_component = MariaDBWrite(
    name="write_users",
    description="Write users to MariaDB",
    comp_type="database",
    credentials_id=1,
    entity_name="users",
    if_exists_strategy="append",  # "fail", "replace", "append"
    strategy_type="bulk"
)
```

**Write Strategies:**
- `fail`: Raise error if table exists
- `replace`: Drop and recreate table
- `append`: Insert data into existing table

**Advanced Features:**
- **Upsert Operations**: INSERT ... ON DUPLICATE KEY UPDATE
- **Bulk Loading**: LOAD DATA INFILE for high-performance loading
- **JSON Handling**: Native JSON data type support
- **Storage Engine Support**: InnoDB, MyISAM, Aria, etc.
- **Constraint Handling**: Foreign key and unique constraint support

## Processing Strategies

### Row Strategy
Processes data row by row for real-time processing:

```python
# Row-by-row processing
async for row in read_component.process_row(data):
    # Process individual row
    processed_row = transform(row)
    await write_component.process_row(processed_row)
```

**Use Cases:**
- Real-time data processing
- Memory-constrained environments
- Streaming data pipelines
- Event-driven processing

### Bulk Strategy
Processes data in batches using pandas DataFrames:

```python
# Bulk processing
df = await read_component.process_bulk(data)
processed_df = transform_dataframe(df)
await write_component.process_bulk(processed_df)
```

**Use Cases:**
- Medium-sized datasets
- Batch processing
- Data analysis workflows
- ETL pipelines

### BigData Strategy
Processes large datasets using Dask DataFrames:

```python
# BigData processing
ddf = await read_component.process_bigdata(data)
processed_ddf = transform_bigdata(ddf)
await write_component.process_bigdata(processed_ddf)
```

**Use Cases:**
- Large datasets (>1GB)
- Distributed processing
- Scalable data pipelines
- Big data analytics

## Advanced MariaDB Features

### JSON Operations

**JSON Queries:**
```sql
-- Query JSON fields
SELECT id, JSON_EXTRACT(metadata, '$.status') as status 
FROM users 
WHERE JSON_EXTRACT(metadata, '$.department') = 'IT'

-- JSON path expressions
SELECT id, JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.address.city')) as city
FROM users
WHERE JSON_CONTAINS(metadata, '{"active": true}')
```

**JSON Functions:**
```sql
-- JSON aggregation
SELECT 
    department,
    JSON_ARRAYAGG(JSON_OBJECT('id', id, 'name', name)) as employees
FROM users 
GROUP BY department

-- JSON validation
SELECT * FROM products 
WHERE JSON_VALID(metadata) = 1
```

### Window Functions

**Analytical Functions:**
```sql
-- Row numbering
SELECT id, name, 
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
FROM employees

-- Running totals
SELECT id, amount,
       SUM(amount) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) as running_total
FROM transactions
```

### Common Table Expressions (CTEs)

**Recursive Queries:**
```sql
-- Hierarchical data
WITH RECURSIVE employee_hierarchy AS (
    SELECT id, name, manager_id, 1 as level
    FROM employees 
    WHERE manager_id IS NULL
    
    UNION ALL
    
    SELECT e.id, e.name, e.manager_id, eh.level + 1
    FROM employees e
    JOIN employee_hierarchy eh ON e.manager_id = eh.id
)
SELECT * FROM employee_hierarchy;
```

### Storage Engines

**Engine Selection:**
- **InnoDB**: ACID transactions, foreign keys, row-level locking
- **MyISAM**: Fast reads, full-text search, table-level locking
- **Aria**: Crash-safe MyISAM replacement
- **Memory**: In-memory storage for temporary data
- **Archive**: Compressed storage for historical data

## Performance Optimization

### Connection Pooling

**Pool Configuration:**
```python
# Connection pool settings
pool_config = {
    "pool_size": 10,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 3600,
    "pool_pre_ping": True
}
```

**Pool Monitoring:**
- Connection usage tracking
- Pool health monitoring
- Connection leak detection
- Performance metrics

### Query Optimization

**Index Usage:**
```sql
-- Create indexes for common queries
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_active ON users(active) WHERE active = 1;
CREATE INDEX idx_users_metadata ON users((CAST(metadata AS CHAR(100) ARRAY)));
```

**Query Planning:**
- EXPLAIN for query analysis
- Index usage optimization
- Join optimization
- Subquery optimization

### Batch Processing

**Bulk Insert:**
```python
# High-performance bulk insert
await write_component.bulk_insert(
    data=df,
    table="users",
    method="load_data"  # Use MariaDB LOAD DATA INFILE
)
```

**Batch Size Optimization:**
- Configurable batch sizes
- Memory usage optimization
- Transaction batching
- Error handling per batch

## Error Handling

### Connection Errors
- Automatic retry with exponential backoff
- Connection pool recovery
- Failover support
- Circuit breaker pattern

### Query Errors
- SQL syntax error handling
- Constraint violation handling
- Timeout error handling
- Resource exhaustion handling

### Transaction Errors
- Automatic rollback on failure
- Deadlock detection and handling
- Transaction timeout handling
- Isolation level management

## Security Features

### Authentication
- Multiple authentication methods
- SSL/TLS encrypted connections
- Certificate-based authentication
- LDAP integration

### Authorization
- Role-based access control
- Column-level permissions
- Audit logging
- User management

### Data Protection
- Data encryption at rest
- Data encryption in transit
- Sensitive data masking
- Compliance support (GDPR, HIPAA)

## Monitoring and Metrics

### Performance Metrics
- Query execution time
- Connection pool usage
- Transaction throughput
- Error rates
- Resource utilization

### Health Monitoring
- Database connectivity
- Query performance
- Connection health
- Resource availability
- Alert thresholds

### Logging
- Structured logging
- Query logging
- Error logging
- Audit logging
- Performance logging

## MariaDB-Specific Optimizations

### Character Set and Collation
- **UTF8MB4**: Full Unicode support including emojis
- **Unicode Collation**: Proper sorting for international data
- **Character Set Conversion**: Automatic character set handling
- **Collation Rules**: Language-specific sorting rules

### Storage Engine Features
- **InnoDB**: ACID compliance, foreign keys, row-level locking
- **MyISAM**: Fast reads, full-text search capabilities
- **Aria**: Crash-safe alternative to MyISAM
- **Memory**: High-speed temporary storage

### Performance Enhancements
- **Query Cache**: Automatic query result caching
- **Buffer Pool**: InnoDB buffer pool optimization
- **Thread Pool**: Connection thread pooling
- **Parallel Replication**: Multi-threaded replication

## Best Practices

### Query Design
- Use parameterized queries
- Optimize for specific use cases
- Use appropriate indexes
- Avoid N+1 queries
- Use EXPLAIN for analysis

### Connection Management
- Use connection pooling
- Monitor pool usage
- Handle connection errors
- Implement retry logic
- Use appropriate timeouts

### Data Handling
- Validate input data
- Use appropriate data types
- Handle NULL values
- Implement data validation
- Use transactions appropriately

### Performance
- Monitor query performance
- Use appropriate strategies
- Optimize batch sizes
- Implement caching
- Monitor resource usage

### MariaDB-Specific
- Choose appropriate storage engines
- Configure character sets properly
- Use JSON functions efficiently
- Leverage window functions
- Optimize for MariaDB features
