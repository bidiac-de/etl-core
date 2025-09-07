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
- `strategy_type`: Execution strategy ("row", "bulk", "bigdata")
- `charset`: Character encoding (default: "utf8")
- `collation`: Collation setting (default: "en_US.UTF-8")
- `pool_size`: Connection pool size
- `pool_timeout`: Connection timeout

**Advanced Query Features:**
- **JSON/JSONB Support**: Native JSON data type handling
- **Array Operations**: PostgreSQL array data types
- **Range Types**: Date, numeric, and text range types
- **Custom Data Types**: PostgreSQL-specific data types
- **Extensions**: PostGIS, Full-Text Search, UUID support

### PostgreSQLWrite

Writes data to PostgreSQL tables with various strategies.

**Key Features:**
- Supports insert, update, delete operations
- Configurable execution strategy (row, bulk, bigdata)
- Transaction support with rollback capability
- Batch processing for performance
- Schema validation and type conversion

**Configuration:**
```python
write_component = PostgreSQLWrite(
    name="write_users",
    description="Write users to PostgreSQL",
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
- **Upsert Operations**: INSERT ... ON CONFLICT support
- **Bulk Loading**: COPY command for high-performance loading
- **JSON Handling**: Native JSON/JSONB support
- **Array Support**: PostgreSQL array data types
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

## Advanced PostgreSQL Features

### JSON/JSONB Operations

**JSON Queries:**
```sql
-- Query JSON fields
SELECT id, metadata->>'status' as status 
FROM users 
WHERE metadata->>'department' = 'IT'

-- JSON path expressions
SELECT id, metadata#>>'{address,city}' as city
FROM users
WHERE metadata @> '{"active": true}'
```

**JSON Aggregation:**
```sql
-- JSON aggregation functions
SELECT 
    department,
    json_agg(json_build_object('id', id, 'name', name)) as employees
FROM users 
GROUP BY department
```

### Array Operations

**Array Queries:**
```sql
-- Array contains
SELECT * FROM products WHERE tags @> ARRAY['electronics']

-- Array length
SELECT * FROM products WHERE array_length(tags, 1) > 3

-- Array operations
SELECT id, unnest(tags) as tag FROM products
```

### Range Types

**Range Queries:**
```sql
-- Date ranges
SELECT * FROM events 
WHERE event_date <@ daterange('2024-01-01', '2024-12-31')

-- Numeric ranges
SELECT * FROM products 
WHERE price <@ numrange(100, 500)
```

### Full-Text Search

**Text Search:**
```sql
-- Full-text search
SELECT * FROM articles 
WHERE to_tsvector('english', content) @@ to_tsquery('english', 'database & performance')

-- Search ranking
SELECT *, ts_rank(to_tsvector('english', content), query) as rank
FROM articles, to_tsquery('english', 'search terms') query
WHERE to_tsvector('english', content) @@ query
ORDER BY rank DESC
```

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
CREATE INDEX idx_users_active ON users(active) WHERE active = true;
CREATE INDEX idx_users_metadata_gin ON users USING gin(metadata);
```

**Query Planning:**
- EXPLAIN ANALYZE for query analysis
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
    method="copy"  # Use PostgreSQL COPY command
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
- Row-level security (RLS)
- Column-level permissions
- Audit logging

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

## Best Practices

### Query Design
- Use parameterized queries
- Optimize for specific use cases
- Use appropriate indexes
- Avoid N+1 queries
- Use EXPLAIN ANALYZE

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
