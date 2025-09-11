# MariaDB Components

The MariaDB components provide **enterprise-grade database integration** with MariaDB databases with comprehensive support for different processing strategies and advanced SQL features.

## Overview

The MariaDB components provide enterprise-grade database integration with MariaDB, offering **MySQL compatibility**, **advanced features**, and **high performance** for data processing scenarios. They support row, bulk, and bigdata processing strategies with comprehensive SQL feature support.

## Components

### Database Functions
- **MariaDB Read**: Read data from MariaDB tables using SQL queries
- **MariaDB Write**: Write data to MariaDB tables with various strategies
- **Connection Management**: Efficient connection pooling and management
- **Transaction Support**: Full ACID transaction support

## Database Types

### MariaDB Read
Reads data from MariaDB tables using SQL queries with support for all processing strategies.

#### Features
- **SQL Query Support**: Execute complex SQL queries with parameters
- **JSON Support**: Native JSON data type handling and functions
- **Window Functions**: Advanced analytical functions
- **Common Table Expressions**: CTE support for complex queries
- **Recursive Queries**: Recursive CTE support for hierarchical data

### MariaDB Write
Writes data to MariaDB tables with various strategies and transaction support.

#### Features
- **Write Strategies**: Fail, replace, and append strategies
- **Upsert Operations**: INSERT ... ON DUPLICATE KEY UPDATE
- **Bulk Loading**: LOAD DATA INFILE for high-performance loading
- **JSON Handling**: Native JSON data type support
- **Storage Engine Support**: InnoDB, MyISAM, Aria, Memory, Archive engines

### Connection Management
Manages database connections with pooling and optimization.

#### Features
- **Connection Pooling**: Efficient connection reuse and management
- **Health Monitoring**: Real-time connection health monitoring
- **Automatic Retry**: Intelligent retry mechanisms for failures
- **Credential Security**: Secure credential handling from context

## Processing Strategies

### Row Strategy
Processes data row by row for real-time processing.

**Use Cases:**
- Real-time data processing
- Memory-constrained environments
- Streaming data pipelines
- Event-driven processing

### Bulk Strategy
Processes data in batches using pandas DataFrames.

**Use Cases:**
- Medium-sized datasets
- Batch processing
- Data analysis workflows
- ETL pipelines

### BigData Strategy
Processes large datasets using Dask DataFrames.

**Use Cases:**
- Large datasets (>1GB)
- Distributed processing
- Scalable data pipelines
- Big data analytics

## JSON Configuration Examples

### Simple Read Configuration

```json
{
  "name": "read_products",
  "comp_type": "database",
  "credentials_id": 2,
  "entity_name": "products",
  "query": "SELECT * FROM products WHERE category = :category",
  "params": {
    "category": "electronics"
  },
  "strategy_type": "bulk"
}
```

### Complex Read with JSON Support

```json
{
  "name": "read_user_metadata",
  "comp_type": "database",
  "credentials_id": 1,
  "entity_name": "users",
  "query": "SELECT id, name, JSON_EXTRACT(metadata, '$.department') as department FROM users WHERE JSON_CONTAINS(metadata, '{\"active\": true}')",
  "strategy_type": "bulk"
}
```

### Write with Upsert Support

```json
{
  "name": "upsert_orders",
  "comp_type": "database",
  "credentials_id": 3,
  "entity_name": "orders",
  "if_exists_strategy": "append",
  "strategy_type": "bulk",
  "upsert_columns": ["order_id"],
  "update_columns": ["status", "updated_at"]
}
```

### BigData Processing Configuration

```json
{
  "name": "bigdata_analytics",
  "comp_type": "database",
  "credentials_id": 1,
  "entity_name": "analytics_data",
  "query": "SELECT * FROM analytics_data WHERE date >= :start_date",
  "params": {
    "start_date": "2024-01-01"
  },
  "strategy_type": "bigdata",
  "chunk_size": 10000
}
```

### Connection Pool Configuration

```json
{
  "name": "high_performance_read",
  "comp_type": "database",
  "credentials_id": 1,
  "entity_name": "transactions",
  "query": "SELECT * FROM transactions",
  "strategy_type": "bulk",
  "pool_size": 20,
  "pool_timeout": 60,
  "pool_recycle": 3600,
  "pool_pre_ping": true
}
```

### Advanced Query with Window Functions

```json
{
  "name": "ranked_sales",
  "comp_type": "database",
  "credentials_id": 2,
  "entity_name": "sales",
  "query": "SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rank FROM sales",
  "strategy_type": "bulk"
}
```

### Recursive Query Configuration

```json
{
  "name": "hierarchy_data",
  "comp_type": "database",
  "credentials_id": 1,
  "entity_name": "employees",
  "query": "WITH RECURSIVE employee_hierarchy AS (SELECT id, name, manager_id, 1 as level FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id, eh.level + 1 FROM employees e JOIN employee_hierarchy eh ON e.manager_id = eh.id) SELECT * FROM employee_hierarchy",
  "strategy_type": "bulk"
}
```

## MariaDB Features

### Character Set Support
- **UTF8MB4 Support**: Full Unicode support including emojis and international characters
- **Unicode Collation**: Accurate sorting for different languages
- **Character Set Conversion**: Automatic character set conversion for data integration
- **Encoding Flexibility**: Support for various character encodings

### Storage Engine Support
- **InnoDB**: Full ACID compliance with foreign key support
- **MyISAM**: Fast read operations and full-text search capabilities
- **Aria**: Crash-safe alternative to MyISAM
- **Memory Engine**: High-speed temporary storage
- **Archive Engine**: Compressed storage for historical data

### Performance Features
- **Query Cache**: Automatic caching of frequently executed queries
- **Buffer Pool Optimization**: Maximizes memory efficiency
- **Thread Pool**: Efficient connection management
- **Parallel Replication**: Multi-threaded replication for large datasets

## Error Handling

### Connection Errors
- **Clear Messages**: Descriptive error messages for connection failures
- **Retry Logic**: Automatic retry with exponential backoff
- **Failover Support**: Automatic switching to backup servers
- **Health Monitoring**: Real-time connection health tracking

### Query Errors
- **SQL Syntax Errors**: Detailed error messages for SQL issues
- **Constraint Violations**: Specific information about data integrity violations
- **Timeout Handling**: Query timeout management
- **Resource Monitoring**: Resource exhaustion handling

### Transaction Errors
- **Rollback Support**: Automatic rollback on transaction failures
- **Deadlock Detection**: Automatic deadlock detection and resolution
- **Isolation Level Management**: Configurable transaction isolation
- **Timeout Handling**: Transaction timeout management

### Error Reporting
```json
{
  "mariadb_error": {
    "error_type": "connection_failure",
    "error_code": 2003,
    "message": "Can't connect to MySQL server on 'localhost' (10061)",
    "query": "SELECT * FROM users"
  }
}
```

## Performance Considerations

### Row Processing
- **Immediate Processing**: Processes data as it arrives
- **Memory Efficient**: Low memory usage for streaming data
- **Real-time**: Immediate database operations
- **Connection Pooling**: Efficient connection management

### Bulk Processing
- **Batch Processing**: Processes entire DataFrames at once
- **High Performance**: Optimized for medium-sized datasets
- **Memory Management**: Efficient DataFrame handling
- **Transaction Support**: Full ACID transaction support

### BigData Processing
- **Distributed Processing**: Handles large datasets across multiple cores
- **Lazy Evaluation**: Deferred computation for memory efficiency
- **Scalable**: Processes datasets of virtually any size
- **Query Optimization**: Optimized SQL query execution

## Configuration

### Database Options
- **Connection Settings**: Configure database connection parameters
- **Query Parameters**: Set query execution parameters
- **Transaction Settings**: Configure transaction behavior
- **Performance Tuning**: Optimize database performance

### Security Settings
- **Authentication**: Configure authentication methods
- **Authorization**: Set up access control
- **Encryption**: Enable data encryption
- **Audit Logging**: Configure audit logging

## Best Practices

### Query Design
- **Parameterized Queries**: Use parameterized queries to prevent SQL injection
- **Query Optimization**: Optimize queries for specific use cases
- **Index Usage**: Use appropriate indexes for performance
- **Query Analysis**: Use EXPLAIN to analyze query execution plans

### Connection Management
- **Connection Pooling**: Use connection pooling for efficient resource utilization
- **Pool Monitoring**: Monitor pool usage and health
- **Error Handling**: Implement robust connection error handling
- **Timeout Settings**: Use appropriate timeout settings

### Data Handling
- **Data Validation**: Validate input data before processing
- **Data Types**: Use appropriate data types for optimal performance
- **NULL Handling**: Handle NULL values appropriately
- **Transaction Management**: Use transactions appropriately

### Performance
- **Monitoring**: Monitor database performance regularly
- **Strategy Selection**: Choose appropriate processing strategies
- **Memory Management**: Monitor memory usage during operations
- **Caching**: Use query caching for frequently executed queries

### MariaDB-Specific
- **Storage Engines**: Choose appropriate storage engines for your use case
- **Character Sets**: Configure character sets properly
- **JSON Functions**: Use JSON functions efficiently
- **Window Functions**: Leverage window functions for analytical queries