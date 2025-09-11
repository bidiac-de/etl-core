# PostgreSQL ETL Components

This document describes the PostgreSQL ETL components that provide read and write capabilities for PostgreSQL databases. These components follow the standard ETL Core architecture with support for row, bulk, and bigdata processing strategies.

## Overview

The PostgreSQL components provide enterprise-grade database integration with PostgreSQL, offering:
- **Advanced SQL Features**: JSON/JSONB support, array operations, range types, full-text search
- **Performance**: Connection pooling, query optimization, batch processing
- **Enterprise Features**: PostGIS, custom data types, extensions, window functions
- **Scalability**: Support for large datasets and distributed processing

## Component Hierarchy

```
Component (base_component.py)
├── DatabaseComponent (database.py)
│   └── SQLDatabaseComponent (sql_database.py)
│       └── PostgreSQLComponent (postgresql/postgresql.py)
│           ├── PostgreSQLRead (postgresql/postgresql_read.py)
│           └── PostgreSQLWrite (postgresql/postgresql_write.py)
```

## Components

### PostgreSQLRead

Reads data from PostgreSQL tables using SQL queries with support for all three processing strategies.

#### Configuration

```json
{
  "name": "read_users",
  "description": "Read users from PostgreSQL",
  "comp_type": "database",
  "credentials_id": 1,
  "entity_name": "users",
  "query": "SELECT id, name, email FROM users WHERE active = :active",
  "params": {
    "active": 1
  },
  "strategy_type": "bulk",
  "charset": "utf8",
  "collation": "en_US.UTF-8",
  "pool_size": 10,
  "pool_timeout": 30
}
```

#### Required Fields
- `credentials_id`: ID of credentials in context
- `entity_name`: Target table name
- `query`: SQL query to execute

#### Optional Fields
- `params`: Query parameters (object)
- `strategy_type`: Execution strategy ("row", "bulk", "bigdata")
- `charset`: Character encoding (default: "utf8")
- `collation`: Collation setting (default: "en_US.UTF-8")
- `pool_size`: Connection pool size
- `pool_timeout`: Connection timeout

#### Advanced Query Features
- **JSON/JSONB Support**: Native JSON data type handling
- **Array Operations**: PostgreSQL array data types
- **Range Types**: Date, numeric, and text range types
- **Custom Data Types**: PostgreSQL-specific data types
- **Extensions**: PostGIS, Full-Text Search, UUID support

### PostgreSQLWrite

Writes data to PostgreSQL tables with various strategies and transaction support.

#### Configuration

```json
{
  "name": "write_users",
  "description": "Write users to PostgreSQL",
  "comp_type": "database",
  "credentials_id": 1,
  "entity_name": "users",
  "if_exists_strategy": "append",
  "strategy_type": "bulk",
  "charset": "utf8",
  "collation": "en_US.UTF-8",
  "pool_size": 10,
  "pool_timeout": 30
}
```

#### Write Strategies
- `fail`: Raise error if table exists
- `replace`: Drop and recreate table
- `append`: Insert data into existing table

#### Advanced Features
- **Upsert Operations**: INSERT ... ON CONFLICT support
- **Bulk Loading**: COPY command for high-performance loading
- **JSON Handling**: Native JSON/JSONB support
- **Array Support**: PostgreSQL array data types
- **Constraint Handling**: Foreign key and unique constraint support

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

### Basic Read Configuration

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
  "query": "SELECT id, name, metadata->>'department' as department FROM users WHERE metadata @> '{\"active\": true}'",
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

### Array Operations

```json
{
  "name": "read_products_with_tags",
  "comp_type": "database",
  "credentials_id": 1,
  "entity_name": "products",
  "query": "SELECT * FROM products WHERE tags @> ARRAY[:tag]",
  "params": {
    "tag": "electronics"
  },
  "strategy_type": "bulk"
}
```

### Full-Text Search

```json
{
  "name": "search_articles",
  "comp_type": "database",
  "credentials_id": 1,
  "entity_name": "articles",
  "query": "SELECT *, ts_rank(to_tsvector('english', content), query) as rank FROM articles, to_tsquery('english', :search_terms) query WHERE to_tsvector('english', content) @@ query ORDER BY rank DESC",
  "params": {
    "search_terms": "database performance"
  },
  "strategy_type": "bulk"
}
```

### BigData Processing

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

## PostgreSQL-Specific Features

### JSON/JSONB Operations

PostgreSQL provides **comprehensive JSON support** with JSONB data type for efficient storage and querying. The **JSON operators** allow complex queries on JSON fields, while **JSON functions** provide powerful data manipulation capabilities. **JSON aggregation** enables grouping and summarizing JSON data, making it ideal for document-based applications.

### Array Operations

PostgreSQL's **array data types** provide powerful array operations including containment checks, array length queries, and array unnesting. The **array operators** enable efficient querying of array fields, while **array functions** provide comprehensive array manipulation capabilities. This makes PostgreSQL ideal for applications requiring **complex data structures**.

### Range Types

PostgreSQL's **range types** provide efficient storage and querying of ranges including date ranges, numeric ranges, and text ranges. The **range operators** enable containment checks and overlap queries, while **range functions** provide comprehensive range manipulation capabilities. This makes PostgreSQL ideal for **temporal and numeric range queries**.

### Full-Text Search

PostgreSQL's **full-text search capabilities** provide powerful text search functionality with language support and ranking. The **tsvector and tsquery** types enable efficient text indexing and querying, while **text search functions** provide comprehensive text manipulation capabilities. This makes PostgreSQL ideal for **content management and search applications**.

## Performance Optimization

### Connection Pooling

**Efficient connection pooling** ensures optimal database connectivity with configurable pool sizes and timeout settings. The **pool monitoring** capabilities provide real-time insights into connection usage and health, while **automatic connection cleanup** prevents connection leaks and resource exhaustion.

### Query Optimization

**Advanced query optimization** leverages PostgreSQL's query planner and optimizer for optimal performance. The **index usage** capabilities ensure efficient data access, while **query planning** provides insights into query execution strategies. The **prepared statements** support improves both security and performance.

### Batch Processing

**High-performance batch processing** uses PostgreSQL's COPY command for efficient bulk data loading. The **batch size optimization** ensures optimal memory usage and performance, while **transaction batching** provides data consistency and error handling.

## Error Handling

### Connection Errors

**Robust connection error handling** ensures reliable database connectivity with automatic retry mechanisms and exponential backoff. The **connection pool recovery** capabilities automatically detect and replace failed connections, while **failover support** enables automatic switching to backup servers.

### Query Errors

**Comprehensive query error handling** provides detailed error information for debugging and monitoring. The **SQL syntax error handling** catches and reports query issues, while **constraint violation handling** provides specific information about data integrity violations.

### Transaction Errors

**Transaction error handling** ensures data consistency with automatic rollback capabilities and deadlock detection. The **transaction timeout handling** prevents long-running transactions from blocking resources, while **isolation level management** ensures appropriate transaction isolation.

## Security Features

### Authentication

**Multiple authentication methods** including username/password, certificate-based authentication, and LDAP integration provide flexible security options. **SSL/TLS encrypted connections** protect data in transit, while **centralized user management** enables enterprise-wide authentication.

### Authorization

**Role-based access control** provides flexible permission management with fine-grained access control capabilities. **Row-level security** enables data-level access control, while **column-level permissions** provide field-level security. **Comprehensive audit logging** tracks all database operations for compliance.

### Data Protection

**Data encryption at rest** protects stored data using industry-standard encryption algorithms, while **data encryption in transit** secures data during transmission. **Sensitive data masking** capabilities allow organizations to hide sensitive information in non-production environments, while **compliance support** ensures regulatory compliance.

## Monitoring and Metrics

### Performance Metrics

**Comprehensive performance monitoring** tracks query execution times, connection pool usage, and transaction throughput. The **error rate monitoring** enables proactive issue resolution, while **resource utilization monitoring** tracks CPU, memory, and disk usage for optimal performance.

### Health Monitoring

**Continuous health monitoring** ensures database availability and performance with real-time connectivity checks and query performance tracking. The **connection health monitoring** ensures stable connections over time, while **configurable alert thresholds** provide early warning of potential issues.

### Logging

**Structured logging** provides detailed insights into database operations with machine-readable log formats for automated analysis. The **query logging** captures detailed SQL query information including execution plans, while **error logging** provides comprehensive error information with stack traces.

## Best Practices

### Query Design

**Effective query design** is crucial for optimal PostgreSQL performance. Always use **parameterized queries** to prevent SQL injection attacks and improve caching efficiency. **Optimize queries** for specific use cases by understanding data access patterns and using appropriate indexes.

### Connection Management

**Proper connection management** ensures efficient resource utilization and reliable database connectivity. Use **connection pooling** to manage connections efficiently and reduce overhead. **Monitor pool usage** to ensure optimal configuration and identify bottlenecks.

### Data Handling

**Careful data handling** ensures data integrity and optimal performance. Always **validate input data** before processing to prevent corruption and security issues. Use **appropriate data types** that match actual requirements to optimize storage and performance.

### Performance

**Continuous performance monitoring** and optimization are essential for maintaining optimal database performance. **Monitor query performance** regularly to identify slow queries and optimization opportunities. Use **appropriate processing strategies** based on data size and requirements.

### PostgreSQL-Specific

**Leverage PostgreSQL-specific features** to maximize performance and functionality. Use **JSON/JSONB operations** efficiently for document-based data processing, and **leverage array operations** for complex data structures. **Optimize for PostgreSQL features** like full-text search and range types.