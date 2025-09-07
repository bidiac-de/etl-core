# Database Components

This directory contains comprehensive components for database connectivity and operations across various database systems. The Database Components provide enterprise-grade database integration capabilities with support for both SQL and NoSQL databases, offering unified interfaces for data access, manipulation, and management.

## Overview

The Database Components are designed to provide seamless integration with various database systems while maintaining high performance, reliability, and security. They support both traditional SQL databases and modern NoSQL databases, offering consistent interfaces and advanced features for enterprise data processing scenarios.

## Core Features

### Universal Database Support
- **SQL Databases**: PostgreSQL, MariaDB, SQL Server with full SQL support
- **NoSQL Databases**: MongoDB with document-based operations
- **Unified Interface**: Consistent API across all database types
- **Type Safety**: Full type checking and validation throughout

### Advanced Database Operations
- **Read Operations**: Query execution with parameterized statements
- **Write Operations**: Insert, update, delete with transaction support
- **Bulk Operations**: High-performance batch processing
- **Schema Management**: Dynamic schema handling and validation

### Enterprise Features
- **Connection Pooling**: Efficient connection management and reuse
- **Transaction Support**: Full ACID transaction compliance
- **Security**: Comprehensive security and authentication support
- **Monitoring**: Detailed metrics and performance monitoring

## Component Architecture

### Database Component Hierarchy

```
DatabaseComponent (database.py)
├── SQLDatabaseComponent (sql_database.py)
│   ├── PostgreSQLComponent (postgresql/)
│   │   ├── PostgreSQLRead (postgresql_read.py)
│   │   └── PostgreSQLWrite (postgresql_write.py)
│   ├── MariaDBComponent (mariadb/)
│   │   ├── MariaDBRead (mariadb_read.py)
│   │   └── MariaDBWrite (mariadb_write.py)
│   └── SQLServerComponent (sqlserver/)
│       ├── SQLServerRead (sqlserver_read.py)
│       └── SQLServerWrite (sqlserver_write.py)
└── MongoDBComponent (mongodb/)
    ├── MongoDBRead (mongodb_read.py)
    └── MongoDBWrite (mongodb_write.py)
```

## Database Components

### SQL Database Components

#### [PostgreSQL Component](./postgresql/README.md)
**Purpose**: PostgreSQL database operations with advanced SQL features
- **Features**: JSON/JSONB support, array operations, range types, full-text search
- **Strategies**: Row streaming, bulk processing, bigdata distributed
- **Advanced**: PostGIS, custom data types, extensions, window functions
- **Performance**: Connection pooling, query optimization, batch processing

#### [MariaDB Component](./mariadb/README.md)
**Purpose**: MariaDB database operations with MySQL compatibility
- **Features**: UTF8MB4 support, window functions, CTEs, recursive queries
- **Strategies**: Row processing, bulk operations, bigdata lazy evaluation
- **Advanced**: Storage engines, JSON functions, parallel replication
- **Performance**: Query cache, buffer pool optimization, thread pooling

#### [SQL Server Component](./sqlserver/README.md)
**Purpose**: Microsoft SQL Server database operations
- **Features**: T-SQL support, stored procedures, functions, views
- **Strategies**: Row processing, bulk operations, bigdata distributed
- **Advanced**: CLR integration, XML support, spatial data types
- **Performance**: Connection pooling, query optimization, batch processing

### NoSQL Database Components

#### [MongoDB Component](./mongodb/README.md)
**Purpose**: MongoDB document database operations
- **Features**: Document queries, aggregation pipelines, indexing
- **Strategies**: Document streaming, bulk operations, bigdata sharding
- **Advanced**: GridFS, change streams, transactions, replica sets
- **Performance**: Connection pooling, query optimization, batch operations

## Common Features

### Processing Strategies

All database components support three processing strategies:

#### Row Processing
- **Real-time Processing**: Individual record processing
- **Streaming Support**: Continuous data streaming
- **Low Latency**: Minimal processing delay
- **Memory Efficient**: Optimized for streaming data

#### Bulk Processing
- **Batch Operations**: Process data in batches
- **High Throughput**: Optimized for medium datasets
- **Memory Management**: Efficient DataFrame handling
- **Transaction Support**: Batch transaction processing

#### BigData Processing
- **Distributed Processing**: Large dataset handling
- **Lazy Evaluation**: Deferred computation
- **Scalability**: Horizontal scaling support
- **Performance**: Optimized for big data scenarios

### Connection Management

All database components provide sophisticated connection management:

- **Connection Pooling**: Efficient connection management and reuse
- **Credential Management**: Secure credential handling from context
- **Connection Monitoring**: Real-time connection health monitoring
- **Automatic Retry**: Intelligent retry mechanisms for connection failures

### Transaction Support

- **ACID Compliance**: Full ACID transaction support
- **Isolation Levels**: Configurable transaction isolation levels
- **Deadlock Handling**: Automatic deadlock detection and resolution
- **Rollback Support**: Comprehensive rollback capabilities

### Security Features

- **Credential Management**: Secure credential handling through context
- **Connection Security**: Encrypted connections and SSL/TLS support
- **Access Control**: Database-level access control integration
- **Audit Logging**: Comprehensive audit trails for compliance

### Error Handling

- **Robust Error Handling**: Comprehensive error handling and recovery
- **Circuit Breaker**: Circuit breaker pattern for fault tolerance
- **Retry Logic**: Intelligent retry mechanisms with exponential backoff
- **Graceful Degradation**: Graceful error recovery and fallback strategies

### Monitoring and Metrics

- **Performance Metrics**: Detailed performance monitoring
- **Health Checks**: Real-time health monitoring
- **Query Analysis**: Query performance analysis and optimization
- **Resource Monitoring**: Connection pool and resource utilization monitoring

## Performance Optimization

### Connection Pooling

```python
class ConnectionPool:
    """Advanced connection pooling with monitoring."""
    
    def __init__(self, pool_config: PoolConfig):
        self.pool_size = pool_config.pool_size
        self.max_overflow = pool_config.max_overflow
        self.pool_timeout = pool_config.pool_timeout
        self.pool_recycle = pool_config.pool_recycle
        self.pool_pre_ping = pool_config.pool_pre_ping
        
        self._pool = None
        self._pool_stats = {
            "total_connections": 0,
            "active_connections": 0,
            "idle_connections": 0,
            "overflow_connections": 0
        }
```

### Query Optimization

```python
class QueryOptimizer:
    """Optimizes database queries for performance."""
    
    def __init__(self, database_type: str):
        self.database_type = database_type
        self.query_cache = {}
        self.optimization_rules = self._load_optimization_rules()
    
    def optimize_query(self, query: str, parameters: Dict[str, Any]) -> str:
        """Optimize query based on database type and parameters."""
        # Apply database-specific optimizations
        if self.database_type == "postgresql":
            return self._optimize_postgresql_query(query, parameters)
        elif self.database_type == "mariadb":
            return self._optimize_mariadb_query(query, parameters)
        elif self.database_type == "sqlserver":
            return self._optimize_sqlserver_query(query, parameters)
        else:
            return query
```

### Batch Processing

```python
class BatchProcessor:
    """Handles batch processing for database operations."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.batch_buffer = []
        self.batch_stats = {
            "batches_processed": 0,
            "rows_processed": 0,
            "processing_time": 0.0
        }
    
    async def process_batch(self, data: List[Dict[str, Any]], operation: str) -> List[Any]:
        """Process data in batches for optimal performance."""
        results = []
        
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            batch_result = await self._process_single_batch(batch, operation)
            results.extend(batch_result)
            
            # Update statistics
            self.batch_stats["batches_processed"] += 1
            self.batch_stats["rows_processed"] += len(batch)
        
        return results
```

## Monitoring and Metrics

### Database Metrics

```python
class DatabaseMetrics(ComponentMetrics):
    """Comprehensive metrics for database operations."""
    
    def __init__(self):
        super().__init__()
        # Connection metrics
        self.connections_created = 0
        self.connections_closed = 0
        self.connection_pool_usage = 0.0
        self.connection_errors = 0
        
        # Query metrics
        self.queries_executed = 0
        self.query_execution_time = 0.0
        self.query_cache_hits = 0
        self.query_cache_misses = 0
        
        # Transaction metrics
        self.transactions_started = 0
        self.transactions_committed = 0
        self.transactions_rolled_back = 0
        self.transaction_duration = 0.0
        
        # Performance metrics
        self.rows_processed = 0
        self.bytes_transferred = 0
        self.throughput = 0.0  # rows per second
        self.latency = 0.0     # average response time
```

### Performance Monitoring

```python
class DatabasePerformanceMonitor:
    """Monitors database performance and health."""
    
    def __init__(self, database_type: str):
        self.database_type = database_type
        self.performance_metrics = {}
        self.health_checks = {}
    
    def monitor_query_performance(self, query: str, execution_time: float, rows_affected: int):
        """Monitor individual query performance."""
        query_hash = hashlib.md5(query.encode()).hexdigest()
        
        if query_hash not in self.performance_metrics:
            self.performance_metrics[query_hash] = {
                "query": query,
                "execution_count": 0,
                "total_execution_time": 0.0,
                "avg_execution_time": 0.0,
                "max_execution_time": 0.0,
                "total_rows_affected": 0
            }
        
        metrics = self.performance_metrics[query_hash]
        metrics["execution_count"] += 1
        metrics["total_execution_time"] += execution_time
        metrics["avg_execution_time"] = metrics["total_execution_time"] / metrics["execution_count"]
        metrics["max_execution_time"] = max(metrics["max_execution_time"], execution_time)
        metrics["total_rows_affected"] += rows_affected
```

## Configuration Examples

### Database Connection Configuration

```python
# PostgreSQL configuration
postgresql_config = {
    "credentials_id": 1,
    "database": "analytics_db",
    "pool_size": 20,
    "pool_timeout": 30,
    "charset": "utf8",
    "collation": "en_US.UTF-8",
    "ssl_enabled": True,
    "ssl_cert_path": "/path/to/cert.pem"
}

# MariaDB configuration
mariadb_config = {
    "credentials_id": 2,
    "database": "transaction_db",
    "pool_size": 15,
    "pool_timeout": 25,
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "session_variables": {
        "sql_mode": "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"
    }
}

# MongoDB configuration
mongodb_config = {
    "credentials_id": 3,
    "database": "document_store",
    "pool_size": 10,
    "pool_timeout": 20,
    "auth_db_name": "admin",
    "client_kwargs": {
        "maxPoolSize": 50,
        "minPoolSize": 5,
        "maxIdleTimeMS": 30000
    }
}
```

### Pipeline Integration

```python
# Database pipeline with multiple sources
pipeline = DataPipeline([
    # Read from multiple databases
    PostgreSQLReader("analytics_data", query="SELECT * FROM user_events"),
    MariaDBReader("transaction_data", query="SELECT * FROM transactions"),
    MongoDBReader("user_profiles", collection="profiles"),
    
    # Merge data
    MergeComponent(name="data_consolidator"),
    
    # Process data
    FilterComponent(rule=active_users_rule),
    AggregationComponent(group_by=["user_id"], aggregations=[total_events, total_transactions]),
    
    # Write to destination
    PostgreSQLWriter("consolidated_analytics")
])
```

## Best Practices

### Database Design
- **Indexing**: Create appropriate indexes for query performance
- **Normalization**: Use proper database normalization
- **Constraints**: Implement data integrity constraints
- **Partitioning**: Use table partitioning for large datasets

### Performance
- **Connection Pooling**: Use appropriate pool sizes
- **Query Optimization**: Optimize queries for performance
- **Batch Processing**: Use batch operations for large datasets
- **Monitoring**: Implement comprehensive performance monitoring

### Security
- **Credential Management**: Use secure credential storage
- **Connection Security**: Use encrypted connections
- **Access Control**: Implement proper access controls
- **Audit Logging**: Log all database operations

### Error Handling
- **Retry Logic**: Implement appropriate retry strategies
- **Circuit Breakers**: Use circuit breakers for fault tolerance
- **Graceful Degradation**: Handle errors gracefully
- **Monitoring**: Monitor error rates and patterns
