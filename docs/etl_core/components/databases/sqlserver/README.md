# SQL Server Components Documentation

## Overview

The SQL Server Components provide enterprise-grade database operations with comprehensive T-SQL support, advanced security features, and high availability capabilities. They offer full integration with Microsoft SQL Server's enterprise features, including Always On availability groups, in-memory OLTP, and advanced security features.

## Core Features

### Enterprise Database Operations
- **T-SQL Support**: Full Transact-SQL language support with advanced features
- **Stored Procedures**: Execute complex stored procedures and functions
- **Advanced Security**: Row-level security, dynamic data masking, transparent data encryption
- **High Availability**: Always On availability groups and failover clustering
- **Performance**: In-memory OLTP, columnstore indexes, and query optimization

### Advanced SQL Features
- **Complex Queries**: Window functions, CTEs, recursive queries, and advanced joins
- **Data Types**: Support for all SQL Server data types including spatial and XML
- **Functions**: Built-in functions, user-defined functions, and table-valued functions
- **Views**: Complex views, indexed views, and partitioned views
- **Triggers**: Database triggers and DDL triggers

### Enterprise Integration
- **Integration Services**: SSIS integration and data flow support
- **Analysis Services**: SSAS integration for OLAP operations
- **Reporting Services**: SSRS integration for report generation
- **Azure Integration**: Azure SQL Database and Azure Synapse support
- **Power BI**: Power BI integration and data modeling

## Components

### SQL Server Read Component
- **File**: `sqlserver_read.py`
- **Purpose**: Read data from SQL Server databases
- **Features**: T-SQL query execution, result set processing, schema validation

### SQL Server Write Component
- **File**: `sqlserver_write.py`
- **Purpose**: Write data to SQL Server databases
- **Features**: Insert, update, delete operations, batch processing, T-SQL support

### SQL Server Base Component
- **File**: `sqlserver.py`
- **Purpose**: Base class with SQL Server-specific functionality
- **Features**: Connection management, T-SQL support, enterprise features

## Features

### Enterprise Features
- **T-SQL Support**: Full Transact-SQL language support
- **Advanced Security**: Row-level security, dynamic data masking
- **High Availability**: Always On availability groups
- **Performance**: In-memory OLTP, columnstore indexes

### Connection Management
- **Connection Pooling**: Efficient connection reuse
- **Multiple Protocols**: TCP/IP, Named Pipes, Shared Memory
- **Authentication**: Windows Authentication, SQL Server Authentication
- **Encryption**: TLS/SSL encrypted connections

### Data Processing
- **Row Processing**: Individual record operations
- **Bulk Processing**: Batch operations for pandas DataFrames
- **BigData Processing**: Large dataset handling with Dask
- **Transaction Support**: Full ACID transaction compliance

## Configuration

### Required Fields
- `credentials_id`: ID for database credentials in context
- `database`: Database name to connect to
- `table`: Table name for operations (write components)

### Optional Fields
- `server`: SQL Server instance name
- `port`: Port number (default: 1433)
- `pool_size`: Connection pool size
- `pool_timeout`: Connection timeout settings
- `authentication`: Authentication method (Windows/SQL Server)

### Connection String
Automatically built connection string with:
- Server and instance specification
- Database name
- Authentication method
- Connection parameters

## Example Usage

### Read Component
```python
from etl_core.components.databases.sqlserver.sqlserver_read import SQLServerReadComponent

# Configure read component
component = SQLServerReadComponent(
    name="customer_reader",
    credentials_id="sqlserver_creds",
    database="sales_db",
    query="SELECT * FROM customers WHERE region = @region AND status = @status",
    query_params={"region": "North America", "status": "active"}
)
```

### Write Component
```python
from etl_core.components.databases.sqlserver.sqlserver_write import SQLServerWriteComponent

# Configure write component
component = SQLServerWriteComponent(
    name="customer_writer",
    credentials_id="sqlserver_creds",
    database="sales_db",
    table="customers",
    if_exists_strategy="append"
)
```

## Advanced Features

### T-SQL Support
- **Stored Procedures**: Execute stored procedures
- **Functions**: User-defined function support
- **Triggers**: Database trigger integration
- **Views**: Complex view support

### Enterprise Features
- **Always On**: High availability groups
- **Replication**: Transactional and merge replication
- **Partitioning**: Table and index partitioning
- **Compression**: Data and backup compression

### Security Features
- **Row-Level Security**: Fine-grained access control
- **Dynamic Data Masking**: Sensitive data protection
- **Transparent Data Encryption**: Database encryption
- **Audit**: Comprehensive audit logging

## Performance Optimizations

### Connection Pooling
- Reuses database connections efficiently
- Configurable pool size and timeout
- Automatic connection cleanup

### Query Optimization
- **Prepared Statements**: Security and performance
- **Query Hints**: SQL Server query hints
- **Index Usage**: Automatic index utilization
- **Parallel Processing**: Parallel query execution

### Batch Processing
- Bulk insert operations for better performance
- Configurable batch sizes
- Transaction batching for consistency
- BULK INSERT for high-performance loading

## Error Handling

- **Connection Errors**: Automatic retry with exponential backoff
- **Query Errors**: Detailed error messages with query context
- **Transaction Errors**: Automatic rollback on failure
- **Constraint Violations**: Detailed constraint violation messages
- **Timeout Errors**: Connection and query timeout handling

## Security Features

- **Credential Management**: Secure credential handling through context
- **SQL Injection Prevention**: Parameterized queries
- **Connection Security**: TLS/SSL encrypted connections
- **Authentication**: Windows and SQL Server authentication
- **Authorization**: Role-based access control
- **Audit Logging**: Comprehensive operation auditing

## Monitoring and Metrics

- **Connection Metrics**: Pool usage, connection timeouts
- **Query Metrics**: Execution time, row counts, query plans
- **Error Metrics**: Error rates, retry attempts
- **Performance Metrics**: Throughput, latency measurements
- **Enterprise Metrics**: Always On, replication metrics

## Integration Features

- **Integration Services**: SSIS integration support
- **Analysis Services**: SSAS integration
- **Reporting Services**: SSRS integration
- **Azure Integration**: Azure SQL Database support
- **Power BI**: Power BI integration support
