# MongoDB Components Documentation

## Overview

The MongoDB Components provide comprehensive NoSQL database operations with document-based data storage, flexible schema support, and powerful aggregation capabilities. They offer enterprise-grade MongoDB integration with support for both read and write operations, advanced querying, and real-time data processing.

## Core Features

### Document-Based Operations
- **Flexible Schema**: No fixed schema requirements for maximum flexibility
- **Document Storage**: Native BSON document storage with full type support
- **Nested Documents**: Support for complex nested data structures
- **Array Operations**: Advanced array field operations and queries
- **GridFS Support**: Large file storage and retrieval capabilities

### Advanced Query Capabilities
- **Rich Queries**: Complex query expressions with MongoDB's query language
- **Aggregation Pipelines**: Powerful multi-stage data aggregation
- **Index Support**: Automatic and manual index utilization
- **Text Search**: Full-text search capabilities with language support
- **Geospatial Queries**: Geographic data queries and operations

### Enterprise Features
- **Connection Pooling**: Efficient connection reuse and management
- **Replica Set Support**: High availability with automatic failover
- **Sharding Support**: Horizontal scaling with automatic data distribution
- **Change Streams**: Real-time data change notifications
- **Transactions**: ACID transaction support for multi-document operations

## Components

### MongoDB Read Component
- **File**: `mongodb_read.py`
- **Purpose**: Read data from MongoDB collections
- **Features**: Query execution, aggregation pipelines, result set processing

### MongoDB Write Component
- **File**: `mongodb_write.py`
- **Purpose**: Write data to MongoDB collections
- **Features**: Insert, update, delete operations, bulk operations, document validation

### MongoDB Base Component
- **File**: `mongodb.py`
- **Purpose**: Base class with MongoDB-specific functionality
- **Features**: Connection management, database selection, collection handling

### MongoDB Connection Handler
- **File**: `mongodb_connection_handler.py`
- **Purpose**: MongoDB connection management
- **Features**: Connection pooling, URI building, client configuration

## Features

### Document-Based Operations
- **Flexible Schema**: No fixed schema requirements
- **Document Storage**: Native BSON document storage
- **Nested Documents**: Support for complex nested data structures
- **Array Operations**: Array field operations and queries

### Query Capabilities
- **Rich Queries**: Complex query expressions
- **Aggregation Pipelines**: Powerful data aggregation
- **Index Support**: Automatic and manual index utilization
- **Text Search**: Full-text search capabilities

### Connection Management
- **Connection Pooling**: Efficient connection reuse
- **Replica Set Support**: High availability with replica sets
- **Sharding Support**: Horizontal scaling with sharding
- **Authentication**: Multiple authentication methods

## Configuration

### Required Fields
- `credentials_id`: ID for database credentials in context
- `database`: Database name to connect to
- `collection`: Collection name for operations (write components)

### Optional Fields
- `auth_db_name`: Authentication database name
- `pool_size`: Connection pool size
- `pool_timeout`: Connection timeout settings
- `read_preference`: Read preference for replica sets
- `write_concern`: Write concern for data durability

### Connection URI
Automatically built connection URI with:
- User authentication
- Host and port configuration
- Database specification
- Connection parameters

## Example Usage

### Read Component
```python
from etl_core.components.databases.mongodb.mongodb_read import MongoDBReadComponent

# Configure read component
component = MongoDBReadComponent(
    name="customer_reader",
    credentials_id="mongodb_creds",
    database="sales_db",
    collection="customers",
    query={"region": "North America", "status": "active"}
)
```

### Write Component
```python
from etl_core.components.databases.mongodb.mongodb_write import MongoDBWriteComponent

# Configure write component
component = MongoDBWriteComponent(
    name="customer_writer",
    credentials_id="mongodb_creds",
    database="sales_db",
    collection="customers",
    if_exists_strategy="append"
)
```

## Advanced Features

### Aggregation Pipelines
- **Pipeline Stages**: $match, $group, $sort, $project, etc.
- **Complex Aggregations**: Multi-stage aggregation pipelines
- **Data Transformation**: Real-time data transformation
- **Analytics**: Built-in analytics functions

### Document Operations
- **Upsert Operations**: Insert or update documents
- **Bulk Operations**: Efficient bulk insert/update/delete
- **Document Validation**: Schema validation rules
- **Change Streams**: Real-time change notifications

### Index Management
- **Automatic Indexing**: Automatic index creation
- **Compound Indexes**: Multi-field indexes
- **Text Indexes**: Full-text search indexes
- **Geospatial Indexes**: Geographic data indexes

## Data Processing

### Row Processing
- Individual document operations
- Real-time processing
- Immediate results

### Bulk Processing
- Batch document operations
- Efficient bulk processing
- Transaction-like behavior

### BigData Processing
- Large dataset handling
- Distributed processing
- Memory-efficient operations

## Performance Optimizations

### Connection Pooling
- Reuses database connections efficiently
- Configurable pool size and timeout
- Automatic connection cleanup

### Query Optimization
- **Index Usage**: Automatic index utilization
- **Query Planning**: MongoDB query planner optimization
- **Projection**: Field projection for reduced data transfer
- **Limit/Skip**: Pagination support

### Bulk Operations
- Bulk insert operations for better performance
- Configurable batch sizes
- Ordered and unordered bulk operations
- Error handling for bulk operations

## Error Handling

- **Connection Errors**: Automatic retry with exponential backoff
- **Query Errors**: Detailed error messages with query context
- **Write Errors**: Detailed write error information
- **Validation Errors**: Document validation error details
- **Network Errors**: Network connectivity error handling

## Security Features

- **Credential Management**: Secure credential handling through context
- **Authentication**: Multiple authentication methods (SCRAM, LDAP, etc.)
- **Authorization**: Role-based access control
- **Encryption**: TLS/SSL encrypted connections
- **Audit Logging**: Database operation auditing

## Monitoring and Metrics

- **Connection Metrics**: Pool usage, connection timeouts
- **Query Metrics**: Execution time, document counts
- **Error Metrics**: Error rates, retry attempts
- **Performance Metrics**: Throughput, latency measurements
- **Index Metrics**: Index usage and performance
