# Data Persistence

Data persistence components provide **data storage**, **database models**, and **data access management** for ETL pipeline data with comprehensive support for job configurations and execution metadata.

## Overview

Data persistence components define the structure, storage, and access patterns for data flowing through the ETL pipeline, ensuring proper data management and type safety. They provide **comprehensive persistence support** and **data access capabilities** for robust system management.

### Key Concepts
- **Data Models**: Database model definitions and relationships
- **Data Access**: Repository patterns and data access handlers
- **Configuration Management**: Job and component configuration storage
- **Migration Support**: Database migration and schema management

## Components

### [Base Models](./base_models/README.md)
Database model definitions and base classes.

### [Configs](./configs/README.md)
Configuration models and schemas.

### [Handlers](./handlers/README.md)
Data access handlers and repository patterns.

### [Database](./db.py)
- **File**: `db.py`
- **Purpose**: Database connection and configuration
- **Features**: Database setup, connection management, migration support

### [Errors](./errors.py)
- **File**: `errors.py`
- **Purpose**: Persistence-specific error handling
- **Features**: Custom exception classes, error handling utilities

### [Table Definitions](./table_definitions.py)
- **File**: `table_definitions.py`
- **Purpose**: Database table definitions
- **Features**: Table schemas, relationships, constraints

## Persistence Types

### Data Models
- **Component Base**: Base class for component models
- **Dataclasses Base**: Base class for dataclass models
- **Job Base**: Base class for job models
- **SQLModel Integration**: SQLModel-based model definitions
- **Pydantic Validation**: Pydantic model validation

### Data Access
- **Component Handler**: Component data access
- **Dataclasses Handler**: Dataclass data access
- **Job Handler**: Job data access
- **CRUD Operations**: Create, read, update, delete operations
- **Query Building**: Dynamic query building

### Features
- **Database Mapping**: Database table mapping
- **Relationship Management**: Model relationships
- **Migration Support**: Database migration support
- **Connection Pooling**: Database connection pooling

## JSON Configuration Examples

### Database Configuration
```json
{
  "database": {
    "url": "postgresql://user:password@localhost/etl_db",
    "pool_size": 10,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 3600
  }
}
```

### Model Configuration
```json
{
  "models": {
    "component": {
      "table_name": "components",
      "fields": [
        {
          "name": "id",
          "type": "integer",
          "primary_key": true
        },
        {
          "name": "name",
          "type": "string",
          "nullable": false
        },
        {
          "name": "type",
          "type": "string",
          "nullable": false
        }
      ]
    },
    "job": {
      "table_name": "jobs",
      "fields": [
        {
          "name": "id",
          "type": "integer",
          "primary_key": true
        },
        {
          "name": "name",
          "type": "string",
          "nullable": false
        },
        {
          "name": "status",
          "type": "string",
          "nullable": false
        }
      ]
    }
  }
}
```

## Persistence Features

### Data Models
- **SQLModel Integration**: SQLModel-based model definitions
- **Pydantic Validation**: Pydantic model validation
- **Database Mapping**: Database table mapping
- **Relationship Management**: Model relationships
- **Migration Support**: Database migration support

### Data Access
- **Repository Pattern**: Repository pattern implementation
- **Query Building**: Dynamic query building
- **Relationship Loading**: Eager and lazy loading
- **Transaction Support**: Transaction management
- **Error Handling**: Comprehensive error handling

## Error Handling

### Persistence Errors
- **Clear Messages**: Descriptive error messages for persistence issues
- **Database Validation**: Path-based error reporting for database problems
- **Query Errors**: Detailed query error information
- **Context**: Persistence and database context in error messages

### Error Types
- **Connection Errors**: Database connection errors
- **Query Errors**: SQL query errors
- **Validation Errors**: Data validation errors
- **Constraint Errors**: Database constraint errors
- **Transaction Errors**: Transaction-related errors

### Error Reporting
```json
{
  "persistence_error": {
    "error_type": "query_error",
    "table_name": "components",
    "query": "SELECT * FROM components WHERE id = ?",
    "message": "Column 'id' not found in table 'components'"
  }
}
```

## Performance Considerations

### Database Performance
- **Connection Pooling**: Optimize database connection pooling
- **Query Optimization**: Optimize query performance
- **Index Usage**: Optimize index usage
- **Caching**: Implement query result caching

### Data Access Performance
- **Repository Pattern**: Optimize repository pattern implementation
- **Batch Operations**: Optimize batch data operations
- **Lazy Loading**: Optimize lazy data loading
- **Memory Usage**: Minimize memory usage for data access

## Configuration

### Persistence Options
- **Database Configuration**: Configure database connection and settings
- **Model Configuration**: Configure data models and relationships
- **Migration Settings**: Configure database migration settings
- **Performance Settings**: Configure performance optimization settings

### Security Configuration
- **Access Control**: Configure data access control
- **Encryption**: Configure data encryption settings
- **Audit Logging**: Configure audit logging settings
- **Compliance**: Configure compliance requirements

## Best Practices

### Model Design
- **Normalization**: Proper database normalization
- **Relationship Design**: Well-designed relationships
- **Index Strategy**: Effective indexing strategy
- **Validation**: Comprehensive data validation
- **Documentation**: Clear model documentation

### Data Access
- **Repository Pattern**: Use repository pattern
- **Transaction Management**: Proper transaction management
- **Error Handling**: Comprehensive error handling
- **Performance Monitoring**: Monitor data access performance
- **Security**: Implement proper security measures

### Migration Management
- **Version Control**: Maintain migration version control
- **Testing**: Test migrations thoroughly
- **Rollback Planning**: Plan for rollback scenarios
- **Documentation**: Document migration changes
- **Validation**: Validate migration results
