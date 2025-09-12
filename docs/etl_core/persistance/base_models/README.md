# Base Models

This directory contains database model definitions and base classes for data persistence.

## Overview

Base models provide the foundation for data persistence in the ETL system, defining database schemas, relationships, and common functionality for all persistent entities.

## Components

### [Component Base](./component_base.py)
- **File**: `component_base.py`
- **Purpose**: Base model for component persistence
- **Features**: Component schema, component relationships, component metadata

### [Dataclasses Base](./dataclasses_base.py)
- **File**: `dataclasses_base.py`
- **Purpose**: Base model for dataclass persistence
- **Features**: Dataclass schema, dataclass relationships, dataclass metadata

### [Job Base](./job_base.py)
- **File**: `job_base.py`
- **Purpose**: Base model for job persistence
- **Features**: Job schema, job relationships, job metadata

## Model Architecture

### Base Model Classes
All persistent models inherit from base model classes that provide:
- **Common Fields**: Common database fields
- **Relationships**: Model relationships
- **Validation**: Data validation
- **Serialization**: Data serialization
- **Metadata**: Model metadata

### SQLModel Integration
- **SQLModel Base**: All models inherit from SQLModel
- **Pydantic Integration**: Pydantic model validation
- **Database Mapping**: Automatic database table mapping
- **Relationship Management**: Automatic relationship management
- **Migration Support**: Database migration support

## Component Base Model

### Component Schema
```python
class ComponentBase(SQLModel):
    name: str
    component_type: str
    configuration: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime
```

### Component Features
- **Component Identification**: Unique component identification
- **Component Type**: Component type classification
- **Configuration Storage**: Component configuration storage
- **Status Tracking**: Component status tracking
- **Timestamps**: Creation and update timestamps

## Dataclasses Base Model

### Dataclass Schema
```python
class DataclassesBase(SQLModel):
    name: str
    dataclass_type: str
    schema_definition: Dict[str, Any]
    validation_rules: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
```

### Dataclass Features
- **Dataclass Identification**: Unique dataclass identification
- **Type Classification**: Dataclass type classification
- **Schema Definition**: Dataclass schema definition
- **Validation Rules**: Data validation rules
- **Timestamps**: Creation and update timestamps

## Job Base Model

### Job Schema
```python
class JobBase(SQLModel):
    name: str
    num_of_retries: int
    file_logging: bool
    strategy_type: StrategyType
    created_at: datetime
    updated_at: datetime
```

### Job Features
- **Job Identification**: Unique job identification
- **Retry Configuration**: Retry attempt configuration
- **Logging Configuration**: File logging configuration
- **Strategy Type**: Execution strategy type
- **Timestamps**: Creation and update timestamps

## Model Relationships

### One-to-One Relationships
- **Component to Configuration**: One component has one configuration
- **Job to Execution**: One job has one execution
- **Dataclass to Schema**: One dataclass has one schema

### One-to-Many Relationships
- **Job to Components**: One job has many components
- **Component to Metrics**: One component has many metrics
- **Job to Executions**: One job has many executions

### Many-to-Many Relationships
- **Components to Dependencies**: Components can have multiple dependencies
- **Jobs to Tags**: Jobs can have multiple tags
- **Components to Metrics**: Components can have multiple metrics

## Data Validation

### Field Validation
- **Type Validation**: Field type validation
- **Range Validation**: Numeric range validation
- **Format Validation**: String format validation
- **Required Validation**: Required field validation
- **Custom Validation**: Custom validation rules

### Model Validation
- **Schema Validation**: Model schema validation
- **Relationship Validation**: Relationship validation
- **Business Rule Validation**: Business rule validation
- **Constraint Validation**: Database constraint validation
- **Integrity Validation**: Data integrity validation

## Serialization

### JSON Serialization
- **Automatic Serialization**: Automatic JSON serialization
- **Custom Serializers**: Custom serialization logic
- **Field Filtering**: Field filtering during serialization
- **Nested Serialization**: Nested object serialization
- **Date Formatting**: Date formatting for JSON

### Database Serialization
- **ORM Mapping**: Object-relational mapping
- **Field Mapping**: Database field mapping
- **Type Conversion**: Automatic type conversion
- **Relationship Loading**: Relationship loading
- **Lazy Loading**: Lazy relationship loading

## Migration Support

### Schema Migration
- **Version Control**: Database schema version control
- **Migration Scripts**: Automated migration scripts
- **Data Migration**: Data migration support
- **Rollback Support**: Migration rollback support
- **Validation**: Migration validation

### Model Evolution
- **Field Addition**: Add new fields to models
- **Field Removal**: Remove fields from models
- **Field Modification**: Modify existing fields
- **Relationship Changes**: Change model relationships
- **Index Changes**: Modify database indexes

## Performance Optimization

### Query Optimization
- **Index Usage**: Optimal index usage
- **Query Planning**: Query execution planning
- **Join Optimization**: Join operation optimization
- **Subquery Optimization**: Subquery optimization
- **Caching**: Query result caching

### Data Access Optimization
- **Connection Pooling**: Efficient connection pooling
- **Batch Operations**: Batch data operations
- **Lazy Loading**: Lazy data loading
- **Pagination**: Efficient pagination
- **Caching**: Data caching strategies

## Security

### Data Security
- **Access Control**: Data access control
- **Encryption**: Data encryption at rest
- **Audit Logging**: Data access audit logging
- **Data Masking**: Sensitive data masking
- **Compliance**: Security compliance features

### Model Security
- **Field Protection**: Protect sensitive fields
- **Access Validation**: Validate data access
- **Permission Checking**: Check user permissions
- **Data Sanitization**: Sanitize input data
- **SQL Injection Prevention**: Prevent SQL injection

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
