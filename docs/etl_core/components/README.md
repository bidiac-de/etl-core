# ETL Core Components

ETL Core Components provide **data processing**, **database connectivity**, **file handling**, and **wiring** for ETL pipeline data with comprehensive support for complex data operations and validation rules.

## Overview

ETL Core Components define the structure, operations, and validation rules for data flowing through the ETL pipeline, ensuring data integrity and type safety. They provide **comprehensive processing support** and **validation capabilities** for robust data processing.

### Key Concepts
- **Data Operations**: Transform and manipulate data with filtering, aggregation, merging, splitting, and schema mapping
- **Database Connectivity**: Connect to various database systems for data operations
- **File Handling**: Read and write various file formats for data processing
- **Wiring**: Define connections, schemas, and validation rules

## Components

### [Data Operations](./data_operations/README.md)
Components for data transformation and manipulation operations including filtering, aggregation, merging, splitting, and schema mapping.

### [Database Components](./databases/README.md)
Database connectivity and operations for various database systems including MariaDB, MongoDB, PostgreSQL, and SQL Server.

### [File Components](./file_components/README.md)
Components for reading and writing various file formats including CSV, Excel, and JSON.

### [Wiring Components](./wiring/README.md)
Core wiring and configuration components for ports, schema definitions, and validation.

## Component Types

### Data Operations
- **Filtering**: Filter data based on conditions and rules
- **Aggregation**: Aggregate data using various functions
- **Merging**: Merge data from multiple sources
- **Splitting**: Split data into multiple outputs
- **Schema Mapping**: Transform data schemas and structures

### Database Components
- **MariaDB**: MySQL-compatible database operations
- **MongoDB**: Document database operations
- **PostgreSQL**: Advanced relational database operations
- **SQL Server**: Microsoft SQL Server operations

### File Components
- **CSV**: Comma-separated value file operations
- **Excel**: Microsoft Excel file operations
- **JSON**: JavaScript Object Notation file operations

### Features
- **Type Safety**: Comprehensive type checking and validation
- **Processing Strategies**: Support for row, bulk, and bigdata processing
- **Error Handling**: Robust error handling and recovery mechanisms
- **Performance**: Optimized processing for various data scales

## Processing Strategies

### Row Processing
- **Use Case**: Real-time streaming data processing
- **Characteristics**: Low latency, memory efficient, immediate processing
- **Performance**: Optimized for streaming, minimal buffering

### Bulk Processing
- **Use Case**: Medium-sized datasets, batch processing
- **Characteristics**: Higher throughput, pandas DataFrames, batch operations
- **Performance**: Optimized for batch operations, memory management

### BigData Processing
- **Use Case**: Large datasets, distributed processing
- **Characteristics**: Dask DataFrames, lazy evaluation, distributed computing
- **Performance**: Scalable to large datasets, distributed execution

## JSON Configuration Examples

### Component Configuration
```json
{
  "component_type": "filter",
  "processing_strategy": "bulk",
  "filter_rules": [
    {
      "field": "age",
      "operator": ">",
      "value": 18
    }
  ]
}
```

### Pipeline Configuration
```json
{
  "pipeline": [
    {
      "component": "csv_reader",
      "config": {
        "file_path": "input.csv"
      }
    },
    {
      "component": "filter",
      "config": {
        "filter_rules": [
          {
            "field": "status",
            "operator": "==",
            "value": "active"
          }
        ]
      }
    },
    {
      "component": "csv_writer",
      "config": {
        "file_path": "output.csv"
      }
    }
  ]
}
```

## Component Features

### Data Operations
- **Flexible Processing**: Support for row, bulk, and bigdata processing
- **Type Safety**: Comprehensive type checking and validation
- **Error Handling**: Robust error handling and recovery mechanisms
- **Performance**: Optimized processing for various data scales

### Database Components
- **Multi-Database Support**: Support for various database systems
- **Connection Management**: Efficient connection pooling and management
- **Query Optimization**: Optimized queries for better performance
- **Transaction Support**: Full ACID transaction support

### File Components
- **Multi-Format Support**: Support for various file formats
- **Streaming**: Efficient streaming for large files
- **Error Handling**: Robust error handling for file operations
- **Performance**: Optimized file I/O operations

## Error Handling

### Component Errors
- **Clear Messages**: Descriptive error messages for component issues
- **Error Context**: Detailed error information and context
- **Recovery**: Graceful error recovery mechanisms
- **Logging**: Comprehensive error logging and monitoring

### Error Types
- **Data Errors**: Data validation and processing errors
- **Connection Errors**: Database and file connection errors
- **Configuration Errors**: Component configuration errors
- **Processing Errors**: Data processing and transformation errors

### Error Reporting
```json
{
  "component_error": {
    "component_type": "filter",
    "error_type": "validation_failed",
    "message": "Filter rule validation failed",
    "context": {
      "field": "age",
      "rule": "age > 18"
    }
  }
}
```

## Performance Considerations

### Processing Optimization
- **Strategy Selection**: Choose appropriate processing strategy
- **Memory Management**: Optimize memory usage for large datasets
- **Parallel Processing**: Use parallel processing where beneficial
- **Caching**: Implement caching for frequently used data

### Database Optimization
- **Connection Pooling**: Use connection pooling for better performance
- **Query Optimization**: Optimize queries for better performance
- **Indexing**: Use appropriate indexes for better query performance
- **Batch Operations**: Use batch operations for better performance

## Configuration

### Component Options
- **Processing Strategy**: Configure processing strategy (row, bulk, bigdata)
- **Error Handling**: Configure error handling strategies
- **Performance Tuning**: Optimize component performance
- **Monitoring**: Configure monitoring and logging

## Best Practices

### Component Design
- **Single Responsibility**: Focus on specific data operations
- **Error Handling**: Implement robust error handling
- **Performance**: Optimize for your use case
- **Testing**: Test components thoroughly

### Pipeline Design
- **Modular Design**: Use modular component design
- **Error Recovery**: Implement error recovery mechanisms
- **Monitoring**: Monitor pipeline performance and errors
- **Documentation**: Document component usage and configuration
