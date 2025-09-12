# Receivers

Receiver components provide **data processing operations**, **specialized implementations**, and **component logic** for ETL pipeline data with comprehensive support for different processing strategies and error handling.

## Overview

Receiver components define the structure, processing logic, and implementation patterns for data flowing through the ETL pipeline, ensuring proper data processing and type safety. They provide **comprehensive processing support** and **implementation capabilities** for robust data operations.

### Key Concepts
- **Data Processing**: Specialized data processing logic for different operations
- **Component Separation**: Clean separation between interfaces and implementations
- **Processing Strategies**: Support for row, bulk, and bigdata processing
- **Error Handling**: Comprehensive error handling and recovery

## Components

### [Base Receiver](./base_receiver.py)
- **File**: `base_receiver.py`
- **Purpose**: Abstract base class for all receivers
- **Features**: Common interface, unique identification, base functionality

### [Data Operations Receivers](./data_operations_receivers/README.md)
Receivers for data transformation and manipulation operations.

### [Database Receivers](./databases/README.md)
Receivers for database operations across different database systems.

### [File Receivers](./files/README.md)
Receivers for file operations across different file formats.

## Receiver Types

### Data Operations Receivers
- **Aggregation**: Group-by and aggregation operations
- **Filter**: Data filtering and comparison operations
- **Merge**: Data merging and combining operations
- **Schema Mapping**: Schema transformation and mapping
- **Split**: Data splitting and distribution operations

### Database Receivers
- **Read Operations**: Query execution and result processing
- **Write Operations**: Insert, update, delete operations
- **Connection Management**: Database connection handling
- **Transaction Management**: Transaction processing
- **Query Optimization**: Query performance optimization

### File Receivers
- **Read Operations**: File reading and parsing
- **Write Operations**: File writing and formatting
- **Format Support**: Multiple file format support
- **Streaming**: Streaming file processing
- **Error Handling**: File operation error handling

### Features
- **Processing Strategies**: Support for row, bulk, and bigdata processing
- **Metrics Integration**: Performance metrics collection
- **Error Handling**: Comprehensive error handling and recovery
- **Resource Management**: Efficient resource management

## JSON Configuration Examples

### Receiver Configuration
```json
{
  "receiver": {
    "type": "filter_receiver",
    "processing_strategy": "bulk",
    "parameters": {
      "filter_rules": [
        {
          "field": "status",
          "operator": "==",
          "value": "active"
        }
      ]
    },
    "resource_limits": {
      "max_memory": "1GB",
      "max_cpu": 80
    }
  }
}
```

### Data Operations Receiver Configuration
```json
{
  "data_operations_receiver": {
    "type": "aggregation_receiver",
    "processing_strategy": "bigdata",
    "parameters": {
      "group_by": ["category", "region"],
      "aggregations": [
        {
          "field": "amount",
          "function": "sum"
        },
        {
          "field": "count",
          "function": "count"
        }
      ]
    }
  }
}
```

## Receiver Features

### Data Processing
- **Processing Logic**: Implement specific data processing logic
- **Error Handling**: Handle processing errors gracefully
- **Metrics Collection**: Collect performance and processing metrics
- **Resource Management**: Manage resources efficiently
- **Validation**: Validate input data and processing results

## Error Handling

### Receiver Errors
- **Clear Messages**: Descriptive error messages for receiver issues
- **Processing Validation**: Path-based error reporting for processing problems
- **Resource Errors**: Detailed resource error information
- **Context**: Receiver and processing context in error messages

### Error Types
- **Input Validation Errors**: Invalid input data
- **Processing Errors**: Errors during data processing
- **Resource Errors**: Memory or CPU resource errors
- **Configuration Errors**: Invalid configuration parameters

### Error Reporting
```json
{
  "receiver_error": {
    "error_type": "processing_error",
    "receiver_type": "filter_receiver",
    "processing_strategy": "bulk",
    "message": "Failed to process data batch"
  }
}
```

## Performance Considerations

### Receiver Performance
- **Processing Efficiency**: Optimize receiver processing performance
- **Memory Management**: Optimize memory usage for different strategies
- **Resource Utilization**: Efficient resource utilization
- **Scalability**: Optimize scalability for different processing modes

### Processing Strategies
- **Row Processing**: Optimize for memory efficiency
- **Bulk Processing**: Optimize for medium datasets
- **BigData Processing**: Optimize for large datasets
- **Performance**: Balance processing efficiency with resource usage

## Configuration

### Receiver Options
- **Processing Strategy**: Configure processing strategy (row, bulk, bigdata)
- **Resource Limits**: Set memory and CPU limits
- **Error Handling**: Configure error handling strategies
- **Metrics Collection**: Configure metrics collection settings

### Performance Tuning
- **Memory Management**: Optimize memory usage for different strategies
- **Processing Efficiency**: Optimize processing algorithms
- **Resource Utilization**: Efficiently use available resources
- **Scalability**: Configure scalability settings

## Best Practices

### Receiver Design
- **Single Responsibility**: Each receiver has a single responsibility
- **Error Handling**: Implement comprehensive error handling
- **Resource Management**: Manage resources efficiently
- **Metrics Collection**: Collect relevant performance metrics

### Performance Optimization
- **Memory Management**: Optimize memory usage
- **Processing Efficiency**: Optimize processing algorithms
- **Resource Utilization**: Efficiently use available resources
- **Monitoring**: Monitor receiver performance

### Error Handling
- **Validation**: Validate inputs thoroughly
- **Error Recovery**: Implement appropriate recovery mechanisms
- **Logging**: Log errors and processing information
- **Monitoring**: Monitor error rates and patterns
