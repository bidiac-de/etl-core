# Receivers

This directory contains receiver components that handle data processing operations for different component types.

## Overview

Receivers are specialized components that implement the actual data processing logic for different types of operations. They provide a clean separation between component interfaces and processing implementations.

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

## Receiver Architecture

### Base Receiver
All receivers inherit from the base receiver class:
```python
from abc import ABC
from pydantic import BaseModel, PrivateAttr
from uuid import uuid4

class Receiver(BaseModel, ABC):
    """Base class for all receivers in the system"""
    
    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))
```

### Receiver Responsibilities
- **Data Processing**: Implement specific data processing logic
- **Error Handling**: Handle processing errors gracefully
- **Metrics Collection**: Collect performance and processing metrics
- **Resource Management**: Manage resources efficiently
- **Validation**: Validate input data and processing results

## Receiver Types

### Data Operations Receivers
Handle data transformation operations:
- **Aggregation**: Group-by and aggregation operations
- **Filter**: Data filtering and comparison operations
- **Merge**: Data merging and combining operations
- **Schema Mapping**: Schema transformation and mapping
- **Split**: Data splitting and distribution operations

### Database Receivers
Handle database operations:
- **Read Operations**: Query execution and result processing
- **Write Operations**: Insert, update, delete operations
- **Connection Management**: Database connection handling
- **Transaction Management**: Transaction processing
- **Query Optimization**: Query performance optimization

### File Receivers
Handle file operations:
- **Read Operations**: File reading and parsing
- **Write Operations**: File writing and formatting
- **Format Support**: Multiple file format support
- **Streaming**: Streaming file processing
- **Error Handling**: File operation error handling

## Implementation Pattern

### Receiver Interface
Receivers implement specific methods for different processing modes:
- **Row Processing**: Process individual records
- **Bulk Processing**: Process data batches
- **BigData Processing**: Process large datasets

### Metrics Integration
Receivers collect metrics for:
- **Processing Time**: Time spent processing data
- **Data Volume**: Amount of data processed
- **Error Count**: Number of errors encountered
- **Resource Usage**: Memory and CPU usage
- **Throughput**: Data processing rate

### Error Handling
Receivers handle errors by:
- **Validation**: Validating input data
- **Error Recovery**: Implementing recovery mechanisms
- **Error Reporting**: Providing clear error messages
- **Resource Cleanup**: Cleaning up resources on errors

## Configuration

### Receiver Configuration
Receivers are configured with:
- **Processing Parameters**: Strategy-specific parameters
- **Resource Limits**: Memory and CPU limits
- **Error Handling**: Error handling configuration
- **Metrics Collection**: Metrics collection settings

### Component Integration
Receivers integrate with components through:
- **Interface Methods**: Standardized interface methods
- **Data Envelopes**: Data envelope system for data passing
- **Metrics Objects**: Metrics objects for performance tracking
- **Error Handling**: Error handling integration

## Performance Characteristics

### Memory Usage
- **Row Receivers**: Low memory usage
- **Bulk Receivers**: Medium to high memory usage
- **BigData Receivers**: Distributed memory usage

### Processing Speed
- **Row Receivers**: Slower but memory efficient
- **Bulk Receivers**: Fast for medium datasets
- **BigData Receivers**: Fast for large datasets

### Scalability
- **Row Receivers**: Limited scalability
- **Bulk Receivers**: Limited by memory
- **BigData Receivers**: Highly scalable

## Error Handling

### Common Error Types
- **Input Validation Errors**: Invalid input data
- **Processing Errors**: Errors during data processing
- **Resource Errors**: Memory or CPU resource errors
- **Configuration Errors**: Invalid configuration parameters

### Error Recovery
- **Retry Logic**: Automatic retry mechanisms
- **Fallback Strategies**: Alternative processing strategies
- **Resource Cleanup**: Cleanup on error conditions
- **Error Reporting**: Clear error messages and logging

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
