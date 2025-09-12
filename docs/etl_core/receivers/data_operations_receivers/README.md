# Data Operations Receivers

This directory contains receivers for data transformation and manipulation operations.

## Overview

Data operations receivers implement the actual data processing logic for various data transformation operations, providing a clean separation between component interfaces and processing implementations.

## Components

### [Aggregation Receivers](./aggregation/README.md)
Receivers for group-by and aggregation operations.

### [Filter Receivers](./filter/README.md)
Receivers for data filtering and comparison operations.

### [Merge Receivers](./merge/README.md)
Receivers for data merging and combining operations.

### [Schema Mapping Receivers](./schema_mapping/README.md)
Receivers for schema transformation and mapping operations.

### [Split Receivers](./split/README.md)
Receivers for data splitting and distribution operations.

### [Data Operations Receiver](./data_operations_receiver.py)
- **File**: `data_operations_receiver.py`
- **Purpose**: Base receiver for data operations
- **Features**: Common data operations functionality, base implementation

## Receiver Architecture

### Base Data Operations Receiver
All data operations receivers inherit from the base data operations receiver, providing:
- **Common Interface**: Standardized interface for data operations
- **Processing Methods**: Row, bulk, and bigdata processing methods
- **Error Handling**: Common error handling patterns
- **Metrics Integration**: Metrics collection integration

### Receiver Responsibilities
- **Data Processing**: Implement specific data processing logic
- **Error Handling**: Handle processing errors gracefully
- **Metrics Collection**: Collect performance and processing metrics
- **Resource Management**: Manage resources efficiently
- **Validation**: Validate input data and processing results

## Processing Modes

### Row Processing
- **Streaming**: Process data row by row
- **Memory Efficient**: Low memory usage
- **Real-time**: Immediate processing
- **Use Case**: Real-time processing, memory-constrained environments

### Bulk Processing
- **Batch Processing**: Process data in batches
- **DataFrame Processing**: Pandas DataFrame processing
- **Memory Intensive**: Higher memory usage
- **Use Case**: Medium-sized datasets, batch processing

### BigData Processing
- **Distributed Processing**: Dask DataFrame processing
- **Scalable**: Handles large datasets
- **Parallel Processing**: Multi-core processing
- **Use Case**: Large datasets, distributed processing

## Common Features

### Data Validation
- **Input Validation**: Validate input data
- **Type Checking**: Check data types
- **Schema Validation**: Validate against schemas
- **Range Validation**: Validate data ranges
- **Format Validation**: Validate data formats

### Error Handling
- **Validation Errors**: Handle validation errors
- **Processing Errors**: Handle processing errors
- **Resource Errors**: Handle resource errors
- **Recovery**: Implement error recovery
- **Logging**: Log errors appropriately

### Metrics Collection
- **Processing Time**: Track processing time
- **Data Volume**: Track data volume processed
- **Error Count**: Track error count
- **Resource Usage**: Track resource usage
- **Throughput**: Track processing throughput

## Implementation Pattern

### Receiver Interface
```python
class DataOperationsReceiver(Receiver):
    async def process_row(self, data, **kwargs):
        # Row processing implementation
        pass

    async def process_bulk(self, data, **kwargs):
        # Bulk processing implementation
        pass

    async def process_bigdata(self, data, **kwargs):
        # BigData processing implementation
        pass
```

### Metrics Integration
- **Performance Metrics**: Collect performance metrics
- **Processing Metrics**: Collect processing metrics
- **Error Metrics**: Collect error metrics
- **Resource Metrics**: Collect resource metrics
- **Custom Metrics**: Collect custom metrics

## Configuration

### Receiver Configuration
- **Processing Parameters**: Strategy-specific parameters
- **Resource Limits**: Memory and CPU limits
- **Error Handling**: Error handling configuration
- **Metrics Collection**: Metrics collection settings
- **Validation Rules**: Data validation rules

### Component Integration
- **Interface Methods**: Standardized interface methods
- **Data Envelopes**: Data envelope system integration
- **Metrics Objects**: Metrics objects integration
- **Error Handling**: Error handling integration
- **Resource Management**: Resource management integration

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

## Best Practices

### Receiver Design
- **Single Responsibility**: Each receiver has a single responsibility
- **Error Handling**: Implement comprehensive error handling
- **Resource Management**: Manage resources efficiently
- **Metrics Collection**: Collect relevant performance metrics
- **Testing**: Implement comprehensive testing

### Performance Optimization
- **Memory Management**: Optimize memory usage
- **Processing Efficiency**: Optimize processing algorithms
- **Resource Utilization**: Efficiently use available resources
- **Monitoring**: Monitor receiver performance
- **Profiling**: Profile receiver performance

### Error Handling
- **Validation**: Validate inputs thoroughly
- **Error Recovery**: Implement appropriate recovery mechanisms
- **Logging**: Log errors and processing information
- **Monitoring**: Monitor error rates and patterns
- **Alerting**: Implement error alerting
