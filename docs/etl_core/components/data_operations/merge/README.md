# Merge Component

The MergeComponent provides **efficient data routing and consolidation** capabilities for multi-source data integration scenarios with comprehensive support for different processing strategies and data types.

## Overview

The MergeComponent provides efficient data routing and consolidation capabilities for multi-source data integration scenarios. It acts as a sophisticated forwarding mechanism that routes payloads from multiple named input ports to a single output port, enabling **data unification**, pipeline branching, and multi-source data consolidation without data modification or aggregation.

## Components

### Merge Functions
- **Data Routing**: Route data from multiple inputs to single output
- **Payload Preservation**: Maintain original data structure and content
- **Port Management**: Intelligent port detection and management
- **Envelope Support**: Full support for tagged data envelopes

## Merge Types

### Data Routing
Routes data from multiple input ports to a single output port.

#### Features
- **Multi-Input Support**: Accepts data from multiple input ports simultaneously
- **Single Output Routing**: Routes all incoming data to a single output port
- **Payload Preservation**: Maintains original data structure and content
- **Port Management**: Intelligent port detection and management

### Processing Strategies
Supports different processing strategies for various data types and sizes.

#### Features
- **Row Processing**: Real-time streaming data forwarding
- **Bulk Processing**: Batch DataFrame forwarding
- **BigData Processing**: Distributed Dask DataFrame forwarding
- **Tagged Envelope Support**: Full support for tagged data envelopes

### Performance Optimization
Optimized for high-performance data routing scenarios.

#### Features
- **Zero-Copy Forwarding**: Minimal memory overhead
- **Immediate Processing**: No buffering or queuing
- **Memory Efficiency**: No data accumulation or combination
- **High Throughput**: Optimized for high-volume data streams

## JSON Configuration Examples

### Basic Merge Component

```json
{
  "name": "data_merger",
  "description": "Merge data from multiple sources",
  "comp_type": "merge",
  "strategy_type": "bulk"
}
```

### Advanced Merge with Monitoring

```json
{
  "name": "monitored_merger",
  "description": "Merge component with performance monitoring",
  "comp_type": "merge",
  "strategy_type": "bulk",
  "enable_metrics": true,
  "enable_port_statistics": true,
  "enable_health_checks": true
}
```

### Real-Time Stream Merging

```json
{
  "name": "stream_merger",
  "description": "Merge real-time data streams",
  "comp_type": "merge",
  "strategy_type": "row",
  "buffer_size": 1000,
  "enable_metrics": true
}
```

### BigData Processing

```json
{
  "name": "bigdata_merger",
  "description": "Merge large datasets",
  "comp_type": "merge",
  "strategy_type": "bigdata",
  "chunk_size": 10000,
  "enable_metrics": true
}
```

## Processing Strategies

### Row Processing

**Row processing** provides **immediate forwarding** of individual data records with minimal latency. This strategy is ideal for **real-time streaming scenarios** where you need to process data as it arrives without buffering or queuing. The component maintains **low memory usage** by forwarding data immediately without accumulation.

**Characteristics:**
- **Immediate Processing**: No buffering or queuing
- **Low Latency**: Minimal processing delay
- **Memory Efficient**: No data accumulation
- **Real-time**: Suitable for streaming data

### Bulk Processing

**Bulk processing** handles **entire DataFrames** at once, providing efficient batch processing capabilities. This strategy is perfect for **scheduled reports** and data warehouse updates where you have complete datasets available for processing. The component leverages **pandas' optimized operations** for excellent performance.

**Characteristics:**
- **Batch Processing**: Handles entire DataFrames
- **No Concatenation**: Forwards DataFrames as-is
- **Memory Management**: Efficient DataFrame handling
- **High Throughput**: Optimized for batch operations

### BigData Processing

**BigData processing** uses **Dask DataFrames** for distributed processing of large datasets. This strategy allows you to process datasets of virtually any size by **distributing the work** across multiple cores or machines. The component maintains **lazy evaluation** to defer computations until they're actually needed.

**Characteristics:**
- **Distributed Processing**: Handles large datasets
- **Lazy Evaluation**: Maintains Dask's lazy evaluation
- **No Materialization**: Forwards without computing
- **Scalable**: Suitable for big data scenarios

## Merge Features

### Data Routing
- **Multi-Input Support**: Accepts data from multiple input ports simultaneously
- **Single Output Routing**: Routes all incoming data to a single output port
- **Payload Preservation**: Maintains original data structure and content
- **Port Management**: Intelligent port detection and management

### Envelope Support
- **Tagged Envelopes**: Full support for tagged data envelopes
- **Metadata Preservation**: Preserves metadata and routing information
- **Data Lineage**: Maintains data lineage across components
- **Context Preservation**: Preserves processing context

### Performance Optimization
- **Zero-Copy Forwarding**: Minimal memory overhead
- **Immediate Processing**: No buffering or queuing
- **Memory Efficiency**: No data accumulation or combination
- **High Throughput**: Optimized for high-volume data streams

## Error Handling

### Merge Errors
- **Clear Messages**: Descriptive error messages for merge failures
- **Port Validation**: Path-based error reporting for port issues
- **Data Validation**: Detailed data validation information
- **Context**: Data and routing context in error messages

### Error Types
- **Port Errors**: Missing or invalid input/output ports
- **Data Errors**: Invalid or corrupted data in streams
- **Routing Errors**: Data routing failures
- **Envelope Errors**: Tagged envelope processing errors

### Error Reporting
```json
{
  "merge_error": {
    "port_name": "input_port_1",
    "error_type": "data_corruption",
    "message": "Invalid data format in input stream"
  }
}
```

## Use Cases

### Data Unification
- **Multi-Source Integration**: Combine data from multiple sources
- **Data Consolidation**: Unify data streams into single processing path
- **Pipeline Branching**: Merge data from different pipeline branches
- **Data Routing**: Route data from multiple sources to single destination

### Real-time Processing
- **Stream Processing**: Merge real-time data streams
- **Event Processing**: Combine events from multiple sources
- **Data Broadcasting**: Distribute data to multiple consumers
- **Load Balancing**: Balance data load across processing units

### Batch Processing
- **Batch Consolidation**: Merge batch data from multiple sources
- **Data Warehousing**: Consolidate data for warehousing
- **ETL Pipelines**: Integrate data in ETL processes
- **Data Migration**: Merge data during migration processes

## Performance Considerations

### Row Processing
- **Immediate Processing**: Forwards data as it arrives
- **Memory Efficient**: Low memory usage for streaming data
- **Real-time**: Immediate data forwarding
- **High Throughput**: Optimized for high-volume streams

### Bulk Processing
- **Batch Processing**: Forwards entire DataFrames at once
- **High Performance**: Optimized for medium-sized datasets
- **Memory Management**: Efficient DataFrame handling
- **Zero-Copy**: Minimal memory overhead

### BigData Processing
- **Distributed Processing**: Handles large datasets across multiple cores
- **Lazy Evaluation**: Maintains Dask's lazy evaluation
- **Scalable**: Processes datasets of virtually any size
- **Performance**: Optimized for big data scenarios

## Configuration

### Merge Options
- **Port Configuration**: Configure input/output ports
- **Envelope Support**: Enable tagged envelope support
- **Error Handling**: Set error handling strategies
- **Performance Tuning**: Configure performance parameters

### Port Management
- **Port Detection**: Automatic port detection and management
- **Port Validation**: Validate port configurations
- **Port Statistics**: Track port usage and performance
- **Port Monitoring**: Monitor port health and errors

## Best Practices

### Component Design
- **Single Responsibility**: Focus on data routing only
- **Minimal Processing**: Avoid data modification or transformation
- **Efficient Forwarding**: Optimize for high throughput
- **Error Handling**: Implement robust error handling

### Performance
- **Memory Management**: Monitor memory usage during forwarding
- **Throughput Optimization**: Optimize for high data volumes
- **Parallel Processing**: Use parallel processing when appropriate
- **Monitoring**: Implement comprehensive performance monitoring

### Error Handling
- **Data Validation**: Validate input data before forwarding
- **Error Recovery**: Implement graceful error recovery mechanisms
- **Logging**: Log all errors with sufficient context
- **Monitoring**: Monitor error rates and patterns

### Data Quality
- **Data Validation**: Validate input data before forwarding
- **Schema Validation**: Use schema validation for data quality
- **Error Recovery**: Implement graceful error recovery mechanisms
- **Monitoring**: Monitor data quality and processing errors
