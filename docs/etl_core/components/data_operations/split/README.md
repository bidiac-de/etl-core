# Split Component

The SplitComponent provides **sophisticated data distribution and fan-out capabilities** for parallel processing scenarios with comprehensive support for different processing strategies and data types.

## Overview

The SplitComponent provides sophisticated data distribution and fan-out capabilities for parallel processing scenarios. It efficiently duplicates incoming payloads to multiple defined output ports, enabling data to be processed by **multiple downstream components simultaneously** while maintaining data integrity and performance.

## Components

### Split Functions
- **Data Distribution**: Distribute data to multiple output ports
- **Data Duplication**: Create identical copies for each output port
- **Payload Preservation**: Maintain original data structure and content
- **Port Management**: Intelligent output port detection and management

## Split Types

### Data Distribution
Distributes data to multiple output ports for parallel processing.

#### Features
- **Multi-Output Support**: Distributes data to multiple output ports
- **Data Duplication**: Creates identical copies for each output port
- **Payload Preservation**: Maintains original data structure and content
- **Port Management**: Intelligent output port detection and management

### Processing Strategies
Supports different processing strategies for various data types and sizes.

#### Features
- **Row Processing**: Real-time streaming data duplication
- **Bulk Processing**: Batch DataFrame duplication
- **BigData Processing**: Distributed Dask DataFrame duplication
- **Tagged Envelope Support**: Full support for tagged data envelopes

### Performance Optimization
Optimized for high-performance data distribution scenarios.

#### Features
- **Efficient Duplication**: Optimized data copying mechanisms
- **Memory Management**: Smart memory usage for large datasets
- **Parallel Distribution**: Concurrent data distribution to outputs
- **High Throughput**: Optimized for high-volume data streams

## JSON Configuration Examples

### Basic Split Component

```json
{
  "name": "data_splitter",
  "description": "Split data to multiple outputs",
  "comp_type": "split",
  "output_ports": ["analytics", "reporting"],
  "strategy_type": "bulk"
}
```

### Advanced Split with Multiple Outputs

```json
{
  "name": "multi_destination_splitter",
  "description": "Split data to multiple specialized outputs",
  "comp_type": "split",
  "output_ports": [
    "real_time_processing",
    "batch_processing",
    "data_warehouse",
    "backup_storage",
    "monitoring"
  ],
  "strategy_type": "bulk",
  "enable_metrics": true,
  "enable_port_statistics": true,
  "enable_health_checks": true
}
```

### Real-Time Stream Splitting

```json
{
  "name": "stream_splitter",
  "description": "Split real-time data streams",
  "comp_type": "split",
  "output_ports": ["analytics", "monitoring", "backup"],
  "strategy_type": "row",
  "buffer_size": 1000,
  "enable_metrics": true
}
```

### BigData Processing

```json
{
  "name": "bigdata_splitter",
  "description": "Split large datasets",
  "comp_type": "split",
  "output_ports": ["analytics", "reporting", "backup"],
  "strategy_type": "bigdata",
  "chunk_size": 10000,
  "enable_metrics": true
}
```

## Processing Strategies

### Row Processing

**Row processing** provides **immediate duplication** of individual data records with minimal latency. This strategy is ideal for **real-time streaming scenarios** where you need to distribute data to multiple processing paths as it arrives. The component uses **optimized copying mechanisms** for small data structures.

**Characteristics:**
- **Immediate Duplication**: No buffering or queuing
- **Low Latency**: Minimal processing delay
- **Memory Efficient**: Optimized copying for small data
- **Real-time**: Suitable for streaming data

### Bulk Processing

**Bulk processing** handles **entire DataFrames** at once, providing efficient batch duplication capabilities. This strategy is perfect for **scheduled reports** and data warehouse updates where you have complete datasets available for processing. The component uses **efficient DataFrame copying** mechanisms.

**Characteristics:**
- **Batch Processing**: Handles entire DataFrames
- **Efficient Copying**: Optimized DataFrame copying
- **Memory Management**: Smart memory usage for large DataFrames
- **High Throughput**: Optimized for batch operations

### BigData Processing

**BigData processing** uses **Dask DataFrames** for distributed duplication of large datasets. This strategy allows you to process datasets of virtually any size by **distributing the work** across multiple cores or machines. The component maintains **lazy evaluation** to defer computations until they're actually needed.

**Characteristics:**
- **Distributed Processing**: Handles large datasets
- **Lazy Evaluation**: Maintains Dask's lazy evaluation
- **Reference Copying**: Efficient copying for Dask DataFrames
- **Scalable**: Suitable for big data scenarios

## Split Features

### Data Distribution
- **Multi-Output Support**: Distributes data to multiple output ports
- **Data Duplication**: Creates identical copies for each output port
- **Payload Preservation**: Maintains original data structure and content
- **Port Management**: Intelligent output port detection and management

### Envelope Support
- **Tagged Envelopes**: Full support for tagged data envelopes
- **Metadata Preservation**: Preserves metadata and routing information
- **Data Lineage**: Maintains data lineage across components
- **Context Preservation**: Preserves processing context

### Performance Optimization
- **Efficient Duplication**: Optimized data copying mechanisms
- **Memory Management**: Smart memory usage for large datasets
- **Parallel Distribution**: Concurrent data distribution to outputs
- **High Throughput**: Optimized for high-volume data streams

## Error Handling

### Split Errors
- **Clear Messages**: Descriptive error messages for split failures
- **Port Validation**: Path-based error reporting for port issues
- **Data Validation**: Detailed data validation information
- **Context**: Data and distribution context in error messages

### Error Types
- **Port Errors**: Missing or invalid output ports
- **Data Errors**: Invalid or corrupted data in streams
- **Distribution Errors**: Data distribution failures
- **Envelope Errors**: Tagged envelope processing errors

### Error Reporting
```json
{
  "split_error": {
    "port_name": "output_port_1",
    "error_type": "distribution_failure",
    "message": "Failed to distribute data to output port"
  }
}
```

## Use Cases

### Fan-Out Processing
- **Parallel Processing**: Enable parallel processing of identical data
- **Multi-Pipeline Processing**: Send same data to multiple processing pipelines
- **Load Distribution**: Distribute data load across multiple processing units
- **Redundancy**: Create redundant processing paths for reliability

### Data Distribution
- **Multi-Destination Routing**: Send data to multiple destinations
- **Data Backup**: Create backup copies of data streams
- **Data Broadcasting**: Distribute data to multiple consumers
- **Pipeline Branching**: Create multiple processing branches from single source

### Real-time Processing
- **Stream Processing**: Distribute real-time data streams
- **Event Processing**: Broadcast events to multiple handlers
- **Data Replication**: Replicate data for different use cases
- **Monitoring**: Send data to monitoring and alerting systems

## Performance Considerations

### Row Processing
- **Immediate Processing**: Distributes data as it arrives
- **Memory Efficient**: Low memory usage for streaming data
- **Real-time**: Immediate data distribution
- **High Throughput**: Optimized for high-volume streams

### Bulk Processing
- **Batch Processing**: Distributes entire DataFrames at once
- **High Performance**: Optimized for medium-sized datasets
- **Memory Management**: Efficient DataFrame handling
- **Efficient Copying**: Optimized data copying mechanisms

### BigData Processing
- **Distributed Processing**: Handles large datasets across multiple cores
- **Lazy Evaluation**: Maintains Dask's lazy evaluation
- **Scalable**: Processes datasets of virtually any size
- **Reference Copying**: Efficient copying for Dask DataFrames

## Configuration

### Split Options
- **Output Ports**: Configure output ports for data distribution
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
- **Single Responsibility**: Focus on data distribution only
- **Efficient Duplication**: Optimize data copying mechanisms
- **Port Management**: Properly manage output ports
- **Error Handling**: Implement robust error handling

### Performance
- **Memory Management**: Monitor memory usage during duplication
- **Copying Strategies**: Use appropriate copying strategies for data types
- **Parallel Distribution**: Use parallel distribution when beneficial
- **Monitoring**: Implement comprehensive performance monitoring

### Error Handling
- **Data Validation**: Validate input data before distribution
- **Port Error Handling**: Handle errors per output port
- **Error Recovery**: Implement graceful error recovery mechanisms
- **Logging**: Log all errors with sufficient context

### Data Quality
- **Data Validation**: Validate input data before distribution
- **Schema Validation**: Use schema validation for data quality
- **Error Recovery**: Implement graceful error recovery mechanisms
- **Monitoring**: Monitor data quality and processing errors
