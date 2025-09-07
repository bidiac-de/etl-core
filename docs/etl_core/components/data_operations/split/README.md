# Split Component Documentation

## Overview

The SplitComponent provides sophisticated data distribution and fan-out capabilities for parallel processing scenarios. It efficiently duplicates incoming payloads to multiple defined output ports, enabling data to be processed by multiple downstream components simultaneously while maintaining data integrity and performance.

## Core Features

### Data Distribution
- **Multi-Output Support**: Distributes data to multiple output ports
- **Data Duplication**: Creates identical copies for each output port
- **Payload Preservation**: Maintains original data structure and content
- **Port Management**: Intelligent output port detection and management

### Processing Strategies
- **Row Processing**: Real-time streaming data duplication
- **Bulk Processing**: Batch DataFrame duplication
- **BigData Processing**: Distributed Dask DataFrame duplication
- **Tagged Envelope Support**: Full support for tagged data envelopes

### Performance Optimization
- **Efficient Duplication**: Optimized data copying mechanisms
- **Memory Management**: Smart memory usage for large datasets
- **Parallel Distribution**: Concurrent data distribution to outputs
- **High Throughput**: Optimized for high-volume data streams

## Component Architecture

### SplitComponent Structure

```python
@register_component("split")
class SplitComponent(BaseComponent):
    """
    Split component for data distribution:
    - input:  'in' (required, fanin="one") -> data from single source
    - output: multiple ports (fanout="many") -> distributed data streams
    """
    
    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="one"),)
    OUTPUT_PORTS = (
        OutPortSpec(name="output1", required=True, fanout="many"),
        OutPortSpec(name="output2", required=True, fanout="many"),
        # Additional output ports as needed
    )
    
    # Output ports are defined at component creation
    # At least one output port is required
```

### Output Port Management

```python
class OutputPortManager:
    """Manages output port operations and distribution."""
    
    def __init__(self, output_ports: List[OutPortSpec]):
        self.output_ports = {port.name: port for port in output_ports}
        self.port_stats = {}
        self.distribution_strategies = {}
    
    def register_output_port(self, port_name: str, port_spec: OutPortSpec):
        """Register an output port."""
        self.output_ports[port_name] = port_spec
        self.port_stats[port_name] = {
            "messages_sent": 0,
            "bytes_sent": 0,
            "last_message_time": None,
            "error_count": 0
        }
    
    def get_distribution_strategy(self, data_type: type) -> str:
        """Get optimal distribution strategy for data type."""
        if data_type == pd.DataFrame:
            return "dataframe_copy"
        elif data_type == dd.DataFrame:
            return "dask_reference"
        else:
            return "deep_copy"
```

## Processing Strategies

### Row Processing
Processes individual rows with immediate duplication:

```python
async def process_row(self, row: Dict[str, Any], metrics: SplitMetrics) -> AsyncIterator[Out]:
    """
    Process individual rows with immediate duplication to all outputs.
    """
    # Update metrics
    metrics.rows_processed += 1
    metrics.bytes_processed += len(str(row))
    
    # Duplicate row to all output ports
    for port_name in self.output_ports:
        # Create copy of row for each output
        row_copy = copy.deepcopy(row)
        
        # Forward to output port
        yield Out(port=port_name, payload=row_copy)
        
        # Update port statistics
        self._update_port_stats(port_name, len(str(row)), 0.0)
    
    # Log processing info
    self._log_processing_info(row, metrics)
```

**Characteristics:**
- **Immediate Duplication**: No buffering or queuing
- **Low Latency**: Minimal processing delay
- **Memory Efficient**: Optimized copying for small data
- **Real-time**: Suitable for streaming data

### Bulk Processing
Processes pandas DataFrames with efficient duplication:

```python
async def process_bulk(self, dataframe: pd.DataFrame, metrics: SplitMetrics) -> AsyncIterator[Out]:
    """
    Process DataFrame with efficient duplication to all outputs.
    """
    # Update metrics
    metrics.rows_processed += len(dataframe)
    metrics.bytes_processed += dataframe.memory_usage(deep=True).sum()
    
    # Duplicate DataFrame to all output ports
    for port_name in self.output_ports:
        # Create copy of DataFrame for each output
        df_copy = dataframe.copy(deep=True)
        
        # Forward to output port
        yield Out(port=port_name, payload=df_copy)
        
        # Update port statistics
        self._update_port_stats(port_name, len(dataframe), 0.0)
    
    # Log processing info
    self._log_bulk_processing_info(dataframe, metrics)
```

**Characteristics:**
- **Batch Processing**: Handles entire DataFrames
- **Efficient Copying**: Optimized DataFrame copying
- **Memory Management**: Smart memory usage for large DataFrames
- **High Throughput**: Optimized for batch operations

### BigData Processing
Processes Dask DataFrames with distributed duplication:

```python
async def process_bigdata(self, ddf: dd.DataFrame, metrics: SplitMetrics) -> AsyncIterator[Out]:
    """
    Process Dask DataFrame with distributed duplication to all outputs.
    """
    # Update metrics
    metrics.rows_processed += len(ddf) if hasattr(ddf, '__len__') else 0
    metrics.bytes_processed += ddf.memory_usage(deep=True).sum().compute()
    
    # Duplicate Dask DataFrame to all output ports
    for port_name in self.output_ports:
        # Create reference copy for Dask DataFrame (lazy evaluation)
        ddf_copy = ddf.copy()
        
        # Forward to output port
        yield Out(port=port_name, payload=ddf_copy)
        
        # Update port statistics
        self._update_port_stats(port_name, 0, 0.0)  # Dask size calculation is expensive
    
    # Log processing info
    self._log_bigdata_processing_info(ddf, metrics)
```

**Characteristics:**
- **Distributed Processing**: Handles large datasets
- **Lazy Evaluation**: Maintains Dask's lazy evaluation
- **Reference Copying**: Efficient copying for Dask DataFrames
- **Scalable**: Suitable for big data scenarios

## Advanced Features

### Tagged Envelope Support

```python
async def process_tagged_envelope(self, envelope: TaggedEnvelope, metrics: SplitMetrics) -> AsyncIterator[Out]:
    """
    Process tagged envelopes with metadata preservation and duplication.
    """
    # Extract payload and metadata
    payload = envelope.payload
    metadata = envelope.metadata
    tags = envelope.tags
    
    # Update metrics
    metrics.tagged_envelopes_processed += 1
    metrics.rows_processed += self._count_rows(payload)
    
    # Duplicate envelope to all output ports
    for port_name in self.output_ports:
        # Create copy of envelope for each output
        envelope_copy = TaggedEnvelope(
            payload=copy.deepcopy(payload),
            metadata=copy.deepcopy(metadata),
            tags=copy.deepcopy(tags),
            source_port=envelope.source_port,
            timestamp=envelope.timestamp
        )
        
        # Forward to output port
        yield Out(port=port_name, payload=envelope_copy)
        
        # Update port statistics
        self._update_port_stats(port_name, len(str(payload)), 0.0)
```

### Port Statistics Tracking

```python
class PortStatistics:
    """Tracks statistics for each output port."""
    
    def __init__(self, port_name: str):
        self.port_name = port_name
        self.messages_sent = 0
        self.bytes_sent = 0
        self.last_message_time = None
        self.error_count = 0
        self.distribution_time = 0.0
    
    def update(self, message_size: int, distribution_time: float):
        """Update port statistics."""
        self.messages_sent += 1
        self.bytes_sent += message_size
        self.last_message_time = time.time()
        self.distribution_time += distribution_time
    
    def get_throughput(self) -> float:
        """Calculate throughput in messages per second."""
        if self.distribution_time > 0:
            return self.messages_sent / self.distribution_time
        return 0.0
```

### Error Handling and Recovery

```python
async def safe_process_row(self, row: Dict[str, Any], metrics: SplitMetrics) -> AsyncIterator[Out]:
    """
    Process row with comprehensive error handling and recovery.
    """
    try:
        # Validate input data
        self._validate_row(row)
        
        # Process row with error handling per port
        for port_name in self.output_ports:
            try:
                # Create copy for this port
                row_copy = copy.deepcopy(row)
                
                # Forward to output port
                yield Out(port=port_name, payload=row_copy)
                
                # Update success statistics
                self._update_port_stats(port_name, len(str(row)), 0.0)
                
            except Exception as e:
                logger.error(f"Error distributing to port '{port_name}': {e}")
                metrics.port_errors[port_name] = metrics.port_errors.get(port_name, 0) + 1
                
                # Optionally forward error information
                yield Out(port=port_name, payload={"error": str(e), "data": row})
                
    except ValidationError as e:
        logger.error(f"Row validation failed: {e}")
        metrics.validation_errors += 1
        
        # Forward error to all ports
        for port_name in self.output_ports:
            yield Out(port=port_name, payload={"error": str(e), "data": row})
            
    except Exception as e:
        logger.error(f"Unexpected error processing row: {e}")
        metrics.processing_errors += 1
        
        # Forward error to all ports
        for port_name in self.output_ports:
            yield Out(port=port_name, payload={"error": str(e), "data": row})
```

## Performance Optimization

### Memory Management

```python
def optimize_memory_usage(self, data: Any) -> Any:
    """Optimize memory usage for data duplication."""
    if isinstance(data, pd.DataFrame):
        # Optimize DataFrame memory usage before copying
        optimized_df = data.astype({
            col: 'category' for col in data.select_dtypes(include=['object']).columns
        })
        return optimized_df
    elif isinstance(data, dd.DataFrame):
        # Optimize Dask DataFrame memory usage
        return data.optimize()
    else:
        return data
```

### Copying Strategies

```python
def get_copy_strategy(self, data: Any) -> str:
    """Get optimal copying strategy for data type."""
    if isinstance(data, pd.DataFrame):
        # Use copy() for DataFrames
        return "dataframe_copy"
    elif isinstance(data, dd.DataFrame):
        # Use reference copy for Dask DataFrames
        return "dask_reference"
    elif isinstance(data, dict):
        # Use deep copy for dictionaries
        return "deep_copy"
    else:
        # Use shallow copy for other types
        return "shallow_copy"
```

### Parallel Distribution

```python
async def parallel_distribution(self, data: Any, metrics: SplitMetrics) -> AsyncIterator[Out]:
    """
    Distribute data to all output ports in parallel.
    """
    async def distribute_to_port(port_name: str, data_copy: Any):
        """Distribute data to a single port."""
        try:
            yield Out(port=port_name, payload=data_copy)
            self._update_port_stats(port_name, len(str(data_copy)), 0.0)
        except Exception as e:
            logger.error(f"Error distributing to port '{port_name}': {e}")
            yield Out(port=port_name, payload={"error": str(e), "data": data_copy})
    
    # Create copies for all ports
    data_copies = {}
    for port_name in self.output_ports:
        data_copies[port_name] = self._create_data_copy(data)
    
    # Distribute to all ports in parallel
    tasks = [distribute_to_port(port_name, data_copies[port_name]) 
             for port_name in self.output_ports]
    
    async for result in asyncio.gather(*tasks):
        yield result
```

## Monitoring and Metrics

### Split Metrics

```python
class SplitMetrics(ComponentMetrics):
    """Metrics specific to split operations."""
    
    def __init__(self):
        super().__init__()
        self.rows_processed = 0
        self.bytes_processed = 0
        self.messages_distributed = 0
        self.tagged_envelopes_processed = 0
        self.port_statistics = {}
        self.distribution_time = 0.0
        self.validation_errors = 0
        self.processing_errors = 0
        self.port_errors = {}
```

### Performance Monitoring

```python
def monitor_performance(self, metrics: SplitMetrics):
    """Monitor split component performance."""
    total_throughput = metrics.rows_processed / metrics.distribution_time if metrics.distribution_time > 0 else 0
    
    logger.info(
        f"Split performance: {total_throughput:.2f} rows/sec, {metrics.bytes_processed / 1024 / 1024:.2f} MB processed",
        extra={
            "component": self.name,
            "total_throughput": total_throughput,
            "bytes_processed": metrics.bytes_processed,
            "messages_distributed": metrics.messages_distributed,
            "output_ports": len(self.output_ports),
            "error_rate": (metrics.validation_errors + metrics.processing_errors) / max(metrics.rows_processed, 1)
        }
    )
    
    # Log per-port statistics
    for port_name, stats in metrics.port_statistics.items():
        port_throughput = stats.get("throughput", 0)
        logger.debug(
            f"Port '{port_name}': {port_throughput:.2f} messages/sec, {stats.get('bytes_sent', 0) / 1024 / 1024:.2f} MB sent"
        )
```

### Health Checks

```python
def health_check(self) -> Dict[str, Any]:
    """Perform component health check."""
    return {
        "status": "healthy",
        "input_ports": len(self.input_ports),
        "output_ports": len(self.output_ports),
        "total_rows_processed": self.metrics.rows_processed,
        "error_rate": self._calculate_error_rate(),
        "throughput": self._calculate_throughput(),
        "memory_usage": self._get_memory_usage(),
        "port_health": {
            port_name: {
                "status": "healthy" if stats.get("error_count", 0) == 0 else "degraded",
                "throughput": stats.get("throughput", 0),
                "error_count": stats.get("error_count", 0)
            }
            for port_name, stats in self.metrics.port_statistics.items()
        }
    }
```

## Configuration Examples

### Basic Split Component

```python
# Simple split component with two outputs
OUTPUT_PORTS = (
    OutPortSpec(name="analytics", required=True, fanout="many"),
    OutPortSpec(name="reporting", required=True, fanout="many"),
)

split_component = SplitComponent(
    name="data_splitter",
    OUTPUT_PORTS=OUTPUT_PORTS
)
```

### Advanced Split with Multiple Outputs

```python
# Split component with multiple specialized outputs
OUTPUT_PORTS = (
    OutPortSpec(name="real_time_processing", required=True, fanout="many"),
    OutPortSpec(name="batch_processing", required=True, fanout="many"),
    OutPortSpec(name="data_warehouse", required=True, fanout="many"),
    OutPortSpec(name="backup_storage", required=True, fanout="many"),
    OutPortSpec(name="monitoring", required=True, fanout="many"),
)

split_component = SplitComponent(
    name="multi_destination_splitter",
    OUTPUT_PORTS=OUTPUT_PORTS,
    enable_metrics=True,
    enable_port_statistics=True,
    enable_health_checks=True
)
```

### Pipeline Integration

```python
# Split component in a data pipeline
pipeline = DataPipeline([
    # Single data source
    PostgreSQLReader("source_data"),
    
    # Split data to multiple processing paths
    SplitComponent(
        name="data_distributor",
        OUTPUT_PORTS=(
            OutPortSpec(name="analytics", required=True, fanout="many"),
            OutPortSpec(name="reporting", required=True, fanout="many"),
            OutPortSpec(name="backup", required=True, fanout="many"),
        )
    ),
    
    # Parallel processing paths
    # Analytics path
    FilterComponent(rule=analytics_filter),
    AggregationComponent(group_by=["category"], aggregations=[total_count]),
    PostgreSQLWriter("analytics_table"),
    
    # Reporting path
    FilterComponent(rule=reporting_filter),
    ExcelWriter("monthly_report.xlsx"),
    
    # Backup path
    JSONWriter("backup_data.json")
])
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
- **Validation**: Validate input data before distribution
- **Port Error Handling**: Handle errors per output port
- **Recovery**: Implement graceful error recovery
- **Logging**: Log all errors with sufficient context

### Integration
- **Port Configuration**: Properly configure output ports
- **Envelope Support**: Support tagged envelopes for metadata
- **Pipeline Integration**: Integrate seamlessly with other components
- **Configuration**: Use flexible configuration options
