# Merge Component Documentation

## Overview

The MergeComponent provides efficient data routing and consolidation capabilities for multi-source data integration scenarios. It acts as a sophisticated forwarding mechanism that routes payloads from multiple named input ports to a single output port, enabling data unification, pipeline branching, and multi-source data consolidation without data modification or aggregation.

## Core Features

### Data Routing
- **Multi-Input Support**: Accepts data from multiple input ports simultaneously
- **Single Output Routing**: Routes all incoming data to a single output port
- **Payload Preservation**: Maintains original data structure and content
- **Port Management**: Intelligent port detection and management

### Processing Strategies
- **Row Processing**: Real-time streaming data forwarding
- **Bulk Processing**: Batch DataFrame forwarding
- **BigData Processing**: Distributed Dask DataFrame forwarding
- **Tagged Envelope Support**: Full support for tagged data envelopes

### Performance Optimization
- **Zero-Copy Forwarding**: Minimal memory overhead
- **Immediate Processing**: No buffering or queuing
- **Memory Efficiency**: No data accumulation or combination
- **High Throughput**: Optimized for high-volume data streams

## Component Architecture

### MergeComponent Structure

```python
@register_component("merge")
class MergeComponent(BaseComponent):
    """
    Merge component for data routing:
    - input:  'in' (required, fanin="many") -> data from multiple sources
    - output: 'merge' (required, fanout="one") -> consolidated data stream
    """
    
    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="merge", required=True, fanout="one"),)
    
    # No additional configuration required
    # Component automatically handles multi-input routing
```

### Port Management System

```python
class PortManager:
    """Manages input/output port operations."""
    
    def __init__(self):
        self.input_ports = {}
        self.output_ports = {}
        self.port_stats = {}
    
    def register_input_port(self, port_name: str, port_spec: InPortSpec):
        """Register an input port."""
        self.input_ports[port_name] = port_spec
        self.port_stats[port_name] = {
            "messages_received": 0,
            "bytes_received": 0,
            "last_message_time": None
        }
    
    def register_output_port(self, port_name: str, port_spec: OutPortSpec):
        """Register an output port."""
        self.output_ports[port_name] = port_spec
```

## Processing Strategies

### Row Processing
Processes individual rows with immediate forwarding:

```python
async def process_row(self, row: Dict[str, Any], metrics: MergeMetrics) -> AsyncIterator[Out]:
    """
    Process individual rows with immediate forwarding.
    """
    # Update metrics
    metrics.rows_processed += 1
    metrics.bytes_processed += len(str(row))
    
    # Forward row immediately to output port
    yield Out(port="merge", payload=row)
    
    # Log processing info
    self._log_processing_info(row, metrics)
```

**Characteristics:**
- **Immediate Processing**: No buffering or queuing
- **Low Latency**: Minimal processing delay
- **Memory Efficient**: No data accumulation
- **Real-time**: Suitable for streaming data

### Bulk Processing
Processes pandas DataFrames with immediate forwarding:

```python
async def process_bulk(self, dataframe: pd.DataFrame, metrics: MergeMetrics) -> AsyncIterator[Out]:
    """
    Process DataFrame with immediate forwarding.
    """
    # Update metrics
    metrics.rows_processed += len(dataframe)
    metrics.bytes_processed += dataframe.memory_usage(deep=True).sum()
    
    # Forward DataFrame immediately to output port
    yield Out(port="merge", payload=dataframe)
    
    # Log processing info
    self._log_bulk_processing_info(dataframe, metrics)
```

**Characteristics:**
- **Batch Processing**: Handles entire DataFrames
- **No Concatenation**: Forwards DataFrames as-is
- **Memory Management**: Efficient DataFrame handling
- **High Throughput**: Optimized for batch operations

### BigData Processing
Processes Dask DataFrames with distributed forwarding:

```python
async def process_bigdata(self, ddf: dd.DataFrame, metrics: MergeMetrics) -> AsyncIterator[Out]:
    """
    Process Dask DataFrame with distributed forwarding.
    """
    # Update metrics
    metrics.rows_processed += len(ddf) if hasattr(ddf, '__len__') else 0
    metrics.bytes_processed += ddf.memory_usage(deep=True).sum().compute()
    
    # Forward Dask DataFrame immediately to output port
    yield Out(port="merge", payload=ddf)
    
    # Log processing info
    self._log_bigdata_processing_info(ddf, metrics)
```

**Characteristics:**
- **Distributed Processing**: Handles large datasets
- **Lazy Evaluation**: Maintains Dask's lazy evaluation
- **No Materialization**: Forwards without computing
- **Scalable**: Suitable for big data scenarios

## Advanced Features

### Tagged Envelope Support

```python
async def process_tagged_envelope(self, envelope: TaggedEnvelope, metrics: MergeMetrics) -> AsyncIterator[Out]:
    """
    Process tagged envelopes with metadata preservation.
    """
    # Extract payload and metadata
    payload = envelope.payload
    metadata = envelope.metadata
    tags = envelope.tags
    
    # Update metrics
    metrics.tagged_envelopes_processed += 1
    metrics.rows_processed += self._count_rows(payload)
    
    # Create output envelope with preserved metadata
    output_envelope = TaggedEnvelope(
        payload=payload,
        metadata=metadata,
        tags=tags,
        source_port=envelope.source_port,
        timestamp=envelope.timestamp
    )
    
    # Forward envelope to output port
    yield Out(port="merge", payload=output_envelope)
```

### Port Statistics Tracking

```python
class PortStatistics:
    """Tracks statistics for each port."""
    
    def __init__(self, port_name: str):
        self.port_name = port_name
        self.messages_received = 0
        self.bytes_received = 0
        self.last_message_time = None
        self.error_count = 0
        self.processing_time = 0.0
    
    def update(self, message_size: int, processing_time: float):
        """Update port statistics."""
        self.messages_received += 1
        self.bytes_received += message_size
        self.last_message_time = time.time()
        self.processing_time += processing_time
    
    def get_throughput(self) -> float:
        """Calculate throughput in messages per second."""
        if self.processing_time > 0:
            return self.messages_received / self.processing_time
        return 0.0
```

### Error Handling and Recovery

```python
async def safe_process_row(self, row: Dict[str, Any], metrics: MergeMetrics) -> AsyncIterator[Out]:
    """
    Process row with comprehensive error handling.
    """
    try:
        # Validate input data
        self._validate_row(row)
        
        # Process row
        async for result in self.process_row(row, metrics):
            yield result
            
    except ValidationError as e:
        logger.error(f"Row validation failed: {e}")
        metrics.validation_errors += 1
        # Optionally forward with error flag
        yield Out(port="merge", payload={"error": str(e), "data": row})
        
    except Exception as e:
        logger.error(f"Unexpected error processing row: {e}")
        metrics.processing_errors += 1
        # Forward error information
        yield Out(port="merge", payload={"error": str(e), "data": row})
```

## Performance Optimization

### Memory Management

```python
def optimize_memory_usage(self, data: Any) -> Any:
    """Optimize memory usage for data forwarding."""
    if isinstance(data, pd.DataFrame):
        # Optimize DataFrame memory usage
        return data.astype({
            col: 'category' for col in data.select_dtypes(include=['object']).columns
        })
    elif isinstance(data, dd.DataFrame):
        # Optimize Dask DataFrame memory usage
        return data.optimize()
    else:
        return data
```

### Throughput Optimization

```python
def optimize_throughput(self, data: Any) -> Any:
    """Optimize data for high-throughput forwarding."""
    if isinstance(data, pd.DataFrame):
        # Use efficient data types
        return data.astype({
            col: 'int32' if data[col].dtype == 'int64' else data[col].dtype
            for col in data.select_dtypes(include=['int64']).columns
        })
    else:
        return data
```

### Parallel Processing

```python
async def parallel_forwarding(self, data_streams: List[AsyncIterator], metrics: MergeMetrics) -> AsyncIterator[Out]:
    """
    Forward data from multiple streams in parallel.
    """
    async def forward_stream(stream: AsyncIterator):
        """Forward data from a single stream."""
        async for data in stream:
            yield Out(port="merge", payload=data)
    
    # Process all streams in parallel
    tasks = [forward_stream(stream) for stream in data_streams]
    async for result in asyncio.gather(*tasks):
        yield result
```

## Monitoring and Metrics

### Merge Metrics

```python
class MergeMetrics(ComponentMetrics):
    """Metrics specific to merge operations."""
    
    def __init__(self):
        super().__init__()
        self.rows_processed = 0
        self.bytes_processed = 0
        self.messages_forwarded = 0
        self.tagged_envelopes_processed = 0
        self.port_statistics = {}
        self.forwarding_time = 0.0
        self.validation_errors = 0
        self.processing_errors = 0
```

### Performance Monitoring

```python
def monitor_performance(self, metrics: MergeMetrics):
    """Monitor merge component performance."""
    throughput = metrics.rows_processed / metrics.forwarding_time if metrics.forwarding_time > 0 else 0
    
    logger.info(
        f"Merge performance: {throughput:.2f} rows/sec, {metrics.bytes_processed / 1024 / 1024:.2f} MB processed",
        extra={
            "component": self.name,
            "throughput": throughput,
            "bytes_processed": metrics.bytes_processed,
            "messages_forwarded": metrics.messages_forwarded,
            "error_rate": (metrics.validation_errors + metrics.processing_errors) / max(metrics.rows_processed, 1)
        }
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
        "memory_usage": self._get_memory_usage()
    }
```

## Configuration Examples

### Basic Merge Component

```python
# Simple merge component
merge_component = MergeComponent(
    name="data_merger",
    description="Merge data from multiple sources"
)
```

### Advanced Merge with Monitoring

```python
# Merge component with comprehensive monitoring
merge_component = MergeComponent(
    name="monitored_merger",
    description="Merge component with performance monitoring",
    enable_metrics=True,
    enable_port_statistics=True,
    enable_health_checks=True
)
```

### Pipeline Integration

```python
# Merge component in a data pipeline
pipeline = DataPipeline([
    # Multiple data sources
    CSVReader("source1.csv"),
    JSONReader("source2.json"),
    PostgreSQLReader("source3"),
    
    # Merge all sources
    MergeComponent(name="data_consolidator"),
    
    # Process merged data
    FilterComponent(rule=quality_filter),
    AggregationComponent(group_by=["category"], aggregations=[total_count]),
    
    # Output to destination
    PostgreSQLWriter("consolidated_data")
])
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

## Best Practices

### Component Design
- **Single Responsibility**: Focus on data routing only
- **Minimal Processing**: Avoid data modification or transformation
- **Efficient Forwarding**: Optimize for high throughput
- **Error Handling**: Implement robust error handling

### Performance
- **Memory Management**: Monitor memory usage
- **Throughput Optimization**: Optimize for high data volumes
- **Parallel Processing**: Use parallel processing when appropriate
- **Monitoring**: Implement comprehensive performance monitoring

### Error Handling
- **Validation**: Validate input data before forwarding
- **Error Recovery**: Implement graceful error recovery
- **Logging**: Log all errors with sufficient context
- **Monitoring**: Monitor error rates and patterns

### Integration
- **Port Management**: Properly manage input/output ports
- **Envelope Support**: Support tagged envelopes for metadata
- **Pipeline Integration**: Integrate seamlessly with other components
- **Configuration**: Use flexible configuration options
