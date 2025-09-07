# Data Operations Components

This directory contains comprehensive components for data transformation and manipulation operations. These components form the core of the ETL system's data processing capabilities, providing powerful tools for data aggregation, filtering, merging, splitting, and schema transformation.

## Overview

Data Operations Components are designed to handle complex data transformations across different processing strategies (row, bulk, bigdata) while maintaining high performance, reliability, and flexibility. They provide a unified interface for data manipulation operations that can be easily composed into complex data processing pipelines.

## Component Architecture

### Base Data Operations Pattern

All data operations components follow a consistent architectural pattern:

```python
@register_component("operation_type")
class DataOperationComponent(BaseComponent):
    """
    Data operation component with standard interface.
    """
    
    # Configuration fields
    config_field: str = Field(..., description="Configuration field")
    
    # Port definitions
    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)
    
    # Processing methods
    async def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> AsyncIterator[Out]:
        """Process individual rows."""
        pass
    
    async def process_bulk(self, dataframe: pd.DataFrame, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        """Process pandas DataFrames."""
        pass
    
    async def process_bigdata(self, ddf: dd.DataFrame, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        """Process Dask DataFrames."""
        pass
```

### Processing Strategy Support

All data operations components support three processing strategies:

#### Row Processing
- **Purpose**: Real-time streaming data processing
- **Characteristics**: Low latency, memory efficient, immediate processing
- **Use Cases**: Real-time analytics, streaming pipelines, event processing
- **Performance**: Optimized for streaming, minimal buffering

#### Bulk Processing
- **Purpose**: Medium-sized datasets, batch processing
- **Characteristics**: Higher throughput, pandas DataFrames, batch operations
- **Use Cases**: ETL pipelines, data analysis, batch processing
- **Performance**: Optimized for batch operations, memory management

#### BigData Processing
- **Purpose**: Large datasets, distributed processing
- **Characteristics**: Dask DataFrames, lazy evaluation, distributed computing
- **Use Cases**: Big data analytics, large-scale ETL, distributed processing
- **Performance**: Scalable to large datasets, distributed execution

## Component Categories

### Aggregation Components

#### [Aggregation Component](./aggregation/README.md)
**Purpose**: Group-by operations with various aggregation functions
- **Features**: Sum, count, mean, min, max, std, custom functions
- **Strategies**: Row buffering, bulk processing, bigdata distributed
- **Use Cases**: Data summarization, analytics, reporting
- **Advanced**: Nested field grouping, conditional aggregations, custom functions

**Key Capabilities:**
- **Mathematical Functions**: Sum, mean, median, standard deviation, variance
- **Statistical Functions**: Min, max, count, first, last
- **Custom Functions**: User-defined aggregation functions
- **Conditional Aggregations**: Aggregations with conditions
- **Group By Operations**: Single/multi-field grouping, nested field grouping

**Configuration Example:**
```python
aggregation_component = AggregationComponent(
    name="sales_analytics",
    group_by=["region", "product_category", "year"],
    aggregations=[
        AggOp(src="sales_amount", op="sum", dst="total_revenue"),
        AggOp(src="sales_amount", op="avg", dst="avg_order_value"),
        AggOp(src="*", op="count", dst="order_count")
    ]
)
```

### Filtering Components

#### [Filter Component](./filter/README.md)
**Purpose**: Data filtering based on comparison rules
- **Features**: Simple comparisons, complex logical expressions, nested rules
- **Operators**: ==, !=, >, <, >=, <=, contains, custom operators
- **Strategies**: Row-by-row, bulk DataFrame, bigdata lazy evaluation
- **Use Cases**: Data quality, conditional processing, data routing

**Key Capabilities:**
- **Leaf Rules**: Simple column comparisons with various operators
- **Logical Rules**: Complex expressions using AND, OR, NOT operators
- **Nested Rules**: Unlimited nesting of logical operators
- **Custom Operators**: User-defined comparison operators
- **Performance**: Rule compilation, caching, vectorized evaluation

**Configuration Example:**
```python
filter_component = FilterComponent(
    name="age_filter",
    rule=ComparisonRule(
        logical_operator="AND",
        rules=[
            ComparisonRule(column="age", operator=">=", value=18),
            ComparisonRule(column="status", operator="==", value="active")
        ]
    )
)
```

### Data Routing Components

#### [Merge Component](./merge/README.md)
**Purpose**: Combine data from multiple input sources
- **Features**: Multi-input routing, payload combination, fan-in operations
- **Strategies**: Row merging, bulk DataFrame concatenation, bigdata union
- **Use Cases**: Data consolidation, multi-source integration, data joining

**Key Capabilities:**
- **Multi-Input Support**: Accept data from multiple input ports
- **Payload Routing**: Route payloads from multiple inputs to single output
- **Fan-In Operations**: Combine multiple data streams into one
- **Data Consolidation**: Merge data from different sources
- **Performance**: Efficient routing, memory optimization

#### [Split Component](./split/README.md)
**Purpose**: Distribute data to multiple output destinations
- **Features**: Multi-output routing, payload duplication, fan-out operations
- **Strategies**: Row splitting, bulk DataFrame partitioning, bigdata distribution
- **Use Cases**: Data distribution, parallel processing, multi-destination routing

**Key Capabilities:**
- **Multi-Output Support**: Send data to multiple output ports
- **Payload Duplication**: Duplicate incoming payloads to multiple outputs
- **Fan-Out Operations**: Distribute single data stream to multiple destinations
- **Data Distribution**: Route data to different processing paths
- **Performance**: Efficient duplication, memory optimization

### Schema Transformation Components

#### [Schema Mapping Component](./schema_mapping/README.md)
**Purpose**: Transform data schemas and structure
- **Features**: Field mapping, data type conversion, nested transformations
- **Strategies**: Row transformation, bulk DataFrame operations, bigdata lazy evaluation
- **Use Cases**: Data standardization, format conversion, schema evolution

**Key Capabilities:**
- **Field Mapping**: Map fields from source to target schema
- **Data Type Conversion**: Convert data types during transformation
- **Nested Transformations**: Handle complex nested data structures
- **Schema Evolution**: Support for schema changes over time
- **Performance**: Efficient transformation, lazy evaluation

## Common Features

### Processing Strategy Support

All data operations components support:

#### Row Processing
- **Individual Record Processing**: Process one record at a time
- **Real-time Processing**: Immediate processing without buffering
- **Memory Efficiency**: Low memory usage for streaming data
- **Low Latency**: Minimal processing delay

#### Bulk Processing
- **Pandas DataFrame Processing**: Process entire DataFrames
- **Batch Operations**: Process data in batches for efficiency
- **Memory Management**: Optimized memory usage for medium datasets
- **Higher Throughput**: Better performance for batch operations

#### BigData Processing
- **Dask DataFrame Processing**: Process large datasets with lazy evaluation
- **Distributed Processing**: Scale across multiple machines
- **Lazy Evaluation**: Defer computation until necessary
- **Scalability**: Handle datasets larger than memory

### Metrics Integration

All components provide comprehensive metrics:

```python
class ComponentMetrics(BaseModel):
    """Base metrics for all components."""
    
    # Processing metrics
    rows_processed: int = 0
    processing_time: float = 0.0
    memory_usage: float = 0.0
    
    # Quality metrics
    error_count: int = 0
    success_rate: float = 1.0
    
    # Performance metrics
    throughput: float = 0.0  # rows per second
    latency: float = 0.0     # average processing time
```

### Schema Validation

All components support schema validation:

```python
def validate_input_schema(self, data: Any) -> None:
    """Validate input data against expected schema."""
    if isinstance(data, pd.DataFrame):
        self._validate_dataframe_schema(data)
    elif isinstance(data, dict):
        self._validate_row_schema(data)
    else:
        raise ValueError(f"Unsupported data type: {type(data)}")
```

### Error Handling

Robust error handling across all components:

```python
def safe_processing(self, data: Any) -> Any:
    """Process data with comprehensive error handling."""
    try:
        return self._process_data(data)
    except ValidationError as e:
        logger.error(f"Schema validation failed: {e}")
        return self._handle_validation_error(data, e)
    except ProcessingError as e:
        logger.error(f"Processing failed: {e}")
        return self._handle_processing_error(data, e)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return self._handle_unexpected_error(data, e)
```

## Configuration System

### Pydantic-Based Configuration

All components use Pydantic models for configuration:

```python
class ComponentConfig(BaseModel):
    """Base configuration for all components."""
    
    name: str = Field(..., description="Component name")
    description: Optional[str] = Field(None, description="Component description")
    enabled: bool = Field(True, description="Whether component is enabled")
    
    class Config:
        validate_assignment = True
        extra = "forbid"
```

### Field Validation

Comprehensive field validation:

```python
class FilterConfig(ComponentConfig):
    """Configuration for filter components."""
    
    rule: ComparisonRule = Field(..., description="Filter rule")
    case_sensitive: bool = Field(True, description="Case sensitive matching")
    null_handling: str = Field("skip", description="How to handle null values")
    
    @validator('rule')
    def validate_rule(cls, v):
        """Validate filter rule."""
        if not v:
            raise ValueError("Filter rule cannot be empty")
        return v
```

### Type Checking

Full type checking support:

```python
from typing import Dict, Any, List, Optional, Union

class AggregationConfig(ComponentConfig):
    """Configuration for aggregation components."""
    
    group_by: List[str] = Field(..., description="Fields to group by")
    aggregations: List[AggOp] = Field(..., description="Aggregation operations")
    buffer_size: int = Field(1000, ge=1, description="Buffer size for row processing")
    
    @validator('group_by')
    def validate_group_by(cls, v):
        """Validate group-by fields."""
        if not v:
            raise ValueError("Group-by fields cannot be empty")
        return v
```

## Performance Optimization

### Memory Management

Efficient memory usage across all components:

```python
def optimize_memory_usage(self, data: Any) -> Any:
    """Optimize memory usage for data processing."""
    if isinstance(data, pd.DataFrame):
        return self._optimize_dataframe_memory(data)
    elif isinstance(data, dd.DataFrame):
        return self._optimize_dask_dataframe_memory(data)
    else:
        return data
```

### Caching

Intelligent caching for performance:

```python
from functools import lru_cache

@lru_cache(maxsize=128)
def get_compiled_rule(self, rule_hash: str) -> Callable:
    """Cache compiled rules for reuse."""
    return self.compile_rule(rule)
```

### Parallel Processing

Parallel processing support:

```python
def parallel_processing(self, data: Any, n_workers: int = 4) -> Any:
    """Apply parallel processing to data."""
    if isinstance(data, pd.DataFrame):
        return self._parallel_dataframe_processing(data, n_workers)
    else:
        return self._parallel_row_processing(data, n_workers)
```

## Monitoring and Debugging

### Metrics Collection

Comprehensive metrics collection:

```python
class DataOperationMetrics(ComponentMetrics):
    """Metrics specific to data operations."""
    
    def __init__(self):
        super().__init__()
        self.rows_processed = 0
        self.rows_output = 0
        self.processing_time = 0.0
        self.memory_usage = 0.0
        self.error_count = 0
        self.cache_hits = 0
        self.cache_misses = 0
```

### Logging

Structured logging for debugging:

```python
import logging

logger = logging.getLogger(__name__)

def log_processing_info(self, data: Any, metrics: ComponentMetrics):
    """Log processing information."""
    logger.info(
        f"Processing {len(data)} rows in {metrics.processing_time:.2f}s",
        extra={
            "component": self.name,
            "rows_processed": metrics.rows_processed,
            "processing_time": metrics.processing_time,
            "memory_usage": metrics.memory_usage
        }
    )
```

### Health Checks

Component health monitoring:

```python
def health_check(self) -> Dict[str, Any]:
    """Perform component health check."""
    return {
        "status": "healthy",
        "memory_usage": self._get_memory_usage(),
        "error_rate": self._get_error_rate(),
        "throughput": self._get_throughput(),
        "last_processed": self._get_last_processed_time()
    }
```

## Best Practices

### Component Design

- **Single Responsibility**: Each component should have a single, well-defined purpose
- **Loose Coupling**: Minimize dependencies between components
- **High Cohesion**: Related functionality should be grouped together
- **Interface Segregation**: Use specific interfaces rather than general ones

### Performance

- **Profile Early**: Identify performance bottlenecks early in development
- **Optimize Strategically**: Focus optimization efforts on critical paths
- **Monitor Continuously**: Implement comprehensive monitoring and alerting
- **Test Performance**: Include performance testing in your test suite

### Error Handling

- **Fail Fast**: Detect and report errors as early as possible
- **Graceful Degradation**: Provide fallback mechanisms for critical failures
- **Comprehensive Logging**: Log all errors with sufficient context
- **User-Friendly Messages**: Provide clear, actionable error messages

### Testing

- **Unit Testing**: Test individual components in isolation
- **Integration Testing**: Test component interactions and data flow
- **Performance Testing**: Validate performance under various load conditions
- **End-to-End Testing**: Test complete data processing pipelines

## Usage Examples

### Simple Data Pipeline

```python
# Create a simple data processing pipeline
pipeline = DataPipeline([
    CSVReader("input.csv"),
    FilterComponent(rule=age_rule),
    AggregationComponent(group_by=["region"], aggregations=[sales_sum]),
    PostgreSQLWriter("output_table")
])

# Execute pipeline
await pipeline.execute()
```

### Complex Data Transformation

```python
# Create a complex data transformation pipeline
pipeline = DataPipeline([
    # Read data from multiple sources
    CSVReader("customers.csv"),
    JSONReader("orders.json"),
    
    # Merge data
    MergeComponent(),
    
    # Apply transformations
    SchemaMappingComponent(mapping=customer_order_mapping),
    FilterComponent(rule=active_customers_rule),
    
    # Aggregate data
    AggregationComponent(
        group_by=["customer_segment", "month"],
        aggregations=[total_sales, order_count, avg_order_value]
    ),
    
    # Split for different outputs
    SplitComponent(outputs=["analytics", "reporting"]),
    
    # Write to different destinations
    PostgreSQLWriter("analytics_table"),
    ExcelWriter("monthly_report.xlsx")
])

# Execute with monitoring
await pipeline.execute_with_monitoring()
```

### Real-time Processing

```python
# Create a real-time processing pipeline
pipeline = DataPipeline([
    KafkaReader("events_topic"),
    FilterComponent(rule=high_value_events_rule),
    AggregationComponent(
        group_by=["event_type", "minute"],
        aggregations=[event_count, total_value]
    ),
    RedisWriter("real_time_metrics")
])

# Execute with real-time monitoring
await pipeline.execute_streaming()
```
