# Schema Mapping Component Documentation

## Overview

The SchemaMappingComponent provides comprehensive schema transformation and data joining capabilities for complex data integration scenarios. It supports both simple field mapping operations and sophisticated multi-source join operations, making it ideal for data standardization, format conversion, and schema evolution workflows.

## Core Features

### Schema Transformation
- **Field Mapping**: Map fields from source to destination schemas
- **Data Type Conversion**: Convert data types during transformation
- **Nested Field Access**: Support for complex nested data structures
- **Schema Evolution**: Handle schema changes over time
- **Custom Transformations**: User-defined transformation functions

### Join Operations
- **Multiple Join Types**: Inner, left, right, and outer joins
- **Multi-Source Joins**: Join data from multiple input sources
- **Complex Join Conditions**: Support for complex join logic
- **Join Optimization**: Efficient join algorithms for large datasets
- **Join Validation**: Comprehensive join plan validation

### Processing Strategies
- **Row Processing**: Real-time streaming with buffering for joins
- **Bulk Processing**: Batch processing with pandas DataFrames
- **BigData Processing**: Distributed processing with Dask DataFrames
- **Lazy Evaluation**: Deferred computation for bigdata strategies

## Features

- **Field Mapping**: Map fields from source to destination schemas
- **Join Operations**: Perform inner, left, right, and outer joins
- **Multi-Processing Support**: Row, bulk, and bigdata processing modes
- **Schema Validation**: Validates mapping rules against input/output schemas
- **Buffered Processing**: Supports buffering for join operations
- **Flexible Configuration**: Nested rule configuration for complex mappings

## Configuration

### Required Fields
- `rules_by_dest`: Dictionary mapping destination fields to source field mappings
- `join_plan`: JoinPlan object defining join operations (optional)

### Field Mapping
Each field mapping specifies:
- Source port and field path
- Destination port and field path
- Transformation rules (if any)

### Join Plan
Join plan defines:
- Join steps with left/right ports and join keys
- Join types (inner, left, right, outer)
- Join conditions

## Input/Output

### Input Ports
- Multiple input ports supported (fanin="many")
- Port names defined by join plan or mapping rules

### Output Ports
- Multiple output ports supported (fanout="many")
- Port names defined by mapping rules

## Processing Modes

### Mapping-Only Mode
- Processes single input without joins
- Applies field mapping rules
- No buffering required

### Join Mode
- Buffers data from multiple inputs
- Waits for all required inputs to close
- Performs joins according to join plan
- Resets buffers after processing

### Row Processing
- For mapping: immediate processing
- For joins: buffered processing with tagged envelopes

### Bulk Processing
- For mapping: immediate DataFrame processing
- For joins: buffered DataFrame processing

### BigData Processing
- For mapping: immediate Dask DataFrame processing
- For joins: buffered Dask DataFrame processing

## Example Usage

```python
from etl_core.components.data_operations.schema_mapping.schema_mapping_component import SchemaMappingComponent
from etl_core.components.data_operations.schema_mapping.mapping_rule import FieldMappingSrc
from etl_core.components.data_operations.schema_mapping.join_rules import JoinPlan

# Configure schema mapping
component = SchemaMappingComponent(
    name="customer_mapping",
    rules_by_dest={
        "customer_id": FieldMappingSrc(src_port="in1", src_path="id"),
        "customer_name": FieldMappingSrc(src_port="in1", src_path="name"),
        "order_total": FieldMappingSrc(src_port="in2", src_path="total")
    },
    join_plan=JoinPlan(
        steps=[{
            "left_port": "in1",
            "right_port": "in2", 
            "left_key": "id",
            "right_key": "customer_id",
            "join_type": "inner"
        }]
    )
)
```

## Advanced Features

### Tagged Input Support
- Automatically detects when tagged input is required
- Required when join plan is defined and multiple input ports exist
- Uses InTagged envelopes for proper buffering

### Schema Path Separator
- Supports dotted notation for nested field access
- Configurable path separator for different data formats
- Validates field paths against schema definitions

### Join Completion Detection
- Tracks required input ports for join completion
- Waits for all required ports to signal closure
- Prevents partial join processing

## Error Handling

- Validates field mappings against input/output schemas
- Validates join plan against available ports
- Provides clear error messages for configuration issues
- Handles missing fields gracefully
