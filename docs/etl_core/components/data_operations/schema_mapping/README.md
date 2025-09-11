# Schema Mapping Component

The SchemaMappingComponent provides **comprehensive schema transformation and data joining capabilities** for complex data integration scenarios with comprehensive support for different processing strategies and data types.

## Overview

The SchemaMappingComponent provides comprehensive schema transformation and data joining capabilities for complex data integration scenarios. It supports both simple field mapping operations and sophisticated multi-source join operations, making it ideal for **data standardization**, format conversion, and schema evolution workflows.

## Components

### Schema Mapping Functions
- **Field Mapping**: Map fields from source to destination schemas
- **Data Type Conversion**: Convert data types during transformation
- **Nested Field Access**: Support for complex nested data structures
- **Join Operations**: Multi-source data joining capabilities

## Schema Mapping Types

### Schema Transformation
Transforms data schemas and field structures.

#### Features
- **Field Mapping**: Map fields from source to destination schemas
- **Data Type Conversion**: Convert data types during transformation
- **Nested Field Access**: Support for complex nested data structures
- **Schema Evolution**: Handle schema changes over time
- **Custom Transformations**: User-defined transformation functions

### Join Operations
Joins data from multiple input sources.

#### Features
- **Multiple Join Types**: Inner, left, right, and outer joins
- **Multi-Source Joins**: Join data from multiple input sources
- **Complex Join Conditions**: Support for complex join logic
- **Join Optimization**: Efficient join algorithms for large datasets
- **Join Validation**: Comprehensive join plan validation

### Processing Strategies
Supports different processing strategies for various data types and sizes.

#### Features
- **Row Processing**: Real-time streaming with buffering for joins
- **Bulk Processing**: Batch processing with pandas DataFrames
- **BigData Processing**: Distributed processing with Dask DataFrames
- **Lazy Evaluation**: Deferred computation for bigdata strategies

## JSON Configuration Examples

### Basic Field Mapping

```json
{
  "name": "customer_mapping",
  "comp_type": "schema_mapping",
  "rules_by_dest": {
    "customer_id": {
      "src_port": "in1",
      "src_path": "id"
    },
    "customer_name": {
      "src_port": "in1",
      "src_path": "name"
    },
    "email_address": {
      "src_port": "in1",
      "src_path": "contact.email"
    }
  },
  "strategy_type": "bulk"
}
```

### Complex Multi-Source Join

```json
{
  "name": "customer_order_join",
  "comp_type": "schema_mapping",
  "rules_by_dest": {
    "customer_id": {
      "src_port": "customers",
      "src_path": "id"
    },
    "customer_name": {
      "src_port": "customers",
      "src_path": "name"
    },
    "order_total": {
      "src_port": "orders",
      "src_path": "total"
    },
    "order_date": {
      "src_port": "orders",
      "src_path": "created_at"
    }
  },
  "join_plan": {
    "steps": [
      {
        "left_port": "customers",
        "right_port": "orders",
        "left_key": "id",
        "right_key": "customer_id",
        "join_type": "inner"
      }
    ]
  },
  "strategy_type": "bulk"
}
```

### Nested Field Mapping

```json
{
  "name": "user_profile_mapping",
  "comp_type": "schema_mapping",
  "rules_by_dest": {
    "user_id": {
      "src_port": "in1",
      "src_path": "user.id"
    },
    "first_name": {
      "src_port": "in1",
      "src_path": "user.profile.first_name"
    },
    "last_name": {
      "src_port": "in1",
      "src_path": "user.profile.last_name"
    },
    "address_city": {
      "src_port": "in1",
      "src_path": "user.address.city"
    },
    "preferences_theme": {
      "src_port": "in1",
      "src_path": "user.preferences.ui_theme"
    }
  },
  "strategy_type": "bulk"
}
```

### Data Type Conversion

```json
{
  "name": "data_type_conversion",
  "comp_type": "schema_mapping",
  "rules_by_dest": {
    "id": {
      "src_port": "in1",
      "src_path": "id",
      "transform": "int"
    },
    "price": {
      "src_port": "in1",
      "src_path": "price",
      "transform": "float"
    },
    "is_active": {
      "src_port": "in1",
      "src_path": "status",
      "transform": "bool"
    },
    "created_at": {
      "src_port": "in1",
      "src_path": "timestamp",
      "transform": "datetime"
    }
  },
  "strategy_type": "bulk"
}
```

### Real-Time Stream Mapping

```json
{
  "name": "stream_mapping",
  "comp_type": "schema_mapping",
  "rules_by_dest": {
    "event_id": {
      "src_port": "events",
      "src_path": "id"
    },
    "event_type": {
      "src_port": "events",
      "src_path": "type"
    },
    "user_id": {
      "src_port": "events",
      "src_path": "user.id"
    },
    "timestamp": {
      "src_port": "events",
      "src_path": "created_at"
    }
  },
  "strategy_type": "row",
  "buffer_size": 1000
}
```

## Processing Modes

### Mapping-Only Mode

**Mapping-only mode** processes **single input** without joins, applying field mapping rules immediately. This mode is ideal for **simple data transformation** scenarios where you need to restructure data from a single source. **No buffering** is required, making it efficient for real-time processing.

### Join Mode

**Join mode** buffers data from **multiple inputs** and waits for all required inputs to close before performing joins according to the join plan. This mode is essential for **complex data integration** scenarios where you need to combine data from multiple sources. The component **resets buffers** after processing to maintain memory efficiency.

## Schema Mapping Features

### Field Mapping
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

### Data Processing
- **Type Safety**: Ensures data types match transformation requirements
- **Null Handling**: Configurable handling of null values in transformations
- **Error Recovery**: Graceful handling of transformation errors
- **Performance Optimization**: Efficient algorithms for large datasets

## Error Handling

### Schema Mapping Errors
- **Clear Messages**: Descriptive error messages for mapping failures
- **Field Validation**: Path-based error reporting for nested structures
- **Type Information**: Detailed type mismatch information
- **Context**: Data and mapping context in error messages

### Error Types
- **Missing Fields**: Required source fields not present
- **Type Mismatches**: Data types incompatible with transformations
- **Invalid Mappings**: Malformed or unsupported field mappings
- **Join Errors**: Join operation failures

### Error Reporting
```json
{
  "mapping_error": {
    "field_path": "customer.orders.total",
    "mapping_rule": "order_total",
    "error_type": "field_not_found",
    "message": "Source field 'orders.total' not found in input data"
  }
}
```

## Performance Considerations

### Row Processing
- **Immediate Processing**: Processes data as it arrives
- **Memory Efficient**: Low memory usage with intelligent buffering
- **Real-time**: Immediate transformation results
- **Buffering**: Buffers data for join operations

### Bulk Processing
- **Batch Processing**: Processes entire DataFrames at once
- **High Performance**: Optimized for medium-sized datasets
- **Memory Management**: Efficient DataFrame handling
- **Join Optimization**: Optimized join algorithms

### BigData Processing
- **Distributed Processing**: Handles large datasets across multiple cores
- **Lazy Evaluation**: Deferred computation for memory efficiency
- **Scalable**: Processes datasets of virtually any size
- **Join Optimization**: Dask-optimized join algorithms

## Configuration

### Schema Mapping Options
- **Field Mappings**: Define field mapping rules
- **Join Plans**: Configure join operations
- **Data Type Conversions**: Set data type conversion rules
- **Error Handling**: Configure error handling strategies

### Performance Tuning
- **Buffer Sizes**: Configure buffer sizes for join operations
- **Chunk Sizes**: Set chunk sizes for bigdata processing
- **Memory Limits**: Set appropriate memory limits
- **Parallel Processing**: Configure parallel processing options

## Best Practices

### Schema Design
- **Consistent Naming**: Use consistent field naming conventions
- **Data Type Planning**: Plan data types carefully for optimal performance
- **Nested Structure Design**: Design nested structures for easy access
- **Schema Evolution**: Plan for schema changes over time

### Performance
- **Field Selection**: Only map necessary fields to minimize processing overhead
- **Join Optimization**: Use appropriate join types for your use case
- **Memory Management**: Monitor memory usage during join operations
- **Processing Strategy**: Choose the right processing strategy for your data size

### Error Handling
- **Data Validation**: Validate input data before transformation
- **Error Recovery**: Implement graceful error recovery mechanisms
- **Logging**: Log all errors with sufficient context
- **Monitoring**: Monitor data quality and processing errors

### Data Quality
- **Data Validation**: Validate input data before transformation
- **Schema Validation**: Use schema validation for data quality
- **Error Recovery**: Implement graceful error recovery mechanisms
- **Monitoring**: Monitor data quality and processing errors