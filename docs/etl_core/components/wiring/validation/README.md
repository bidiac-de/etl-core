# Validation Components

Validation components provide **data validation functionality** for ensuring data integrity and schema compliance with comprehensive support for different data formats and error handling.

## Overview

Validation components offer comprehensive data validation capabilities for different data formats, ensuring data quality and schema compliance throughout the ETL pipeline. They provide **multi-format support** and **robust error handling** for reliable data processing.

## Components

### Validation Functions
- **Row Validation**: Validate individual data rows
- **DataFrame Validation**: Validate pandas DataFrames
- **Dask Validation**: Validate Dask DataFrames
- **Schema Validation**: Validate data against schemas

## Validation Types

### Row Validation
Validates individual data rows against schema definitions.

#### Features
- **Type Checking**: Validates data types against schema
- **Required Fields**: Ensures required fields are present
- **Enum Validation**: Validates enum values
- **Nested Validation**: Validates nested object structures
- **Array Validation**: Validates array elements

### DataFrame Validation
Validates pandas DataFrames against schema definitions.

#### Features
- **Column Validation**: Ensures required columns are present
- **Type Validation**: Validates column data types
- **Null Validation**: Checks for null values in non-nullable fields
- **Enum Validation**: Validates enum values in columns
- **Flattened Schema**: Works with flattened nested schemas

### Dask DataFrame Validation
Validates Dask DataFrames against schema definitions.

#### Features
- **Lazy Validation**: Validates without materializing data
- **Distributed Validation**: Works with distributed data
- **Column Validation**: Ensures required columns are present
- **Type Validation**: Validates column data types
- **Null Validation**: Checks for null values in non-nullable fields

## JSON Configuration Examples

### Row Validation

```json
{
  "name": "validate_customer_row",
  "comp_type": "validation",
  "schema": "customer_schema",
  "validation_type": "row",
  "strict_mode": true,
  "error_handling": "raise"
}
```

### DataFrame Validation

```json
{
  "name": "validate_customer_df",
  "comp_type": "validation",
  "schema": "customer_schema",
  "validation_type": "dataframe",
  "strict_mode": true,
  "error_handling": "log"
}
```

### Dask Validation

```json
{
  "name": "validate_bigdata_df",
  "comp_type": "validation",
  "schema": "customer_schema",
  "validation_type": "dask",
  "strict_mode": false,
  "error_handling": "continue"
}
```

### Custom Validation Rules

```json
{
  "name": "custom_validation",
  "comp_type": "validation",
  "schema": "custom_schema",
  "validation_type": "row",
  "custom_rules": [
    {
      "field": "email",
      "rule": "email_format",
      "message": "Invalid email format"
    },
    {
      "field": "age",
      "rule": "range",
      "min": 0,
      "max": 120,
      "message": "Age must be between 0 and 120"
    }
  ],
  "error_handling": "raise"
}
```

## Validation Features

### Type Validation
- **Primitive Types**: String, integer, float, boolean validation
- **Complex Types**: Object and array validation
- **Type Coercion**: Automatic type conversion where appropriate
- **Type Safety**: Ensures data types match schema definitions

### Null Validation
- **Nullable Fields**: Allows null values for nullable fields
- **Required Fields**: Ensures non-nullable fields are not null
- **Null Checking**: Comprehensive null value validation
- **Error Reporting**: Clear error messages for null violations

### Enum Validation
- **Value Checking**: Validates values against allowed enum values
- **Case Sensitivity**: Configurable case sensitivity
- **Error Messages**: Clear error messages for invalid enum values
- **Type Safety**: Ensures enum values match expected types

### Nested Validation
- **Object Validation**: Validates nested object structures
- **Array Validation**: Validates array elements
- **Recursive Validation**: Deep validation of nested structures
- **Path Tracking**: Tracks validation errors with field paths

## Error Handling

### Validation Errors
- **Clear Messages**: Descriptive error messages
- **Field Paths**: Path-based error reporting for nested structures
- **Type Information**: Detailed type mismatch information
- **Context**: Schema and data context in error messages

### Error Types
- **Missing Fields**: Required fields not present
- **Type Mismatches**: Data types don't match schema
- **Invalid Values**: Values outside allowed ranges or enums
- **Null Violations**: Null values in non-nullable fields

### Error Reporting
```json
{
  "validation_error": {
    "field_path": "customer.address.city",
    "expected_type": "STRING",
    "actual_type": "INTEGER",
    "message": "Field 'city' must be a string, got integer"
  }
}
```

## Performance Considerations

### Row Validation
- **Immediate Validation**: Validates each row as it's processed
- **Memory Efficient**: Low memory usage
- **Real-time**: Immediate error detection

### DataFrame Validation
- **Batch Validation**: Validates entire DataFrame at once
- **Memory Intensive**: Higher memory usage
- **Fast Processing**: Efficient for smaller datasets

### Dask Validation
- **Lazy Validation**: Validates without materializing data
- **Distributed**: Works with distributed data
- **Scalable**: Handles very large datasets

## Configuration

### Validation Options
- **Schema Name**: Custom schema name for error messages
- **Path Separator**: Separator for nested field paths (default: ".")
- **Strict Mode**: Strict validation mode for type checking
- **Error Handling**: Configurable error handling behavior

### Schema Integration
- **Schema Definition**: Uses schema definitions for validation
- **Field Definitions**: Validates against field definitions
- **Type Constraints**: Enforces type constraints
- **Validation Rules**: Applies validation rules

## Best Practices

### Validation Strategy
- **Early Validation**: Validate data early in the pipeline
- **Schema-First**: Define schemas before processing
- **Error Handling**: Handle validation errors gracefully
- **Performance**: Balance validation thoroughness with performance

### Error Handling
- **Clear Messages**: Provide clear, actionable error messages
- **Context**: Include context in error messages
- **Recovery**: Implement error recovery strategies
- **Logging**: Log validation errors for debugging

### Schema Design
- **Comprehensive**: Define comprehensive schemas
- **Flexible**: Allow for schema evolution
- **Documented**: Document schema requirements
- **Tested**: Test schemas with sample data

### Performance
- **Validation Timing**: Choose appropriate validation timing
- **Memory Management**: Monitor memory usage during validation
- **Error Recovery**: Implement efficient error recovery
- **Monitoring**: Monitor validation performance and errors