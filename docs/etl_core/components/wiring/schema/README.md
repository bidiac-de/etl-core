# Schema Components

Schema components provide **data structure definitions** and validation for ETL pipeline data with comprehensive support for complex data types and validation rules.

## Overview

Schema components define the structure and validation rules for data flowing through the ETL pipeline, ensuring data integrity and type safety. They provide **comprehensive type support** and **validation capabilities** for robust data processing.

### Key Concepts
- **Schema Definition**: Define data structure and validation rules
- **Type Safety**: Ensure data types match schema definitions
- **Validation**: Comprehensive data validation and error handling
- **Nested Structures**: Support for complex nested data structures

## Components

### Schema Definition
- **Schema**: Root schema container with field definitions
- **FieldDef**: Recursive field definitions with data types
- **DataType**: Supported data types enumeration

## Schema Types

### Primitive Types
- **STRING**: Text data
- **INTEGER**: Whole numbers
- **FLOAT**: Decimal numbers
- **BOOLEAN**: True/false values
- **PATH**: File system paths

### Complex Types
- **OBJECT**: Nested object structures
- **ARRAY**: Array/list data structures
- **ENUM**: Enumerated values with predefined options

### Features
- **Type Safety**: Comprehensive type checking and validation
- **Nested Support**: Recursive field definitions for complex structures
- **Validation Rules**: Flexible validation with custom rules
- **Error Handling**: Detailed error messages and context

## JSON Configuration Examples

### Simple Schema

```json
{
  "name": "customer_schema",
  "fields": [
    {
      "name": "id",
      "data_type": "INTEGER",
      "nullable": false
    },
    {
      "name": "name",
      "data_type": "STRING",
      "nullable": false
    },
    {
      "name": "email",
      "data_type": "STRING",
      "nullable": true
    },
    {
      "name": "active",
      "data_type": "BOOLEAN",
      "nullable": false
    }
  ]
}
```

### Nested Object Schema

```json
{
  "name": "order_schema",
  "fields": [
    {
      "name": "order_id",
      "data_type": "INTEGER",
      "nullable": false
    },
    {
      "name": "customer",
      "data_type": "OBJECT",
      "nullable": false,
      "children": [
        {
          "name": "id",
          "data_type": "INTEGER",
          "nullable": false
        },
        {
          "name": "name",
          "data_type": "STRING",
          "nullable": false
        }
      ]
    },
    {
      "name": "items",
      "data_type": "ARRAY",
      "nullable": false,
      "item": {
        "name": "item",
        "data_type": "OBJECT",
        "nullable": false,
        "children": [
          {
            "name": "product_id",
            "data_type": "INTEGER",
            "nullable": false
          },
          {
            "name": "quantity",
            "data_type": "INTEGER",
            "nullable": false
          },
          {
            "name": "price",
            "data_type": "FLOAT",
            "nullable": false
          }
        ]
      }
    },
    {
      "name": "status",
      "data_type": "ENUM",
      "nullable": false,
      "enum_values": ["pending", "processing", "shipped", "delivered"]
    }
  ]
}
```

### Array Schema

```json
{
  "name": "product_schema",
  "fields": [
    {
      "name": "id",
      "data_type": "INTEGER",
      "nullable": false
    },
    {
      "name": "name",
      "data_type": "STRING",
      "nullable": false
    },
    {
      "name": "tags",
      "data_type": "ARRAY",
      "nullable": true,
      "item": {
        "name": "tag",
        "data_type": "STRING",
        "nullable": false
      }
    }
  ]
}
```

### Enum Schema

```json
{
  "name": "user_schema",
  "fields": [
    {
      "name": "id",
      "data_type": "INTEGER",
      "nullable": false
    },
    {
      "name": "role",
      "data_type": "ENUM",
      "nullable": false,
      "enum_values": ["admin", "user", "guest"]
    },
    {
      "name": "status",
      "data_type": "ENUM",
      "nullable": false,
      "enum_values": ["active", "inactive", "pending"]
    }
  ]
}
```

## Schema Features

### Type Safety
- **Comprehensive Validation**: Ensures data types match schema definitions
- **Type Coercion**: Automatic type conversion where appropriate
- **Type Checking**: Strict type compliance validation
- **Error Messages**: Clear type mismatch error messages

### Nested Structures
- **Recursive Definitions**: Support for complex nested data structures
- **Object Validation**: Proper nested object structure validation
- **Array Validation**: Array elements validated against item schemas
- **Path-based Errors**: Detailed error reporting for nested structures

### Validation Rules
- **Flexible Rules**: Comprehensive data validation capabilities
- **Custom Validation**: Support for custom validation functions
- **Error Context**: Detailed error information and context
- **Validation Recovery**: Graceful handling of validation failures

## Error Handling

### Schema Errors
- **Clear Messages**: Descriptive error messages for schema validation failures
- **Field Validation**: Path-based error reporting for nested structures
- **Type Information**: Detailed type mismatch information
- **Context**: Schema and validation context in error messages

### Error Types
- **Missing Fields**: Required fields not present in data
- **Type Mismatches**: Data types incompatible with schema definitions
- **Invalid Values**: Values not matching enum constraints
- **Structure Errors**: Invalid nested structure or array format

### Error Reporting
```json
{
  "schema_error": {
    "field_path": "customer.orders.total",
    "expected_type": "FLOAT",
    "actual_type": "STRING",
    "error_type": "type_mismatch",
    "message": "Field 'total' expected FLOAT but got STRING"
  }
}
```

## Performance Considerations

### Schema Loading
- **Lazy Loading**: Load schemas only when needed
- **Caching**: Cache frequently used schemas
- **Validation**: Optimize validation performance
- **Memory**: Minimize memory usage for large schemas

### Validation Performance
- **Early Validation**: Validate data early in the pipeline
- **Batch Validation**: Validate data in batches when possible
- **Error Handling**: Handle validation errors efficiently
- **Performance**: Balance validation thoroughness with performance

## Configuration

### Schema Options
- **Schema Definition**: Configure schema structure and validation rules
- **Type Settings**: Set data type constraints and validation options
- **Error Handling**: Configure error handling strategies
- **Performance Tuning**: Optimize schema validation performance

## Best Practices

### Schema Design
- **Descriptive Names**: Use descriptive field names
- **Nullable Fields**: Make fields nullable when appropriate
- **Enum Usage**: Use enums for fixed value sets
- **Extensibility**: Design for schema evolution

### Type Selection
- **Appropriate Types**: Use appropriate data types for your use case
- **Performance**: Consider data size and performance implications
- **Categorical Data**: Use enums for categorical data
- **Complex Structures**: Use objects for complex nested structures

### Validation
- **Early Validation**: Validate data early in the pipeline
- **Clear Messages**: Provide clear and descriptive error messages
- **Error Handling**: Handle validation errors gracefully
- **Data Quality**: Use schema validation for data quality assurance

### Schema Management
- **Version Control**: Version your schemas for tracking changes
- **Documentation**: Document schema requirements and constraints
- **Testing**: Test schemas with sample data
- **Evolution**: Plan for schema evolution and migration
