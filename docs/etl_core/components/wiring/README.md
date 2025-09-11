# Wiring Components

Wiring components provide **data structure definitions**, **port management**, and **validation** for ETL pipeline data with comprehensive support for complex data types and validation rules.

## Overview

Wiring components define the structure, connections, and validation rules for data flowing through the ETL pipeline, ensuring data integrity and type safety. They provide **comprehensive type support** and **validation capabilities** for robust data processing.

### Key Concepts
- **Port Management**: Define and manage component input/output connections
- **Schema Definition**: Define data structure and validation rules
- **Type Safety**: Ensure data types match schema definitions
- **Validation**: Comprehensive data validation and error handling

## Components

### [Ports](./ports/README.md)
Port definitions and management for component input/output connections with **declarative specifications** and connection constraints.

### [Schema](./schema/README.md)
Schema definitions and validation for data structures and field definitions with **comprehensive type support** and validation rules.

### [Validation](./validation/README.md)
Data validation components for ensuring data integrity and schema compliance with **multi-format support** and error handling.

## Wiring Types

### Port Management
- **Input Ports**: Define component input connections
- **Output Ports**: Define component output connections
- **Edge Routing**: Specify data flow between components
- **Connection Constraints**: Enforce fanin/fanout limits

### Schema Definition
- **Primitive Types**: Basic data types (STRING, INTEGER, FLOAT, BOOLEAN, PATH)
- **Complex Types**: Nested structures (OBJECT, ARRAY, ENUM)
- **Validation Rules**: Data validation and constraint definitions
- **Type Safety**: Comprehensive type checking and validation

### Features
- **Type Safety**: Strong typing with Pydantic models
- **Validation**: Comprehensive data validation
- **Documentation**: Self-documenting schemas and configurations
- **Error Handling**: Clear error messages and validation feedback

## JSON Configuration Examples

### Port Configuration
```json
{
  "input_ports": [
    {
      "name": "data_in",
      "fanin": "many",
      "required": true
    }
  ],
  "output_ports": [
    {
      "name": "data_out",
      "fanout": "many",
      "required": true
    }
  ]
}
```

### Schema Configuration
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
    }
  ]
}
```

## Wiring Features

### Port Management
- **Declarative Specifications**: Define ports using declarative configuration
- **Connection Constraints**: Enforce fanin/fanout limits
- **Edge Routing**: Specify data flow between components
- **Validation**: Validate port connections and constraints

### Schema Definition
- **Type Safety**: Comprehensive type checking and validation
- **Nested Structures**: Support for complex nested data structures
- **Validation Rules**: Flexible validation with custom rules
- **Error Handling**: Detailed error messages and context

### Validation
- **Multi-format Support**: Validate various data formats
- **Schema Compliance**: Ensure data matches schema definitions
- **Error Reporting**: Clear error messages and validation feedback
- **Performance**: Optimized validation for large datasets

## Error Handling

### Wiring Errors
- **Clear Messages**: Descriptive error messages for wiring issues
- **Port Validation**: Path-based error reporting for port problems
- **Schema Validation**: Detailed schema validation information
- **Context**: Wiring and validation context in error messages

### Error Types
- **Port Errors**: Missing required ports, invalid port names
- **Schema Errors**: Type mismatches, missing fields, invalid values
- **Validation Errors**: Data validation failures, constraint violations
- **Connection Errors**: Invalid connections, constraint violations

### Error Reporting
```json
{
  "wiring_error": {
    "error_type": "port_validation_failed",
    "port_name": "input_port",
    "component": "filter_component",
    "message": "Required port 'input_port' not connected"
  }
}
```

## Performance Considerations

### Port Management
- **Efficient Registration**: Optimize port registration and detection
- **Connection Pooling**: Use connection pooling for better performance
- **Port Statistics**: Track port usage and performance metrics
- **Connection Optimization**: Optimize port connections for performance

### Schema Validation
- **Lazy Loading**: Load schemas only when needed
- **Caching**: Cache frequently used schemas
- **Validation**: Optimize validation performance
- **Memory**: Minimize memory usage for large schemas

## Configuration

### Wiring Options
- **Port Configuration**: Configure port specifications and constraints
- **Schema Settings**: Set schema structure and validation rules
- **Validation Options**: Configure validation strategies
- **Performance Tuning**: Optimize wiring performance

## Best Practices

### Port Design
- **Descriptive Names**: Use descriptive port names
- **Consistent Conventions**: Follow consistent naming conventions
- **Optional Ports**: Make ports optional when possible
- **Error Handling**: Implement robust error handling

### Schema Design
- **Descriptive Names**: Use descriptive field names
- **Nullable Fields**: Make fields nullable when appropriate
- **Enum Usage**: Use enums for fixed value sets
- **Extensibility**: Design for schema evolution

### Validation
- **Early Validation**: Validate data early in the pipeline
- **Clear Messages**: Provide clear and descriptive error messages
- **Error Handling**: Handle validation errors gracefully
- **Data Quality**: Use validation for data quality assurance