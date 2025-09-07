# Wiring Components

This directory contains core wiring and configuration components for ports, schema definitions, and validation.

## Components Overview

### [Ports](./ports/README.md)
Port definitions and management for component input/output connections.

### [Schema](./schema/README.md)
Schema definitions and validation for data structures and field definitions.

### [Validation](./validation/README.md)
Data validation components for ensuring data integrity and schema compliance.

## Common Features

All wiring components support:
- **Type Safety**: Strong typing with Pydantic models
- **Validation**: Comprehensive data validation
- **Documentation**: Self-documenting schemas and configurations
- **Error Handling**: Clear error messages and validation feedback
- **Extensibility**: Easy extension for custom requirements

## Base Components

### Port Specifications
- **InPortSpec**: Input port specifications with fanin control
- **OutPortSpec**: Output port specifications with fanout control
- **EdgeRef**: Edge routing specifications for component connections

### Schema Definitions
- **Schema**: Root schema container with field definitions
- **FieldDef**: Recursive field definitions with data types
- **DataType**: Supported data types enumeration

### Validation Functions
- **Row Validation**: Validate individual data rows
- **DataFrame Validation**: Validate pandas DataFrames
- **Dask Validation**: Validate Dask DataFrames
- **Schema Validation**: Validate data against schemas

## Data Types

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

## Configuration

Wiring components are configured with:
- **Port Specifications**: Input/output port definitions
- **Schema Definitions**: Data structure specifications
- **Validation Rules**: Data validation criteria
- **Type Constraints**: Data type restrictions

## Usage Pattern

Wiring components typically:
1. Define port specifications for components
2. Create schema definitions for data structures
3. Validate data against schemas
4. Provide clear error messages for validation failures
5. Support nested and complex data structures

## Error Handling

- **Validation Errors**: Clear error messages for validation failures
- **Type Errors**: Data type mismatch error handling
- **Schema Errors**: Schema validation error messages
- **Port Errors**: Port configuration error handling
