# Schema Components

Schema components provide data structure definitions and validation for ETL pipeline data.

## Overview

Schema components define the structure and validation rules for data flowing through the ETL pipeline, ensuring data integrity and type safety.

## Components

### Schema Definition
- **Schema**: Root schema container with field definitions
- **FieldDef**: Recursive field definitions with data types
- **DataType**: Supported data types enumeration

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

## Schema Structure

### Root Schema
```python
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType

schema = Schema(
    fields=[
        FieldDef(
            name="customer_id",
            data_type=DataType.INTEGER,
            nullable=False
        ),
        FieldDef(
            name="customer_name",
            data_type=DataType.STRING,
            nullable=False
        )
    ]
)
```

### Nested Objects
```python
# Nested object structure
address_field = FieldDef(
    name="address",
    data_type=DataType.OBJECT,
    nullable=True,
    children=[
        FieldDef(name="street", data_type=DataType.STRING, nullable=False),
        FieldDef(name="city", data_type=DataType.STRING, nullable=False),
        FieldDef(name="zip_code", data_type=DataType.STRING, nullable=False)
    ]
)
```

### Arrays
```python
# Array of items
tags_field = FieldDef(
    name="tags",
    data_type=DataType.ARRAY,
    nullable=True,
    item=FieldDef(
        name="tag",
        data_type=DataType.STRING,
        nullable=False
    )
)
```

### Enums
```python
# Enumerated values
status_field = FieldDef(
    name="status",
    data_type=DataType.ENUM,
    nullable=False,
    enum_values=["active", "inactive", "pending"]
)
```

## Field Definition

### FieldDef Properties
- **name**: Field name
- **data_type**: Data type from DataType enum
- **nullable**: Whether the field can be null (default: False)
- **enum_values**: Allowed values for ENUM type
- **children**: Child fields for OBJECT type
- **item**: Item schema for ARRAY type

### Validation Rules
- **OBJECT fields**: Must have children defined
- **ARRAY fields**: Must have item schema defined
- **ENUM fields**: Must have enum_values defined
- **Other types**: Cannot have children, item, or enum_values

## Example Usage

### Simple Schema
```python
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType

# Customer schema
customer_schema = Schema(
    fields=[
        FieldDef(name="id", data_type=DataType.INTEGER, nullable=False),
        FieldDef(name="name", data_type=DataType.STRING, nullable=False),
        FieldDef(name="email", data_type=DataType.STRING, nullable=True),
        FieldDef(name="active", data_type=DataType.BOOLEAN, nullable=False)
    ]
)
```

### Complex Schema
```python
# Complex nested schema
order_schema = Schema(
    fields=[
        FieldDef(name="order_id", data_type=DataType.INTEGER, nullable=False),
        FieldDef(name="customer", data_type=DataType.OBJECT, nullable=False, children=[
            FieldDef(name="id", data_type=DataType.INTEGER, nullable=False),
            FieldDef(name="name", data_type=DataType.STRING, nullable=False)
        ]),
        FieldDef(name="items", data_type=DataType.ARRAY, nullable=False, item=FieldDef(
            name="item",
            data_type=DataType.OBJECT,
            nullable=False,
            children=[
                FieldDef(name="product_id", data_type=DataType.INTEGER, nullable=False),
                FieldDef(name="quantity", data_type=DataType.INTEGER, nullable=False),
                FieldDef(name="price", data_type=DataType.FLOAT, nullable=False)
            ]
        )),
        FieldDef(name="status", data_type=DataType.ENUM, nullable=False, 
                enum_values=["pending", "processing", "shipped", "delivered"])
    ]
)
```

## Schema Validation

### Required Fields
- **Non-nullable fields**: Must be present and not null
- **Field presence**: All required fields must be present
- **Type validation**: Fields must match their data types

### Type Validation
- **Primitive types**: Validate against Python types
- **OBJECT types**: Validate nested structure
- **ARRAY types**: Validate array elements
- **ENUM types**: Validate against allowed values

### Error Handling
- **Missing fields**: Clear error messages for missing required fields
- **Type mismatches**: Detailed type error messages
- **Invalid values**: Clear messages for invalid enum values
- **Nested errors**: Path-based error messages for nested structures

## Best Practices

### Schema Design
- Use descriptive field names
- Make fields nullable when appropriate
- Use enums for fixed value sets
- Design for extensibility

### Type Selection
- Use appropriate data types
- Consider data size and performance
- Use enums for categorical data
- Use objects for complex structures

### Validation
- Validate early in the pipeline
- Provide clear error messages
- Handle validation errors gracefully
- Use schema validation for data quality
