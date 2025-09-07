# JSON Components

JSON file components provide reading and writing capabilities for JavaScript Object Notation files with streaming support and flexible schema handling.

## Overview

JSON components offer comprehensive file operations for JSON format with support for streaming, nested data structures, and flexible schema handling. They handle both reading from and writing to JSON files efficiently.

## Components

### JSON Read Component
- **File**: `read_json.py`
- **Purpose**: Read data from JSON files
- **Features**: Streaming support, nested data handling, flexible schema

### JSON Write Component
- **File**: `write_json.py`
- **Purpose**: Write data to JSON files
- **Features**: Streaming writing, formatting options, schema validation

### JSON Base Component
- **File**: `json_component.py`
- **Purpose**: Base class with JSON-specific functionality
- **Features**: Common JSON operations, streaming support

## Features

### Streaming Support
- **Large Files**: Efficient handling of large JSON files
- **Memory Efficient**: Low memory usage for streaming
- **Real-time Processing**: Process data as it's read
- **Incremental Processing**: Process data in chunks

### Flexible Schema
- **Dynamic Schema**: Handle varying JSON structures
- **Nested Data**: Support for complex nested objects
- **Array Processing**: Efficient array data processing
- **Type Flexibility**: Handle various JSON data types

### Data Types
- **Primitive Types**: String, number, boolean, null
- **Complex Types**: Objects, arrays
- **Nested Structures**: Deeply nested JSON structures
- **Mixed Types**: Arrays with mixed data types

## Configuration

### Required Fields
- `filepath`: Path to the JSON file (Path object)

### Optional Fields
- `encoding`: File encoding (default: UTF-8)
- `indent`: JSON formatting indentation
- `ensure_ascii`: Ensure ASCII encoding (default: False)
- `sort_keys`: Sort JSON keys (default: False)

### JSON Options
```python
# Configure JSON formatting
component = WriteJSON(
    name="data_writer",
    filepath="output/data.json",
    indent=2,
    ensure_ascii=False,
    sort_keys=True
)
```

## Example Usage

### Read Component
```python
from etl_core.components.file_components.json.read_json import ReadJSON

# Configure read component
component = ReadJSON(
    name="data_reader",
    filepath="data/input.json"
)
```

### Write Component
```python
from etl_core.components.file_components.json.write_json import WriteJSON

# Configure write component
component = WriteJSON(
    name="data_writer",
    filepath="output/data.json",
    indent=2
)
```

## Processing Modes

### Row Processing
- **Streaming**: Process JSON objects one at a time
- **Memory Efficient**: Low memory usage
- **Real-time**: Immediate processing
- **Use Case**: Large files, real-time processing

### Bulk Processing
- **DataFrame**: Load JSON into pandas DataFrame
- **Memory Intensive**: Higher memory usage
- **Fast Processing**: Efficient for smaller files
- **Use Case**: Medium-sized files, data analysis

### BigData Processing
- **Dask DataFrame**: Use Dask for distributed processing
- **Scalable**: Handles very large files
- **Parallel Processing**: Multi-core processing
- **Use Case**: Very large files, distributed processing

## JSON-Specific Features

### Streaming Support
- **Large Files**: Handle files larger than memory
- **Memory Efficient**: Process data incrementally
- **Real-time**: Stream data as it's processed
- **Chunked Processing**: Process data in manageable chunks

### Nested Data Handling
- **Deep Nesting**: Handle deeply nested JSON structures
- **Object Flattening**: Flatten nested objects for processing
- **Array Processing**: Efficient array data processing
- **Path Access**: Access nested data using dot notation

### Schema Flexibility
- **Dynamic Schema**: Handle varying JSON structures
- **Optional Fields**: Handle missing or optional fields
- **Type Coercion**: Convert data types as needed
- **Validation**: Validate JSON against schemas

## Performance Considerations

### Memory Usage
- **Row Processing**: Minimal memory usage
- **Bulk Processing**: Memory usage proportional to file size
- **BigData Processing**: Distributed memory usage

### Processing Speed
- **Row Processing**: Slower but memory efficient
- **Bulk Processing**: Fastest for smaller files
- **BigData Processing**: Fastest for very large files

### File Size Recommendations
- **Row Processing**: Any size, memory constrained
- **Bulk Processing**: < 500MB files
- **BigData Processing**: > 500MB files

## Error Handling

### File Errors
- **File Not Found**: Clear error messages
- **Permission Denied**: File access error handling
- **Invalid Format**: JSON format validation
- **Corrupted File**: Handle corrupted JSON files

### JSON Errors
- **Invalid JSON**: Malformed JSON syntax handling
- **Encoding Errors**: Character encoding issues
- **Schema Errors**: JSON schema validation errors
- **Type Errors**: Data type conversion errors

### Data Errors
- **Missing Fields**: Handle missing required fields
- **Type Mismatches**: Handle unexpected data types
- **Validation Errors**: Data validation error handling
- **Schema Validation**: JSON schema validation errors

## Advanced Features

### Streaming Processing
- **Large Files**: Handle files larger than available memory
- **Memory Efficient**: Process data incrementally
- **Real-time**: Stream data as it's processed
- **Chunked Reading**: Read data in manageable chunks

### Schema Validation
- **JSON Schema**: Validate against JSON schemas
- **Type Validation**: Validate data types
- **Required Fields**: Ensure required fields are present
- **Custom Validation**: Custom validation rules

### Data Transformation
- **Object Flattening**: Flatten nested objects
- **Array Expansion**: Expand arrays into rows
- **Type Conversion**: Convert data types
- **Field Mapping**: Map fields to different names

### Formatting Options
- **Indentation**: Pretty-print JSON with indentation
- **Key Sorting**: Sort JSON keys alphabetically
- **ASCII Encoding**: Ensure ASCII-only output
- **Compact Format**: Minimize JSON file size
