# CSV Components

CSV file components provide reading and writing capabilities for Comma-Separated Values files with configurable delimiters and encoding support.

## Overview

CSV components offer comprehensive file operations for CSV format with support for various delimiters, encodings, and processing modes. They handle both reading from and writing to CSV files efficiently.

## Components

### CSV Read Component
- **File**: `read_csv.py`
- **Purpose**: Read data from CSV files
- **Features**: Row-by-row streaming, bulk loading, bigdata processing

### CSV Write Component
- **File**: `write_csv.py`
- **Purpose**: Write data to CSV files
- **Features**: Row writing, bulk writing, bigdata writing

### CSV Base Component
- **File**: `csv_component.py`
- **Purpose**: Base class with CSV-specific functionality
- **Features**: Delimiter configuration, common CSV operations

## Features

### Delimiter Support
- **Comma**: Standard comma delimiter (default)
- **Semicolon**: Semicolon delimiter for European formats
- **Tab**: Tab delimiter for TSV files
- **Custom**: Support for custom delimiters

### Processing Modes
- **Row Processing**: Stream data row by row
- **Bulk Processing**: Load entire file into pandas DataFrame
- **BigData Processing**: Use Dask for large file processing

### Data Handling
- **Header Detection**: Automatic header row detection
- **Data Type Inference**: Automatic data type detection
- **Encoding Support**: UTF-8 and other encodings
- **Quote Handling**: Proper handling of quoted fields

## Configuration

### Required Fields
- `filepath`: Path to the CSV file (Path object)

### Optional Fields
- `separator`: Field separator (default: Delimiter.COMMA)
- `encoding`: File encoding (default: UTF-8)
- `header`: Whether file has header row (default: True)

### Delimiter Options
```python
from etl_core.components.file_components.csv.csv_component import Delimiter

# Available delimiters
Delimiter.COMMA      # ","
Delimiter.SEMICOLON  # ";"
Delimiter.TAB        # "\t"
```

## Example Usage

### Read Component
```python
from etl_core.components.file_components.csv.read_csv import ReadCSV
from etl_core.components.file_components.csv.csv_component import Delimiter

# Configure read component
component = ReadCSV(
    name="customer_reader",
    filepath="data/customers.csv",
    separator=Delimiter.COMMA
)
```

### Write Component
```python
from etl_core.components.file_components.csv.write_csv import WriteCSV

# Configure write component
component = WriteCSV(
    name="customer_writer",
    filepath="output/customers.csv",
    separator=Delimiter.COMMA
)
```

## Processing Modes

### Row Processing
- **Streaming**: Process one row at a time
- **Memory Efficient**: Low memory usage
- **Real-time**: Immediate processing
- **Use Case**: Large files, real-time processing

### Bulk Processing
- **DataFrame**: Load entire file into pandas DataFrame
- **Memory Intensive**: Higher memory usage
- **Fast Processing**: Efficient for smaller files
- **Use Case**: Medium-sized files, data analysis

### BigData Processing
- **Dask DataFrame**: Use Dask for distributed processing
- **Scalable**: Handles very large files
- **Parallel Processing**: Multi-core processing
- **Use Case**: Very large files, distributed processing

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
- **Bulk Processing**: < 1GB files
- **BigData Processing**: > 1GB files

## Error Handling

### File Errors
- **File Not Found**: Clear error messages
- **Permission Denied**: File access error handling
- **Invalid Path**: Path validation errors

### Format Errors
- **Invalid Delimiter**: Delimiter detection errors
- **Encoding Errors**: Character encoding issues
- **Malformed CSV**: Invalid CSV format handling

### Data Errors
- **Type Conversion**: Data type conversion errors
- **Missing Values**: Handling of missing data
- **Schema Validation**: Data schema validation errors

## Advanced Features

### Header Handling
- **Automatic Detection**: Detect header row automatically
- **Custom Headers**: Specify custom column names
- **No Headers**: Handle files without headers

### Data Type Inference
- **Automatic Detection**: Infer data types from content
- **Type Conversion**: Convert strings to appropriate types
- **Error Handling**: Handle type conversion errors

### Encoding Support
- **UTF-8**: Default encoding
- **Latin-1**: Legacy encoding support
- **Custom Encodings**: Support for various encodings
- **BOM Handling**: Byte Order Mark handling
