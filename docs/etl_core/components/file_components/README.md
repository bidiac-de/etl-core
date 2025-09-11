# File Components

This directory contains components for reading and writing various file formats with comprehensive support for different processing strategies and file types.

## Components Overview

### [CSV](./csv/README.md)
CSV file reading and writing components with **configurable delimiters** and encoding support for efficient data processing.

### [Excel](./excel/README.md)
Excel file reading and writing components with **sheet support** and formatting preservation for complex spreadsheet operations.

### [JSON](./json/README.md)
JSON file reading and writing components with **streaming support** and flexible schema handling for structured data processing.

## Common Features

All file components support:
- **Multi-Processing Support**: Row, bulk, and bigdata processing modes
- **File Path Validation**: Automatic path validation and conversion
- **Error Handling**: Robust file operation error handling
- **Metrics Integration**: Built-in performance metrics collection
- **Schema Validation**: Input/output schema validation
- **Streaming Support**: Efficient streaming for large files

## File Format Support

### CSV Files
- **Delimiter Support**: Comma, semicolon, tab delimiters
- **Encoding Support**: UTF-8 and other encodings
- **Header Handling**: Automatic header detection and processing
- **Data Type Inference**: Automatic data type detection

### Excel Files
- **Sheet Support**: Multiple worksheet support
- **Format Preservation**: Maintains Excel formatting
- **Data Type Handling**: Proper data type conversion
- **Large File Support**: Efficient handling of large Excel files

### JSON Files
- **Streaming Support**: Efficient streaming for large JSON files
- **Schema Flexibility**: Flexible JSON schema handling
- **Nested Data**: Support for complex nested JSON structures
- **Array Processing**: Efficient array data processing

## Processing Modes

### Row Processing
- **Streaming**: Process data row by row
- **Memory Efficient**: Low memory usage for large files
- **Real-time**: Immediate processing and output

### Bulk Processing
- **DataFrame Processing**: Load entire file into pandas DataFrame
- **Batch Operations**: Efficient batch processing
- **Memory Intensive**: Higher memory usage but faster processing

### BigData Processing
- **Dask DataFrames**: Use Dask for large file processing
- **Distributed Processing**: Parallel processing capabilities
- **Scalable**: Handles very large files efficiently

## Configuration

File components are configured with:
- **File Path**: Path to the input/output file
- **Format-Specific Options**: Delimiters, sheets, encoding, etc.
- **Processing Options**: Row/bulk/bigdata mode selection
- **Schema Options**: Input/output schema definitions

## Usage Pattern

File components typically:
1. Validate file path and format
2. Open file with appropriate settings
3. Process data according to selected mode
4. Output processed data through ports
5. Handle errors and cleanup resources

## Error Handling

- **File Not Found**: Clear error messages for missing files
- **Permission Errors**: File access permission handling
- **Format Errors**: Invalid file format detection
- **Encoding Errors**: Character encoding issue handling
- **Resource Cleanup**: Automatic resource cleanup on errors