# Excel Components

Excel file components provide reading and writing capabilities for Microsoft Excel files with sheet support and formatting preservation.

## Overview

Excel components offer comprehensive file operations for Excel format with support for multiple worksheets, formatting preservation, and various data types. They handle both reading from and writing to Excel files efficiently.

## Components

### Excel Read Component
- **File**: `read_excel.py`
- **Purpose**: Read data from Excel files
- **Features**: Sheet selection, row-by-row streaming, bulk loading, bigdata processing

### Excel Write Component
- **File**: `write_excel.py`
- **Purpose**: Write data to Excel files
- **Features**: Sheet writing, formatting preservation, bulk writing, bigdata writing

### Excel Base Component
- **File**: `excel_component.py`
- **Purpose**: Base class with Excel-specific functionality
- **Features**: Sheet configuration, common Excel operations

## Features

### Sheet Support
- **Multiple Sheets**: Support for multiple worksheets
- **Sheet Selection**: Specify which sheet to read/write
- **Sheet Names**: Custom sheet naming
- **Sheet Creation**: Automatic sheet creation

### Data Types
- **Numeric Data**: Proper handling of numbers and dates
- **Text Data**: String data preservation
- **Date/Time**: Excel date/time format support
- **Boolean Values**: Boolean data type support

### Formatting
- **Format Preservation**: Maintains Excel formatting
- **Cell Formatting**: Preserves cell styles and formatting
- **Column Widths**: Maintains column width settings
- **Row Heights**: Preserves row height settings

## Configuration

### Required Fields
- `filepath`: Path to the Excel file (Path object)

### Optional Fields
- `sheet_name`: Name of the sheet to read/write (default: "Sheet1")
- `engine`: Excel engine to use (default: "openpyxl")
- `header`: Whether file has header row (default: True)

### Sheet Configuration
```python
# Specify sheet by name
component = ReadExcel(
    name="data_reader",
    filepath="data/workbook.xlsx",
    sheet_name="Sales Data"
)

# Use default sheet
component = ReadExcel(
    name="data_reader",
    filepath="data/workbook.xlsx"
)
```

## Example Usage

### Read Component
```python
from etl_core.components.file_components.excel.read_excel import ReadExcel

# Configure read component
component = ReadExcel(
    name="sales_reader",
    filepath="data/sales.xlsx",
    sheet_name="Q1_Sales"
)
```

### Write Component
```python
from etl_core.components.file_components.excel.write_excel import WriteExcel

# Configure write component
component = WriteExcel(
    name="sales_writer",
    filepath="output/sales.xlsx",
    sheet_name="Q1_Sales"
)
```

## Processing Modes

### Row Processing
- **Streaming**: Process one row at a time
- **Memory Efficient**: Low memory usage
- **Real-time**: Immediate processing
- **Use Case**: Large files, real-time processing

### Bulk Processing
- **DataFrame**: Load entire sheet into pandas DataFrame
- **Memory Intensive**: Higher memory usage
- **Fast Processing**: Efficient for smaller files
- **Use Case**: Medium-sized files, data analysis

### BigData Processing
- **Dask DataFrame**: Use Dask for distributed processing
- **Scalable**: Handles very large files
- **Parallel Processing**: Multi-core processing
- **Use Case**: Very large files, distributed processing

## Excel-Specific Features

### Multiple Sheets
- **Sheet Selection**: Choose specific sheet to process
- **Sheet Iteration**: Process multiple sheets
- **Sheet Metadata**: Access sheet properties
- **Sheet Creation**: Create new sheets

### Data Type Handling
- **Excel Types**: Proper handling of Excel data types
- **Date Conversion**: Excel date serial number conversion
- **Number Formatting**: Preserve number formatting
- **Formula Handling**: Handle Excel formulas

### Formatting Preservation
- **Cell Styles**: Preserve cell formatting
- **Colors**: Maintain cell and font colors
- **Borders**: Preserve cell borders
- **Alignment**: Maintain text alignment

## Performance Considerations

### Memory Usage
- **Row Processing**: Minimal memory usage
- **Bulk Processing**: Memory usage proportional to sheet size
- **BigData Processing**: Distributed memory usage

### Processing Speed
- **Row Processing**: Slower but memory efficient
- **Bulk Processing**: Fastest for smaller files
- **BigData Processing**: Fastest for very large files

### File Size Recommendations
- **Row Processing**: Any size, memory constrained
- **Bulk Processing**: < 100MB files
- **BigData Processing**: > 100MB files

## Error Handling

### File Errors
- **File Not Found**: Clear error messages
- **Permission Denied**: File access error handling
- **Invalid Format**: Excel format validation
- **Corrupted File**: Handle corrupted Excel files

### Sheet Errors
- **Sheet Not Found**: Handle missing sheets
- **Invalid Sheet Name**: Sheet name validation
- **Sheet Access**: Sheet permission errors

### Data Errors
- **Type Conversion**: Data type conversion errors
- **Missing Values**: Handling of missing data
- **Formula Errors**: Excel formula error handling
- **Schema Validation**: Data schema validation errors

## Advanced Features

### Engine Selection
- **openpyxl**: Default engine for .xlsx files
- **xlrd**: Legacy engine for .xls files
- **xlsxwriter**: High-performance writing engine
- **pyxlsb**: Binary Excel format support

### Data Type Inference
- **Automatic Detection**: Infer data types from Excel content
- **Type Conversion**: Convert Excel types to Python types
- **Date Handling**: Proper Excel date conversion
- **Number Formatting**: Preserve Excel number formats

### Large File Handling
- **Chunked Reading**: Read large files in chunks
- **Memory Management**: Efficient memory usage
- **Streaming**: Stream large files without loading entirely
- **Progress Tracking**: Track processing progress
