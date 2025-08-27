# Filter System Documentation

## Overview

The Filter System in ETL-Core provides a powerful and flexible way to filter data across different data processing strategies (row, bulk, and bigdata). It supports both simple column-based filtering and complex logical expressions using AND, OR, and NOT operators.

## Core Components

### FilterComponent
The main component that delegates filtering operations to the `FilterReceiver`. It supports three processing strategies:
- **Row-based**: Processes individual rows one at a time
- **Bulk**: Processes pandas DataFrames in chunks
- **BigData**: Processes Dask DataFrames for large-scale data

### ComparisonRule
Defines the structure for filter rules, supporting both leaf rules (simple comparisons) and logical nodes (complex expressions).

## Filter Rule Types

### 1. Leaf Rules (Simple Comparisons)

Leaf rules perform direct comparisons on specific columns:

```python
{
    "column": "age",
    "operator": ">=",
    "value": 18
}
```

**Supported Operators:**
- `==` - Equal to
- `!=` - Not equal to
- `>` - Greater than
- `<` - Less than
- `>=` - Greater than or equal to
- `<=` - Less than or equal to
- `contains` - String contains (for text columns)

### 2. Logical Node Rules (Complex Expressions)

#### AND Operator
Combines multiple rules with logical AND:

```python
{
    "logical_operator": "AND",
    "rules": [
        {"column": "age", "operator": ">=", "value": 18},
        {"column": "status", "operator": "==", "value": "active"}
    ]
}
```

#### OR Operator
Combines multiple rules with logical OR:

```python
{
    "logical_operator": "OR",
    "rules": [
        {"column": "department", "operator": "==", "value": "IT"},
        {"column": "department", "operator": "==", "value": "Engineering"}
    ]
}
```

#### NOT Operator
Negates a single rule:

```python
{
    "logical_operator": "NOT",
    "rules": [
        {"column": "status", "operator": "==", "value": "inactive"}
    ]
}
```

## Complex Nested Rules

The system supports unlimited nesting of logical operators:

```python
{
    "logical_operator": "AND",
    "rules": [
        {"column": "age", "operator": ">=", "value": 18},
        {
            "logical_operator": "OR",
            "rules": [
                {"column": "department", "operator": "==", "value": "IT"},
                {"column": "department", "operator": "==", "value": "Engineering"}
            ]
        },
        {
            "logical_operator": "NOT",
            "rules": [
                {"column": "status", "operator": "==", "value": "terminated"}
            ]
        }
    ]
}
```

## Data Type Support

### Numeric Columns
- All comparison operators (`==`, `!=`, `>`, `<`, `>=`, `<=`)
- Automatic type conversion for compatible types
- Handles integers, floats, and mixed numeric types

### String Columns
- All comparison operators
- `contains` operator for substring matching
- Case-sensitive comparisons
- Handles empty strings and None values

### Boolean Columns
- `==` and `!=` operators
- Direct boolean value comparison
- Handles None values as False

### Date/Time Columns
- All comparison operators
- Automatic parsing of common date formats
- Handles timezone-aware and naive datetime objects

## Edge Cases and Special Behaviors

### 1. Missing Values (None/NaN)

**Behavior:**
- `None` values are treated as missing data
- NaN values in pandas are handled appropriately
- Missing values in comparisons generally result in False

**Examples:**
```python
# Rule: age > 18
# Row: {"age": None} -> Result: False (filtered out)

# Rule: status == "active"
# Row: {"status": None} -> Result: False (filtered out)
```

### 2. Type Mismatches

**Automatic Type Conversion:**
- Numeric strings are converted to numbers when possible
- Date strings are parsed to datetime objects
- Boolean strings ("true"/"false") are converted to booleans

**Failed Conversions:**
- Invalid conversions result in False (row filtered out)
- No exceptions are thrown during filtering

**Examples:**
```python
# Rule: age > 18
# Row: {"age": "25"} -> Result: True (25 > 18)
# Row: {"age": "abc"} -> Result: False (conversion failed)

# Rule: created_date > "2023-01-01"
# Row: {"created_date": "2023-06-15"} -> Result: True
# Row: {"created_date": "invalid-date"} -> Result: False
```

### 3. Empty Collections

**Empty DataFrames:**
- Returns empty DataFrame (no rows to filter)
- No errors thrown

**Empty Lists in OR/AND:**
- AND with empty rules list: Always False
- OR with empty rules list: Always False
- NOT with empty rules list: Validation error

### 4. Column Not Found

**Behavior:**
- Missing columns result in False (row filtered out)
- No exceptions thrown
- Logs warning for debugging

**Example:**
```python
# Rule: salary > 50000
# Row: {"name": "John", "age": 30} -> Result: False (salary column missing)
```

### 5. Invalid Rule Combinations

**Validation Errors:**
- Rule cannot be both leaf and logical node
- NOT operator requires exactly one sub-rule
- AND/OR operators require at least one sub-rule
- Missing required fields for leaf rules

## Performance Considerations

### Row Strategy
- **Best for:** Small datasets, real-time processing
- **Memory usage:** Low (one row at a time)
- **Performance:** Good for simple rules, slower for complex nested rules

### Bulk Strategy
- **Best for:** Medium datasets, pandas operations
- **Memory usage:** Medium (chunked processing)
- **Performance:** Excellent for vectorized operations

### BigData Strategy
- **Best for:** Large datasets, distributed processing
- **Memory usage:** Low (lazy evaluation)
- **Performance:** Best for complex rules on large datasets

## Error Handling

### Validation Errors
- Rule structure validation at component initialization
- Clear error messages for invalid configurations
- Prevents runtime errors during filtering

### Runtime Errors
- Graceful handling of data type mismatches
- No exceptions thrown during filtering operations
- Failed comparisons result in False (row filtered out)

### Logging
- Warning logs for missing columns
- Debug logs for type conversion failures
- Performance metrics for monitoring

## Usage Examples

### Simple Filter
```python
from etl_core.components.data_operations.filter.filter_component import FilterComponent
from etl_core.components.data_operations.filter.comparison_rule import ComparisonRule

# Create a simple filter
rule = ComparisonRule(
    column="age",
    operator=">=",
    value=18
)

filter_component = FilterComponent(rule=rule)
```

### Complex Filter
```python
# Create a complex nested filter
rule = ComparisonRule(
    logical_operator="AND",
    rules=[
        ComparisonRule(column="age", operator=">=", value=18),
        ComparisonRule(
            logical_operator="OR",
            rules=[
                ComparisonRule(column="department", operator="==", value="IT"),
                ComparisonRule(column="department", operator="==", value="Engineering")
            ]
        ),
        ComparisonRule(
            logical_operator="NOT",
            rules=[
                ComparisonRule(column="status", operator="==", value="terminated")
            ]
        )
    ]
)

filter_component = FilterComponent(rule=rule)
```

### Processing Data
```python
# Row processing
async for filtered_row in filter_component.process_row(row, metrics):
    # Process filtered row
    pass

# Bulk processing
async for filtered_df in filter_component.process_bulk(dataframe, metrics):
    # Process filtered DataFrame chunk
    pass

# BigData processing
async for filtered_ddf in filter_component.process_bigdata(dask_df, metrics):
    # Process filtered Dask DataFrame
    pass
```

## Best Practices

### 1. Rule Design
- Keep leaf rules simple and focused
- Use logical operators to combine simple rules
- Avoid deeply nested structures (>3 levels)
- Test rules with sample data before production

### 2. Performance Optimization
- Use bulk strategy for medium datasets
- Use bigdata strategy for large datasets
- Avoid complex rules in row strategy
- Consider rule complexity vs. data size

### 3. Error Prevention
- Validate data types before filtering
- Handle missing columns gracefully
- Test edge cases with sample data
- Monitor filtering performance metrics

### 4. Maintenance
- Document complex rule structures
- Use descriptive column names
- Regular performance monitoring
- Update rules based on data schema changes

## Troubleshooting

### Common Issues

**1. All rows being filtered out:**
- Check column names match exactly
- Verify data types are compatible
- Test with simple rules first

**2. Unexpected filtering results:**
- Check for None/NaN values
- Verify operator logic
- Test individual rule components

**3. Performance issues:**
- Switch to appropriate strategy (bulk/bigdata)
- Simplify complex nested rules
- Monitor memory usage

**4. Validation errors:**
- Check rule structure syntax
- Ensure required fields are present
- Verify logical operator usage

### Debugging Tips

1. **Start Simple:** Test with basic leaf rules first
2. **Incremental Complexity:** Add logical operators one at a time
3. **Sample Data:** Use small datasets for testing
4. **Logging:** Enable debug logging for detailed information
5. **Metrics:** Monitor filtering performance metrics

## Conclusion

The Filter System provides a robust and flexible solution for data filtering across different processing strategies. By understanding the rule structure, edge cases, and best practices, you can create efficient and reliable filtering operations for your ETL pipelines.

For advanced usage and custom implementations, refer to the `FilterReceiver` and `filter_helper` modules for detailed implementation details.
