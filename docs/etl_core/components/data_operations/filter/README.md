# Filter Component

The Filter Component provides **powerful data filtering capabilities** based on comparison rules with comprehensive support for complex logical expressions and multiple processing strategies.

## Overview

The Filter Component provides powerful data filtering capabilities based on comparison rules. It supports both simple column-based filtering and complex logical expressions using AND, OR, and NOT operators across all three processing strategies (row, bulk, bigdata).

## Components

### Filter Functions
- **Row Filtering**: Filter individual data rows
- **DataFrame Filtering**: Filter pandas DataFrames
- **Dask Filtering**: Filter Dask DataFrames
- **Rule Compilation**: Compile filter rules for efficient evaluation

## Filter Types

### Row Filtering
Filters individual data rows based on comparison rules.

#### Features
- **Real-time Processing**: Immediate filtering as data arrives
- **Memory Efficient**: Low memory usage for streaming data
- **Rule Compilation**: Compiled rules for efficient evaluation
- **Port Routing**: Routes data to pass/fail ports based on criteria

### DataFrame Filtering
Filters pandas DataFrames using vectorized operations.

#### Features
- **Vectorized Operations**: High-performance DataFrame filtering
- **Batch Processing**: Efficient processing of medium-sized datasets
- **Complex Rules**: Support for nested logical expressions
- **Type Safety**: Automatic type checking and conversion

### Dask Filtering
Filters Dask DataFrames for distributed processing.

#### Features
- **Distributed Processing**: Handles large datasets across multiple cores
- **Lazy Evaluation**: Deferred computation for memory efficiency
- **Scalable**: Processes datasets of virtually any size
- **Parallel Execution**: Concurrent filtering operations

## Filter Rule Types

### Leaf Rules (Simple Comparisons)

Leaf rules perform direct comparisons on specific columns:

```json
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

### Logical Node Rules (Complex Expressions)

#### AND Operator
Combines multiple rules with logical AND:

```json
{
  "logical_operator": "AND",
  "rules": [
    {
      "column": "age",
      "operator": ">=",
      "value": 18
    },
    {
      "column": "status",
      "operator": "==",
      "value": "active"
    }
  ]
}
```

#### OR Operator
Combines multiple rules with logical OR:

```json
{
  "logical_operator": "OR",
  "rules": [
    {
      "column": "department",
      "operator": "==",
      "value": "IT"
    },
    {
      "column": "department",
      "operator": "==",
      "value": "Engineering"
    }
  ]
}
```

#### NOT Operator
Negates a single rule:

```json
{
  "logical_operator": "NOT",
  "rules": [
    {
      "column": "status",
      "operator": "==",
      "value": "inactive"
    }
  ]
}
```

## Complex Nested Rules

The system supports unlimited nesting of logical operators:

```json
{
  "logical_operator": "AND",
  "rules": [
    {
      "column": "age",
      "operator": ">=",
      "value": 18
    },
    {
      "logical_operator": "OR",
      "rules": [
        {
          "column": "department",
          "operator": "==",
          "value": "IT"
        },
        {
          "column": "department",
          "operator": "==",
          "value": "Engineering"
        }
      ]
    },
    {
      "logical_operator": "NOT",
      "rules": [
        {
          "column": "status",
          "operator": "==",
          "value": "terminated"
        }
      ]
    }
  ]
}
```

## JSON Configuration Examples

### Basic Component Configuration

```json
{
  "name": "adult_filter",
  "description": "Filter for adult users only",
  "comp_type": "filter",
  "rule": {
    "column": "age",
    "operator": ">=",
    "value": 18
  },
  "strategy_type": "bulk"
}
```

### Complex Department Filter

```json
{
  "name": "tech_departments",
  "description": "Filter for technology departments",
  "comp_type": "filter",
  "rule": {
    "logical_operator": "OR",
    "rules": [
      {
        "column": "department",
        "operator": "==",
        "value": "IT"
      },
      {
        "column": "department",
        "operator": "==",
        "value": "Engineering"
      },
      {
        "column": "department",
        "operator": "==",
        "value": "Data Science"
      }
    ]
  },
  "strategy_type": "bulk"
}
```

### Advanced Multi-Criteria Filter

```json
{
  "name": "senior_employees",
  "description": "Filter for senior employees",
  "comp_type": "filter",
  "rule": {
    "logical_operator": "AND",
    "rules": [
      {
        "column": "age",
        "operator": ">=",
        "value": 30
      },
      {
        "column": "experience_years",
        "operator": ">=",
        "value": 5
      },
      {
        "logical_operator": "OR",
        "rules": [
          {
            "column": "department",
            "operator": "==",
            "value": "Management"
          },
          {
            "column": "level",
            "operator": ">=",
            "value": "Senior"
          }
        ]
      },
      {
        "logical_operator": "NOT",
        "rules": [
          {
            "column": "status",
            "operator": "==",
            "value": "inactive"
          }
        ]
      }
    ]
  },
  "strategy_type": "bulk"
}
```

### String Filter with Contains

```json
{
  "name": "urgent_tasks",
  "description": "Filter for urgent tasks",
  "comp_type": "filter",
  "rule": {
    "column": "description",
    "operator": "contains",
    "value": "urgent"
  },
  "strategy_type": "row"
}
```

### Numeric Range Filter

```json
{
  "name": "salary_range",
  "description": "Filter for salary range",
  "comp_type": "filter",
  "rule": {
    "logical_operator": "AND",
    "rules": [
      {
        "column": "salary",
        "operator": ">=",
        "value": 50000
      },
      {
        "column": "salary",
        "operator": "<=",
        "value": 100000
      }
    ]
  },
  "strategy_type": "bulk"
}
```

### Date Filter

```json
{
  "name": "recent_orders",
  "description": "Filter for recent orders",
  "comp_type": "filter",
  "rule": {
    "column": "created_date",
    "operator": ">=",
    "value": "2024-01-01"
  },
  "strategy_type": "bulk"
}
```

### Null Value Filter

```json
{
  "name": "valid_emails",
  "description": "Filter for valid email addresses",
  "comp_type": "filter",
  "rule": {
    "column": "email",
    "operator": "!=",
    "value": null
  },
  "strategy_type": "bulk"
}
```

### Case-Insensitive Filter

```json
{
  "name": "active_status",
  "description": "Filter for active status (case-insensitive)",
  "comp_type": "filter",
  "rule": {
    "column": "status",
    "operator": "==",
    "value": "active",
    "case_sensitive": false
  },
  "strategy_type": "bulk"
}
```

### BigData Processing Filter

```json
{
  "name": "bigdata_filter",
  "description": "Filter for big data processing",
  "comp_type": "filter",
  "rule": {
    "column": "value",
    "operator": ">",
    "value": 1000
  },
  "strategy_type": "bigdata",
  "chunk_size": 10000
}
```

### Row Processing Filter

```json
{
  "name": "real_time_filter",
  "description": "Real-time data filtering",
  "comp_type": "filter",
  "rule": {
    "logical_operator": "AND",
    "rules": [
      {
        "column": "priority",
        "operator": "==",
        "value": "high"
      },
      {
        "column": "processed",
        "operator": "==",
        "value": false
      }
    ]
  },
  "strategy_type": "row"
}
```

## Advanced Features

### Data Type Support

**Numeric Comparisons:**
```json
{
  "column": "age",
  "operator": ">=",
  "value": 18
}
```

**String Comparisons:**
```json
{
  "column": "status",
  "operator": "==",
  "value": "active"
}
```

**Date/Time Comparisons:**
```json
{
  "column": "created_date",
  "operator": ">=",
  "value": "2024-01-01"
}
```

### Custom Operators

**Array Contains:**
```json
{
  "column": "tags",
  "operator": "array_contains",
  "value": "urgent"
}
```

**Regex Matching:**
```json
{
  "column": "email",
  "operator": "regex",
  "value": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
}
```

**Range Checking:**
```json
{
  "column": "score",
  "operator": "between",
  "value": [80, 100]
}
```

## Filter Features

### Rule Compilation
- **Automatic Compilation**: Rules are compiled into efficient evaluation functions
- **Performance Optimization**: Compiled rules provide better performance
- **Caching**: Frequently used rules are cached to avoid recompilation
- **Vectorized Evaluation**: Vectorized operations for DataFrame processing

### Data Type Support
- **Primitive Types**: String, integer, float, boolean validation
- **Complex Types**: Object and array validation
- **Type Coercion**: Automatic type conversion where appropriate
- **Type Safety**: Ensures data types match filter requirements

### Logical Operations
- **Comparison Operators**: ==, !=, >, <, >=, <=, contains
- **Logical Operators**: AND, OR, NOT operations
- **Nested Rules**: Unlimited depth of logical expressions
- **Custom Operators**: User-defined comparison operators

## Error Handling

### Filter Errors
- **Clear Messages**: Descriptive error messages for filter failures
- **Rule Validation**: Path-based error reporting for complex rules
- **Type Information**: Detailed type mismatch information
- **Context**: Data and rule context in error messages

### Error Types
- **Missing Fields**: Required fields not present in data
- **Type Mismatches**: Data types incompatible with operators
- **Invalid Rules**: Malformed or unsupported filter rules
- **Evaluation Errors**: Runtime errors during rule evaluation

### Error Reporting
```json
{
  "filter_error": {
    "field_path": "customer.age",
    "operator": ">=",
    "expected_type": "INTEGER",
    "actual_type": "STRING",
    "message": "Cannot compare string field 'age' with numeric operator"
  }
}
```

## Performance Considerations

### Row Processing
- **Immediate Processing**: Filters data as it arrives
- **Memory Efficient**: Low memory usage for streaming data
- **Rule Compilation**: Compiled rules for efficient evaluation
- **Real-time**: Immediate filtering results

### Bulk Processing
- **Vectorized Operations**: High-performance DataFrame filtering
- **Batch Processing**: Efficient processing of medium-sized datasets
- **Memory Management**: Optimized memory usage for DataFrames
- **Parallel Processing**: Concurrent filtering operations

### BigData Processing
- **Distributed Processing**: Handles large datasets across multiple cores
- **Lazy Evaluation**: Deferred computation for memory efficiency
- **Scalable**: Processes datasets of virtually any size
- **Performance**: Optimized for big data scenarios

## Configuration

### Filter Options
- **Rule Definition**: Define filter rules and logical expressions
- **Port Configuration**: Configure input/output ports
- **Error Handling**: Set error handling strategies
- **Performance Tuning**: Configure performance parameters

### Rule Compilation
- **Automatic Compilation**: Rules are compiled automatically
- **Caching**: Enable rule caching for better performance
- **Optimization**: Enable rule optimization features
- **Validation**: Enable rule validation before compilation

## Best Practices

### Rule Design
- **Keep Rules Simple**: Prefer simple rules over complex nested ones
- **Use Appropriate Operators**: Choose operators that match data types
- **Validate Early**: Validate rules before deployment
- **Test Thoroughly**: Test rules with various data scenarios

### Performance
- **Use Indexed Columns**: Filter on indexed columns when possible
- **Avoid Complex Nested Rules**: Simplify complex logical expressions
- **Monitor Performance**: Track evaluation times and resource usage
- **Cache Rules**: Reuse compiled rules when possible

### Error Handling
- **Handle Missing Data**: Account for NULL/missing values
- **Type Safety**: Ensure data types match expected values
- **Graceful Degradation**: Handle errors without stopping processing
- **Logging**: Log errors and performance metrics

### Data Quality
- **Data Validation**: Validate input data before filtering
- **Schema Validation**: Use schema validation for data quality
- **Error Recovery**: Implement graceful error recovery mechanisms
- **Monitoring**: Monitor filter performance and accuracy
