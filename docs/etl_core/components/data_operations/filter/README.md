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

## Component Architecture

### FilterComponent Structure

```python
@register_component("filter")
class FilterComponent(Component):
    """
    Two-port filter:
    - input:  'in'   (required)
    - output: 'pass' (required) -> rows that meet the rule
              'fail' (optional) -> rows that do not meet the rule
    """
    
    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (
        OutPortSpec(name="pass", required=True, fanout="many"),
        OutPortSpec(name="fail", required=False, fanout="many"),
    )
    
    rule: ComparisonRule = Field(..., description="Filter rule expression.")
```

### ComparisonRule Structure

```python
class ComparisonRule(BaseModel):
    """
    Defines filter rules with support for:
    - Simple column comparisons
    - Complex logical expressions
    - Nested rule structures
    """
    
    # For leaf rules
    column: Optional[str] = None
    operator: Optional[str] = None
    value: Optional[Any] = None
    
    # For logical rules
    logical_operator: Optional[str] = None  # "AND", "OR", "NOT"
    rules: Optional[List["ComparisonRule"]] = None
```

## Processing Strategies

### Row Strategy
Processes individual rows one at a time:

```python
async def process_row(self, row: Dict[str, Any], metrics: FilterMetrics) -> AsyncIterator[Out]:
    """
    Route rows to 'pass' if they satisfy the rule, otherwise to 'fail'.
    """
    async for port, payload in self._receiver.process_row(
        row=row, rule=self.rule, metrics=metrics
    ):
        yield Out(port=port, payload=payload)
```

**Characteristics:**
- Immediate processing
- Low memory usage
- Real-time filtering
- Suitable for streaming data

### Bulk Strategy
Processes pandas DataFrames in chunks:

```python
async def process_bulk(self, dataframe: pd.DataFrame, metrics: FilterMetrics) -> AsyncIterator[Out]:
    """
    Route DataFrame partitions to 'pass'/'fail'. Empty frames are skipped.
    """
    async for port, payload in self._receiver.process_bulk(
        dataframe=dataframe, rule=self.rule, metrics=metrics
    ):
        yield Out(port=port, payload=payload)
```

**Characteristics:**
- Batch processing
- Higher memory usage
- Better performance for medium datasets
- Suitable for ETL pipelines

### BigData Strategy
Processes Dask DataFrames for large-scale data:

```python
async def process_bigdata(self, ddf: dd.DataFrame, metrics: FilterMetrics) -> AsyncIterator[Out]:
    """
    Route Dask DataFrames to 'pass'/'fail' without materializing them.
    """
    async for port, payload in self._receiver.process_bigdata(
        ddf=ddf, rule=self.rule, metrics=metrics
    ):
        yield Out(port=port, payload=payload)
```

**Characteristics:**
- Distributed processing
- Lazy evaluation
- Scalable to large datasets
- Suitable for big data analytics

## Rule Evaluation Engine

### Rule Parser
The system includes a sophisticated rule parser that:

1. **Validates Rule Structure**: Ensures rules are properly formatted
2. **Type Checking**: Validates data types for comparisons
3. **Operator Validation**: Ensures operators are supported for data types
4. **Nested Rule Processing**: Handles complex nested logical expressions

### Evaluation Logic

```python
def evaluate_rule(self, data: Any, rule: ComparisonRule) -> bool:
    """
    Recursively evaluate filter rules against data.
    """
    if rule.logical_operator:
        return self._evaluate_logical_rule(data, rule)
    else:
        return self._evaluate_leaf_rule(data, rule)

def _evaluate_logical_rule(self, data: Any, rule: ComparisonRule) -> bool:
    """Evaluate logical operators (AND, OR, NOT)."""
    if rule.logical_operator == "AND":
        return all(self.evaluate_rule(data, sub_rule) for sub_rule in rule.rules)
    elif rule.logical_operator == "OR":
        return any(self.evaluate_rule(data, sub_rule) for sub_rule in rule.rules)
    elif rule.logical_operator == "NOT":
        return not self.evaluate_rule(data, rule.rules[0])

def _evaluate_leaf_rule(self, data: Any, rule: ComparisonRule) -> bool:
    """Evaluate simple column comparisons."""
    value = data.get(rule.column)
    return self._compare_values(value, rule.operator, rule.value)
```

## Advanced Features

### Data Type Support

**Numeric Comparisons:**
```python
# Integer comparisons
{"column": "age", "operator": ">=", "value": 18}

# Float comparisons
{"column": "salary", "operator": ">", "value": 50000.0}

# Null checks
{"column": "bonus", "operator": "!=", "value": None}
```

**String Comparisons:**
```python
# Exact match
{"column": "status", "operator": "==", "value": "active"}

# Contains check
{"column": "description", "operator": "contains", "value": "urgent"}

# Case-insensitive matching
{"column": "name", "operator": "==", "value": "john", "case_sensitive": False}
```

**Date/Time Comparisons:**
```python
# Date comparisons
{"column": "created_date", "operator": ">=", "value": "2024-01-01"}

# DateTime comparisons
{"column": "last_login", "operator": "<", "value": "2024-01-01T12:00:00Z"}
```

### Custom Operators

The system supports custom operators for specific data types:

```python
# Array contains
{"column": "tags", "operator": "array_contains", "value": "urgent"}

# Regex matching
{"column": "email", "operator": "regex", "value": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}

# Range checking
{"column": "score", "operator": "between", "value": [80, 100]}
```

## Performance Optimization

### Rule Compilation
Rules are compiled into efficient evaluation functions:

```python
def compile_rule(self, rule: ComparisonRule) -> Callable:
    """
    Compile rule into optimized evaluation function.
    """
    if rule.logical_operator:
        return self._compile_logical_rule(rule)
    else:
        return self._compile_leaf_rule(rule)
```

### Caching
Frequently used rules are cached for better performance:

```python
@lru_cache(maxsize=128)
def get_compiled_rule(self, rule_hash: str) -> Callable:
    """Cache compiled rules for reuse."""
    return self.compile_rule(rule)
```

### Batch Processing
For bulk and bigdata strategies, rules are vectorized:

```python
def vectorized_evaluation(self, df: pd.DataFrame, rule: ComparisonRule) -> pd.Series:
    """
    Vectorized rule evaluation for pandas DataFrames.
    """
    if rule.logical_operator:
        return self._vectorized_logical_evaluation(df, rule)
    else:
        return self._vectorized_leaf_evaluation(df, rule)
```

## Error Handling

### Rule Validation
Comprehensive rule validation ensures data integrity:

```python
def validate_rule(self, rule: ComparisonRule) -> None:
    """
    Validate rule structure and data types.
    """
    if rule.logical_operator:
        self._validate_logical_rule(rule)
    else:
        self._validate_leaf_rule(rule)

def _validate_leaf_rule(self, rule: ComparisonRule) -> None:
    """Validate leaf rule structure."""
    if not rule.column:
        raise ValueError("Leaf rule must specify a column")
    if not rule.operator:
        raise ValueError("Leaf rule must specify an operator")
    if rule.value is None and rule.operator not in ["is_null", "is_not_null"]:
        raise ValueError("Leaf rule must specify a value")
```

### Data Type Errors
Graceful handling of data type mismatches:

```python
def _compare_values(self, actual: Any, operator: str, expected: Any) -> bool:
    """
    Compare values with proper type handling.
    """
    try:
        return self._safe_compare(actual, operator, expected)
    except TypeError as e:
        logger.warning(f"Type mismatch in comparison: {e}")
        return False
    except Exception as e:
        logger.error(f"Error in value comparison: {e}")
        return False
```

## Configuration Examples

### Simple Age Filter
```python
age_filter = FilterComponent(
    name="adult_filter",
    rule=ComparisonRule(
        column="age",
        operator=">=",
        value=18
    )
)
```

### Complex Department Filter
```python
dept_filter = FilterComponent(
    name="tech_departments",
    rule=ComparisonRule(
        logical_operator="OR",
        rules=[
            ComparisonRule(column="department", operator="==", value="IT"),
            ComparisonRule(column="department", operator="==", value="Engineering"),
            ComparisonRule(column="department", operator="==", value="Data Science")
        ]
    )
)
```

### Advanced Multi-Criteria Filter
```python
advanced_filter = FilterComponent(
    name="senior_employees",
    rule=ComparisonRule(
        logical_operator="AND",
        rules=[
            ComparisonRule(column="age", operator=">=", value=30),
            ComparisonRule(column="experience_years", operator=">=", value=5),
            ComparisonRule(
                logical_operator="OR",
                rules=[
                    ComparisonRule(column="department", operator="==", value="Management"),
                    ComparisonRule(column="level", operator=">=", value="Senior")
                ]
            ),
            ComparisonRule(
                logical_operator="NOT",
                rules=[
                    ComparisonRule(column="status", operator="==", value="inactive")
                ]
            )
        ]
    )
)
```

## Monitoring and Metrics

### Filter Metrics
Comprehensive metrics collection for monitoring:

```python
class FilterMetrics(ComponentMetrics):
    """Metrics specific to filter operations."""
    
    def __init__(self):
        super().__init__()
        self.rows_processed = 0
        self.rows_passed = 0
        self.rows_failed = 0
        self.evaluation_time = 0.0
        self.rule_complexity = 0
```

### Performance Monitoring
- **Throughput**: Rows processed per second
- **Efficiency**: Pass/fail ratio
- **Latency**: Average evaluation time
- **Resource Usage**: Memory and CPU utilization
- **Error Rates**: Rule evaluation errors

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

### Maintenance
- **Document Rules**: Document complex filter rules
- **Version Control**: Track rule changes over time
- **Testing**: Implement comprehensive rule testing
- **Monitoring**: Monitor rule performance and accuracy
