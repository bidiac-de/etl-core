# MariaDB ETL Component Tests

This directory contains comprehensive tests for the MariaDB ETL components.

## Test Structure

```
tests/components/databases/
├── README.md                           # This file
├── test_mariadb_components.py          # Unit tests for MariaDB components
├── test_mariadb_receivers.py           # Unit tests for MariaDB receivers
└── test_mariadb_integration.py         # Integration tests
```

## Running the Tests

### Prerequisites

Make sure you have the required testing dependencies:

```bash
pip install pytest pytest-asyncio
```

### Run All Tests

```bash
# From the project root
pytest tests/components/databases/ -v

# Or from this directory
cd tests/components/databases/
pytest -v
```

### Run Specific Test Files

```bash
# Test only components
pytest test_mariadb_components.py -v

# Test only receivers
pytest test_mariadb_receivers.py -v

# Test only integration
pytest test_mariadb_integration.py -v
```

### Run Specific Test Classes

```bash
# Test only component tests
pytest test_mariadb_components.py::TestMariaDBComponents -v

# Test only receiver tests
pytest test_mariadb_receivers.py::TestMariaDBReceivers -v

# Test only integration tests
pytest test_mariadb_integration.py::TestMariaDBIntegration -v
```

### Run Specific Test Methods

```bash
# Test specific method
pytest test_mariadb_components.py::TestMariaDBComponents::test_mariadb_read_initialization -v

# Test with pattern matching
pytest -k "initialization" -v
```

## Test Coverage

### Component Tests (`test_mariadb_components.py`)

- **Initialization**: Component creation and configuration
- **Process Methods**: `process_row`, `process_bulk`, `process_bigdata`
- **Connection Setup**: Database connection handling
- **Error Handling**: Exception propagation
- **Strategy Integration**: Strategy execution flow

### Receiver Tests (`test_mariadb_receivers.py`)

- **Initialization**: Receiver creation and setup
- **Read Operations**: `read_row`, `read_bulk`, `read_bigdata`
- **Write Operations**: `write_row`, `write_bulk`, `write_bigdata`
- **SQL Generation**: Query building and execution
- **Error Handling**: Database error scenarios

### Integration Tests (`test_mariadb_integration.py`)

- **Read-to-Write Pipeline**: Complete ETL flow
- **Strategy Streaming**: Row-by-row processing
- **Big Data Flow**: Dask DataFrame handling
- **Error Propagation**: End-to-end error handling
- **Metrics Integration**: Performance monitoring

## Mocking Strategy

All tests use **mocking** to avoid requiring actual MariaDB instances:

### What's Mocked

- **Database Connections**: SQLAlchemy connections
- **SQL Execution**: Query results and transactions
- **Network Calls**: Database server communication
- **File I/O**: Any file operations

### What's Tested

- **Component Logic**: Business logic and data flow
- **Receiver Operations**: Database operation handling
- **Strategy Integration**: Execution strategy flow
- **Error Handling**: Exception scenarios
- **Data Transformation**: DataFrame and Dask operations

## Test Data

Tests use realistic sample data:

```python
sample_data = [
    {"id": 1, "name": "John Doe", "email": "john@example.com"},
    {"id": 2, "name": "Jane Smith", "email": "jane@example.com"},
    {"id": 3, "name": "Bob Johnson", "email": "bob@example.com"}
]
```

## Async Testing

All tests use `pytest-asyncio` for testing async components:

```python
@pytest.mark.asyncio
async def test_async_method():
    result = await component.async_method()
    assert result is not None
```

## Debugging Tests

### Verbose Output

```bash
pytest -v -s --tb=long
```

### Stop on First Failure

```bash
pytest -x
```

### Run Only Failed Tests

```bash
pytest --lf
```

### Debug Specific Test

```bash
pytest -k "test_name" --pdb
```

## Adding New Tests

### Component Tests

1. Add test method to `TestMariaDBComponents`
2. Use existing fixtures for common setup
3. Mock dependencies appropriately
4. Test both success and failure scenarios

### Receiver Tests

1. Add test method to `TestMariaDBReceivers`
2. Mock SQL execution results
3. Verify SQL query generation
4. Test error conditions

### Integration Tests

1. Add test method to `TestMariaDBIntegration`
2. Test complete component interactions
3. Verify data flow through the pipeline
4. Test end-to-end scenarios

## Best Practices

- **Use Fixtures**: Reuse common test setup
- **Mock Dependencies**: Don't test external systems
- **Test Edge Cases**: Include error scenarios
- **Async Testing**: Use proper async test decorators
- **Clear Assertions**: Make test failures easy to debug
- **Realistic Data**: Use data that represents real usage

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure PYTHONPATH includes project root
2. **Async Issues**: Use `@pytest.mark.asyncio` decorator
3. **Mock Problems**: Verify mock setup and assertions
4. **Database Errors**: All database calls should be mocked

### Getting Help

- Check test output for specific error messages
- Verify all dependencies are installed
- Ensure tests run from correct directory
- Check that mocks are properly configured
