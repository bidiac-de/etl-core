# MariaDB ETL Component Tests

This directory contains comprehensive tests for the MariaDB ETL components.

## Test Structure

```
tests/components/databases/
├── README.md                           # This file
├── test_mariadb_components.py          # Unit tests for MariaDB components
├── test_mariadb_receivers.py           # Unit tests for MariaDB receivers
├── test_mariadb_integration.py         # Integration tests
└── test_credentials_integration.py     # Credential system integration tests
```

## Architecture Changes (v2.0)

The MariaDB components have been updated to use the new connection architecture:

- **Old**: `ConnectionHandler` with direct connection access
- **New**: `SQLConnectionHandler` with connection pooling and lease-based access
- **Pool Management**: Centralized connection pool registry
- **Credentials**: Enhanced credential management with pool settings

### Key Changes in Tests

1. **Connection Handler**: Tests now mock `SQLConnectionHandler` instead of `ConnectionHandler`
2. **Connection Access**: Uses `lease()` context manager instead of direct `.connection` access
3. **Pool Integration**: Tests include pool parameter handling
4. **Credentials**: Enhanced credential mocking with `decrypted_password` support

## Testing Strategy

### Mixed Approach: Mocks vs Real Objects

We use a **mixed testing strategy** to balance performance and thoroughness:

#### **Unit Tests (Mock-based)**
- **Purpose**: Fast, isolated testing of component logic
- **Files**: `test_mariadb_components.py`, `test_mariadb_receivers.py`
- **Benefits**: Quick execution, controlled environment, isolated functionality

#### **Integration Tests (Mock-based)**
- **Purpose**: Test component interactions without external dependencies
- **Files**: `test_mariadb_integration.py`
- **Benefits**: Fast integration testing, controlled data flow

#### **Credential System Tests (Real Objects)**
- **Purpose**: Thorough testing of the credential and context system
- **Files**: `test_credentials_integration.py`
- **Benefits**: Full validation, real object behavior, early error detection

### Why This Approach?

- **Performance**: Unit tests remain fast for daily development
- **Coverage**: Integration tests cover component interactions
- **Validation**: Credential tests ensure real object functionality
- **Maintainability**: Clear separation of concerns

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

# Test only credentials system
pytest test_credentials_integration.py -v
```

### Run Specific Test Classes

```bash
# Test only component tests
pytest test_mariadb_components.py::TestMariaDBComponents -v

# Test only receiver tests
pytest test_mariadb_receivers.py::TestMariaDBReceivers -v

# Test only integration tests
pytest test_mariadb_integration.py::TestMariaDBIntegration -v

# Test only credential integration tests
pytest test_credentials_integration.py::TestCredentialsIntegration -v
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
- **Connection Setup**: Database connection handling with new SQLConnectionHandler
- **Error Handling**: Exception propagation
- **Strategy Integration**: Strategy execution flow

### Receiver Tests (`test_mariadb_receivers.py`)

- **Initialization**: Receiver creation and setup
- **Read Operations**: `read_row`, `read_bulk`, `read_bigdata`
- **Write Operations**: `write_row`, `write_bulk`, `write_bigdata`
- **SQL Generation**: Query building and execution
- **Error Handling**: Database error scenarios
- **Connection Leasing**: Proper use of lease() context manager

### Integration Tests (`test_mariadb_integration.py`)

- **Read-to-Write Pipeline**: Complete ETL flow
- **Strategy Streaming**: Row-by-row processing
- **Big Data Flow**: Dask DataFrame handling
- **Error Propagation**: End-to-end error handling
- **Metrics Integration**: Performance monitoring

### Credential System Tests (`test_credentials_integration.py`)

- **Credentials Creation**: Real Credentials object validation
- **Context Management**: Real Context object functionality
- **Parameter Handling**: ContextParameter validation
- **Pool Integration**: Connection pool parameter testing
- **Component Integration**: MariaDB components with real credentials
- **Password Handling**: Secure credential management

## Mocking Strategy

### Unit and Integration Tests (Mock-based)

All unit and integration tests use **mocking** to avoid requiring actual MariaDB instances:

#### What's Mocked

- **Database Connections**: SQLAlchemy connections
- **SQL Execution**: Query results and transactions
- **Network Calls**: Database server communication
- **File I/O**: Any file operations
- **Credentials**: Mock credential objects with controlled behavior

#### What's Tested

- **Component Logic**: Business logic and data flow
- **Receiver Operations**: Database operation handling
- **Strategy Integration**: Execution strategy flow
- **Error Handling**: Exception scenarios
- **Data Transformation**: DataFrame and Dask operations

### Credential System Tests (Real Objects)

These tests use **real objects** to validate the complete credential system:

#### What's Real

- **Credentials**: Real `Credentials` Pydantic models
- **Context**: Real `Context` objects with proper validation
- **ContextParameter**: Real `ContextParameter` objects
- **Environment**: Real `Environment` enum values

#### What's Tested

- **Object Validation**: Pydantic model validation
- **Method Functionality**: Real method behavior
- **Error Handling**: Proper exception handling
- **Integration**: Real object interactions
- **Pool Parameters**: Connection pool configuration

## Test Data

### Mock-based Tests

Tests use realistic sample data with mocked dependencies:

```python
sample_data = [
    {"id": 1, "name": "John Doe", "email": "john@example.com"},
    {"id": 2, "name": "Jane Smith", "email": "jane@example.com"},
    {"id": 3, "name": "Bob Johnson", "email": "bob@example.com"}
]
```

### Real Object Tests

Tests create real objects with proper configuration:

```python
# Real Credentials object
credentials = Credentials(
    credentials_id=1,
    name="test_db_creds",
    user="testuser",
    database="testdb",
    password="testpass123",
    pool_max_size=10,
    pool_timeout_s=30
)

# Real Context object
context = Context(
    id=1,
    name="test_context",
    environment=Environment.TEST,
    parameters={...}
)
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

### Credential System Tests

1. Add test method to `TestCredentialsIntegration`
2. Use real object fixtures
3. Test credential validation and management
4. Verify context integration

## Best Practices

### For Mock-based Tests

- **Use Fixtures**: Reuse common test setup
- **Mock Dependencies**: Don't test external systems
- **Test Edge Cases**: Include error scenarios
- **Async Testing**: Use proper async test decorators
- **Clear Assertions**: Make test failures easy to debug
- **Realistic Data**: Use data that represents real usage

### For Real Object Tests

- **Proper Configuration**: Ensure all required fields are set
- **Validation Testing**: Test Pydantic model validation
- **Error Scenarios**: Test invalid configurations
- **Integration Testing**: Test object interactions
- **Performance Awareness**: Real objects may be slower

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure PYTHONPATH includes project root
2. **Async Issues**: Use `@pytest.mark.asyncio` decorator
3. **Mock Problems**: Verify mock setup and assertions
4. **Database Errors**: All database calls should be mocked
5. **Validation Errors**: Check Pydantic model requirements for real objects

### Getting Help

- Check test output for specific error messages
- Verify all dependencies are installed
- Ensure tests run from correct directory
- Check that mocks are properly configured
- For real object tests, verify all required fields are set

## Test Results Summary

### Current Status

- **Unit Tests**: ✅ All passing (fast execution)
- **Integration Tests**: ✅ All passing (controlled environment)
- **Credential Tests**: ✅ All passing (real object validation)

### Performance

- **Mock-based Tests**: Fast execution (< 5 seconds for all)
- **Real Object Tests**: Slightly slower but thorough validation
- **Overall Suite**: Optimized for daily development workflow
