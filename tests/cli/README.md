# CLI Tests

This directory contains comprehensive tests for the ETL Core CLI module (`etl_core.api.cli`).

## Test Structure

### `test_cli.py` - Main CLI Tests
- **TestLocalJobsClient**: Tests for local job management operations
- **TestLocalExecutionClient**: Tests for local execution operations
- **TestHttpJobsClient**: Tests for HTTP-based job management operations
- **TestHttpExecutionClient**: Tests for HTTP-based execution operations
- **TestClientSelection**: Tests for client selection logic
- **TestCLICommands**: Tests for actual CLI command execution

### `test_cli_error_handling.py` - Error Handling Tests
- File not found scenarios
- Invalid JSON handling
- HTTP connection errors
- Missing required arguments
- Invalid base URL formats
- Job config validation errors
- Unicode handling

### `test_cli_protocols.py` - Protocol and Integration Tests
- Protocol compliance verification
- Type hint validation
- Client integration testing
- State independence verification
- Error propagation testing
- Client configuration testing

### `conftest.py` - Test Fixtures
- Mock job and execution handlers
- Temporary config file management
- Mock HTTP requests
- Singleton mocking

## Test Coverage

The tests achieve **100% code coverage** for the CLI module, covering:

- All client implementations (Local and HTTP)
- All CLI commands
- Error handling scenarios
- Protocol implementations
- Client selection logic
- File I/O operations
- JSON parsing and validation

## Key Features Tested

### Local Clients
- Direct interaction with job and execution handlers
- Singleton pattern usage
- Job configuration validation

### HTTP Clients
- REST API communication
- Error response handling
- Base URL configuration
- Request/response mocking

### CLI Commands
- All command implementations
- Local vs remote execution
- Parameter validation
- Error output handling

### Error Scenarios
- File system errors
- Network errors
- Validation errors
- Missing dependencies

## Running the Tests

```bash
# Run all CLI tests
python -m pytest tests/cli/

# Run with coverage
python -m pytest tests/cli/ --cov=etl_core.api.cli --cov-report=term-missing

# Run specific test file
python -m pytest tests/cli/test_cli.py -v

# Run specific test class
python -m pytest tests/cli/test_cli.py::TestLocalJobsClient -v
```

## Test Dependencies

- `pytest` - Test framework
- `pytest-cov` - Coverage reporting
- `typer.testing.CliRunner` - CLI testing utilities
- `unittest.mock` - Mocking and patching
- `tempfile` - Temporary file management

## Code Duplication Prevention

To avoid code duplication, common test utilities are placed in:
- `tests/helpers.py` - General test helpers
- `tests/cli/conftest.py` - CLI-specific fixtures
- Shared mock objects and test data

## Best Practices

1. **Mock External Dependencies**: All external services are mocked
2. **Temporary File Management**: Proper cleanup using helper functions
3. **Error Scenario Coverage**: Comprehensive error handling testing
4. **Protocol Compliance**: Verification of interface implementations
5. **State Independence**: Ensuring client instances don't interfere
