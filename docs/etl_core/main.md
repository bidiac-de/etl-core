# Main Application

The main application entry point for the ETL Core system.

## Overview

The main application (`main.py`) serves as the entry point for the ETL Core system, providing FastAPI application setup, component registration, and system initialization.

## Features

### FastAPI Application
- **FastAPI Framework**: Modern, fast web framework for building APIs
- **Async Support**: Full async/await support
- **Automatic Documentation**: Automatic API documentation generation
- **Type Safety**: Full type safety with Pydantic models

### Application Lifecycle
- **Startup**: System initialization and component registration
- **Runtime**: Request handling and processing
- **Shutdown**: Cleanup and resource management

### Component Registration
- **Auto-Discovery**: Automatic component discovery
- **Registry Mode**: Configurable registry modes
- **Component Loading**: Dynamic component loading
- **Mode Resolution**: Environment-based mode resolution

## Configuration

### Environment Configuration
- **Environment Variables**: Configuration via environment variables
- **Configuration Files**: Support for configuration files
- **Default Values**: Sensible default configurations
- **Mode Selection**: Registry mode selection

### Registry Modes
- **Development**: Development mode with additional features
- **Production**: Production mode with optimized settings
- **Testing**: Testing mode with test-specific configurations

## Application Setup

### FastAPI Configuration
```python
app = FastAPI(lifespan=lifespan)
app.include_router(schemas.router)
app.include_router(setup.router)
app.include_router(jobs.router)
app.include_router(execution.router)
app.include_router(contexts.router)
```

### Router Integration
- **Schemas Router**: Schema management endpoints
- **Setup Router**: System setup endpoints
- **Jobs Router**: Job management endpoints
- **Execution Router**: Execution control endpoints
- **Contexts Router**: Context management endpoints

## Lifecycle Management

### Startup Process
1. **Logging Setup**: Initialize logging system
2. **Registry Mode**: Set registry mode based on configuration
3. **Component Discovery**: Auto-discover and register components
4. **System Initialization**: Initialize system components

### Shutdown Process
1. **Resource Cleanup**: Clean up system resources
2. **Connection Cleanup**: Close database connections
3. **Logging Cleanup**: Finalize logging operations
4. **Graceful Shutdown**: Ensure graceful shutdown

## Development Server

### Local Development
```python
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("etl_core.main:app", host="127.0.0.1", port=8000, reload=True)
```

### Server Configuration
- **Host**: Localhost (127.0.0.1)
- **Port**: 8000
- **Reload**: Automatic reload on code changes
- **Debug**: Debug mode for development

## Component Integration

### Component Registry
- **Mode Resolution**: Environment-based mode selection
- **Component Loading**: Dynamic component loading
- **Registration**: Automatic component registration
- **Discovery**: Component discovery mechanism

### Auto-Discovery
- **Package Scanning**: Scan packages for components
- **Class Detection**: Detect component classes
- **Registration**: Register discovered components
- **Validation**: Validate component registration

## Error Handling

### Application Errors
- **Startup Errors**: Handle startup failures
- **Configuration Errors**: Handle configuration issues
- **Component Errors**: Handle component registration errors
- **Runtime Errors**: Handle runtime errors

### Error Recovery
- **Graceful Degradation**: Degrade gracefully on errors
- **Error Logging**: Log errors appropriately
- **Error Reporting**: Report errors to monitoring systems
- **Recovery Procedures**: Implement recovery procedures

## Monitoring and Logging

### Application Monitoring
- **Health Checks**: Application health monitoring
- **Performance Metrics**: Performance monitoring
- **Error Tracking**: Error tracking and reporting
- **Resource Monitoring**: Resource usage monitoring

### Logging Integration
- **Structured Logging**: Structured log output
- **Log Levels**: Configurable log levels
- **Log Rotation**: Log file rotation
- **Centralized Logging**: Centralized logging system

## Best Practices

### Application Design
- **Modular Design**: Design for modularity
- **Error Handling**: Implement comprehensive error handling
- **Configuration Management**: Manage configuration properly
- **Resource Management**: Manage resources efficiently

### Development Practices
- **Type Safety**: Use type hints throughout
- **Documentation**: Document code clearly
- **Testing**: Implement comprehensive testing
- **Code Quality**: Maintain high code quality

### Deployment Practices
- **Environment Configuration**: Configure for different environments
- **Security**: Implement security best practices
- **Monitoring**: Implement comprehensive monitoring
- **Scaling**: Design for scalability
