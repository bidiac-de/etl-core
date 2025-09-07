# Singletons

Singleton pattern implementation for core system services.

## Overview

The singletons module (`singletons.py`) provides singleton instances of core system services, ensuring single instances across the application and providing centralized access to key services.

## Features

### Singleton Pattern
- **Single Instance**: Ensures only one instance per service
- **Global Access**: Provides global access to services
- **Lazy Initialization**: Services are initialized on first access
- **Thread Safety**: Thread-safe singleton implementation

### Service Management
- **Job Handler**: Singleton job handler instance
- **Execution Handler**: Singleton execution handler instance
- **Service Discovery**: Easy service discovery and access
- **Dependency Injection**: Supports dependency injection patterns

## Services

### Job Handler Singleton
```python
def job_handler() -> JobHandler:
    global _job_handler_singleton
    if _job_handler_singleton is None:
        _job_handler_singleton = JobHandler()
    return _job_handler_singleton
```

**Purpose**: Provides singleton access to the job handler service
**Features**:
- Job management operations
- Job persistence
- Job configuration
- Job lifecycle management

### Execution Handler Singleton
```python
def execution_handler() -> JobExecutionHandler:
    global _execution_handler_singleton
    if _execution_handler_singleton is None:
        _execution_handler_singleton = JobExecutionHandler()
    return _execution_handler_singleton
```

**Purpose**: Provides singleton access to the execution handler service
**Features**:
- Job execution management
- Execution coordination
- Resource management
- Metrics collection

## Implementation Details

### Singleton Pattern Implementation
- **Global Variables**: Use global variables for singleton storage
- **Lazy Initialization**: Initialize services only when first accessed
- **Null Check**: Check for null before initialization
- **Thread Safety**: Consider thread safety in implementation

### Service Initialization
- **On-Demand**: Services are created when first accessed
- **Single Creation**: Each service is created only once
- **Global Access**: Services are accessible globally
- **Dependency Management**: Manage service dependencies

## Usage

### Accessing Services
```python
from etl_core.singletons import job_handler, execution_handler

# Get job handler instance
job_service = job_handler()

# Get execution handler instance
execution_service = execution_handler()
```

### Service Operations
```python
# Job operations
job_service.create_job(job_config)
job_service.get_job(job_id)
job_service.update_job(job_id, updates)

# Execution operations
execution_service.execute_job(job)
execution_service.stop_job(job_id)
execution_service.get_status(job_id)
```

## Benefits

### Resource Management
- **Memory Efficiency**: Single instance reduces memory usage
- **Resource Sharing**: Shared resources across the application
- **Connection Pooling**: Efficient connection management
- **Cache Sharing**: Shared cache instances

### Consistency
- **State Consistency**: Consistent state across the application
- **Configuration Consistency**: Consistent configuration access
- **Data Consistency**: Consistent data access patterns
- **Behavior Consistency**: Consistent service behavior

### Performance
- **Reduced Overhead**: Reduced object creation overhead
- **Faster Access**: Faster service access
- **Optimized Resources**: Optimized resource usage
- **Better Caching**: Better caching strategies

## Thread Safety

### Thread Safety Considerations
- **Global Variables**: Global variables need thread safety
- **Initialization**: Initialization needs to be thread-safe
- **Access Patterns**: Access patterns need to be thread-safe
- **State Management**: State management needs thread safety

### Implementation Strategies
- **Locking**: Use locks for thread safety
- **Atomic Operations**: Use atomic operations where possible
- **Thread-Local Storage**: Use thread-local storage when appropriate
- **Immutable Objects**: Use immutable objects when possible

## Error Handling

### Initialization Errors
- **Service Creation Errors**: Handle service creation failures
- **Configuration Errors**: Handle configuration errors
- **Dependency Errors**: Handle dependency errors
- **Resource Errors**: Handle resource allocation errors

### Runtime Errors
- **Service Access Errors**: Handle service access errors
- **State Errors**: Handle state-related errors
- **Operation Errors**: Handle operation errors
- **Recovery Errors**: Handle recovery errors

## Best Practices

### Singleton Design
- **Use Sparingly**: Use singletons sparingly
- **Clear Purpose**: Have a clear purpose for singletons
- **Thread Safety**: Ensure thread safety
- **Testing**: Make singletons testable

### Service Management
- **Lazy Initialization**: Use lazy initialization
- **Error Handling**: Implement proper error handling
- **Resource Cleanup**: Implement resource cleanup
- **Monitoring**: Monitor singleton usage

### Code Organization
- **Clear Interface**: Provide clear service interfaces
- **Documentation**: Document singleton usage
- **Dependencies**: Manage dependencies clearly
- **Testing**: Test singleton behavior

## Alternatives

### Dependency Injection
- **Service Container**: Use service container instead
- **Factory Pattern**: Use factory pattern
- **Service Locator**: Use service locator pattern
- **Inversion of Control**: Use IoC container

### Stateless Services
- **Stateless Design**: Design stateless services
- **Functional Approach**: Use functional approach
- **Immutable Objects**: Use immutable objects
- **Pure Functions**: Use pure functions

## Monitoring

### Singleton Monitoring
- **Usage Tracking**: Track singleton usage
- **Performance Monitoring**: Monitor singleton performance
- **Memory Usage**: Monitor memory usage
- **Error Tracking**: Track singleton errors

### Service Health
- **Health Checks**: Implement health checks
- **Status Monitoring**: Monitor service status
- **Performance Metrics**: Collect performance metrics
- **Error Rates**: Monitor error rates
