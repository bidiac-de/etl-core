# API and CLI

This directory contains API endpoints, CLI tools, and HTTP utilities for the ETL system.

## Overview

The API and CLI components provide programmatic and command-line interfaces for managing ETL operations, including job management, execution control, and system monitoring.

## Components

### [CLI](./cli.py)
- **File**: `cli.py`
- **Purpose**: Command-line interface for ETL operations
- **Features**: Job management, execution control, monitoring commands

### [Dependencies](./dependencies.py)
- **File**: `dependencies.py`
- **Purpose**: API dependency injection
- **Features**: Dependency management, service injection, configuration

### [Helpers](./helpers.py)
- **File**: `helpers.py`
- **Purpose**: API utility functions
- **Features**: Common utilities, helper functions, shared logic

### [HTTP Errors](./http_errors.py)
- **File**: `http_errors.py`
- **Purpose**: HTTP error handling
- **Features**: Custom error classes, error responses, error formatting

### [Routers](./routers/README.md)
API route definitions and endpoint implementations.

## CLI Interface

### CLI Commands
- **Job Management**: Create, update, delete, list jobs
- **Execution Control**: Start, stop, pause, resume jobs
- **Monitoring**: View job status, logs, metrics
- **Configuration**: Manage system configuration
- **Health Checks**: System health monitoring

### CLI Features
- **Interactive Mode**: Interactive command interface
- **Batch Mode**: Batch command execution
- **Configuration**: CLI configuration management
- **Help System**: Comprehensive help system
- **Error Handling**: User-friendly error messages

### CLI Usage
```bash
# Job management
etl-cli job create --config job.json
etl-cli job list
etl-cli job start --job-id 123

# Execution control
etl-cli execution start --job-id 123
etl-cli execution stop --job-id 123
etl-cli execution status --job-id 123

# Monitoring
etl-cli metrics show --job-id 123
etl-cli logs tail --job-id 123
etl-cli health check
```

## API Endpoints

### Job Management Endpoints
- **POST /jobs**: Create a new job
- **GET /jobs**: List all jobs
- **GET /jobs/{id}**: Get job details
- **PUT /jobs/{id}**: Update job configuration
- **DELETE /jobs/{id}**: Delete a job

### Execution Endpoints
- **POST /jobs/{id}/start**: Start job execution
- **POST /jobs/{id}/stop**: Stop job execution
- **POST /jobs/{id}/pause**: Pause job execution
- **POST /jobs/{id}/resume**: Resume job execution
- **GET /jobs/{id}/status**: Get execution status

### Monitoring Endpoints
- **GET /jobs/{id}/metrics**: Get job metrics
- **GET /jobs/{id}/logs**: Get job logs
- **GET /system/health**: System health check
- **GET /system/metrics**: System metrics
- **GET /system/status**: System status

## HTTP Error Handling

### Error Types
- **Validation Errors**: Request validation errors
- **Not Found Errors**: Resource not found errors
- **Authentication Errors**: Authentication failures
- **Authorization Errors**: Authorization failures
- **Server Errors**: Internal server errors

### Error Responses
- **Standardized Format**: Consistent error response format
- **Error Codes**: HTTP status codes
- **Error Messages**: Descriptive error messages
- **Error Details**: Additional error context
- **Request ID**: Request tracking for debugging

## Dependency Injection

### Service Dependencies
- **Job Handler**: Job management service
- **Execution Handler**: Execution management service
- **Metrics Service**: Metrics collection service
- **Context Service**: Context management service
- **Database Service**: Database access service

### Dependency Features
- **Service Registration**: Service registration and discovery
- **Lifecycle Management**: Service lifecycle management
- **Configuration Injection**: Configuration injection
- **Error Handling**: Dependency error handling
- **Testing Support**: Testing dependency injection

## API Utilities

### Helper Functions
- **Request Validation**: Request data validation
- **Response Formatting**: Response data formatting
- **Error Handling**: Common error handling
- **Logging**: API request/response logging
- **Authentication**: Authentication utilities

### Common Utilities
- **Data Serialization**: Data serialization/deserialization
- **Validation**: Input validation utilities
- **Formatting**: Data formatting utilities
- **Conversion**: Data type conversion
- **Sanitization**: Input sanitization

## Router System

### Router Organization
- **Context Router**: Context management endpoints
- **Execution Router**: Execution control endpoints
- **Jobs Router**: Job management endpoints
- **Metrics Router**: Metrics and monitoring endpoints
- **Schemas Router**: Schema management endpoints
- **Setup Router**: System setup endpoints

### Router Features
- **Route Registration**: Automatic route registration
- **Middleware Support**: Middleware integration
- **Error Handling**: Route-specific error handling
- **Authentication**: Route authentication
- **Rate Limiting**: Rate limiting support

## Configuration

### API Configuration
- **Host and Port**: API server configuration
- **CORS Settings**: Cross-origin resource sharing
- **Authentication**: Authentication configuration
- **Rate Limiting**: Rate limiting configuration
- **Logging**: API logging configuration

### CLI Configuration
- **Command Configuration**: CLI command configuration
- **Output Format**: Output formatting options
- **Verbose Mode**: Verbose output configuration
- **Configuration File**: CLI configuration file
- **Environment Variables**: Environment variable support

## Security

### API Security
- **Authentication**: API authentication
- **Authorization**: API authorization
- **Rate Limiting**: API rate limiting
- **Input Validation**: Input validation and sanitization
- **HTTPS Support**: HTTPS encryption support

### CLI Security
- **Credential Management**: Secure credential handling
- **Access Control**: CLI access control
- **Audit Logging**: CLI operation logging
- **Secure Communication**: Secure API communication
- **Configuration Security**: Secure configuration handling

## Monitoring and Logging

### API Monitoring
- **Request Metrics**: API request metrics
- **Response Metrics**: API response metrics
- **Error Metrics**: API error metrics
- **Performance Metrics**: API performance metrics
- **Usage Metrics**: API usage metrics

### CLI Monitoring
- **Command Usage**: CLI command usage tracking
- **Performance Metrics**: CLI performance metrics
- **Error Tracking**: CLI error tracking
- **User Activity**: User activity monitoring
- **System Health**: System health monitoring

## Best Practices

### API Design
- **RESTful Design**: Follow REST principles
- **Consistent Naming**: Use consistent naming conventions
- **Versioning**: Implement API versioning
- **Documentation**: Comprehensive API documentation
- **Error Handling**: Consistent error handling

### CLI Design
- **User-Friendly**: Design for user experience
- **Consistent Interface**: Consistent command interface
- **Help System**: Comprehensive help system
- **Error Messages**: Clear error messages
- **Configuration**: Flexible configuration options

### Security Practices
- **Input Validation**: Validate all inputs
- **Authentication**: Implement proper authentication
- **Authorization**: Implement proper authorization
- **Audit Logging**: Maintain audit logs
- **Regular Updates**: Keep security components updated
