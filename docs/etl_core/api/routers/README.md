# API Routers

This directory contains API route definitions and endpoint implementations for the ETL system.

## Overview

API routers provide RESTful endpoints for managing ETL operations, including job management, execution control, context management, and system monitoring.

## Components

### [Contexts Router](./contexts.py)
- **File**: `contexts.py`
- **Purpose**: Context management endpoints
- **Features**: Context CRUD operations, credential management, environment settings

### [Execution Router](./execution.py)
- **File**: `execution.py`
- **Purpose**: Job execution control endpoints
- **Features**: Start/stop execution, execution status, execution monitoring

### [Jobs Router](./jobs.py)
- **File**: `jobs.py`
- **Purpose**: Job management endpoints
- **Features**: Job CRUD operations, job configuration, job monitoring

### [Metrics Router](./metrics.py)
- **File**: `metrics.py`
- **Purpose**: Metrics and monitoring endpoints
- **Features**: Metrics collection, performance monitoring, system health

### [Schemas Router](./schemas.py)
- **File**: `schemas.py`
- **Purpose**: Schema management endpoints
- **Features**: Schema CRUD operations, schema validation, schema documentation

### [Setup Router](./setup.py)
- **File**: `setup.py`
- **Purpose**: System setup and configuration endpoints
- **Features**: System initialization, configuration management, health checks

## Router Architecture

### FastAPI Integration
All routers are integrated with FastAPI, providing:
- **Automatic Documentation**: Automatic API documentation generation
- **Request Validation**: Automatic request validation
- **Response Serialization**: Automatic response serialization
- **Error Handling**: Consistent error handling
- **Type Safety**: Full type safety with Pydantic

### Router Organization
- **Logical Grouping**: Routes grouped by functionality
- **Consistent Patterns**: Consistent route patterns
- **Error Handling**: Consistent error handling
- **Authentication**: Consistent authentication
- **Authorization**: Consistent authorization

## Contexts Router

### Context Management
- **POST /contexts**: Create new context
- **GET /contexts**: List all contexts
- **GET /contexts/{id}**: Get context by ID
- **PUT /contexts/{id}**: Update context
- **DELETE /contexts/{id}**: Delete context

### Credential Management
- **POST /contexts/{id}/credentials**: Add credentials to context
- **GET /contexts/{id}/credentials**: Get context credentials
- **PUT /contexts/{id}/credentials/{cred_id}**: Update credentials
- **DELETE /contexts/{id}/credentials/{cred_id}**: Delete credentials

### Environment Management
- **GET /contexts/{id}/environment**: Get context environment
- **PUT /contexts/{id}/environment**: Update context environment
- **GET /contexts/{id}/parameters**: Get context parameters
- **PUT /contexts/{id}/parameters**: Update context parameters

## Execution Router

### Execution Control
- **POST /jobs/{id}/start**: Start job execution
- **POST /jobs/{id}/stop**: Stop job execution
- **POST /jobs/{id}/pause**: Pause job execution
- **POST /jobs/{id}/resume**: Resume job execution
- **POST /jobs/{id}/restart**: Restart job execution

### Execution Monitoring
- **GET /jobs/{id}/status**: Get execution status
- **GET /jobs/{id}/logs**: Get execution logs
- **GET /jobs/{id}/metrics**: Get execution metrics
- **GET /jobs/{id}/errors**: Get execution errors
- **GET /jobs/{id}/progress**: Get execution progress

### Execution History
- **GET /jobs/{id}/executions**: Get execution history
- **GET /jobs/{id}/executions/{exec_id}**: Get specific execution
- **GET /jobs/{id}/executions/{exec_id}/logs**: Get execution logs
- **GET /jobs/{id}/executions/{exec_id}/metrics**: Get execution metrics

## Jobs Router

### Job Management
- **POST /jobs**: Create new job
- **GET /jobs**: List all jobs
- **GET /jobs/{id}**: Get job by ID
- **PUT /jobs/{id}**: Update job
- **DELETE /jobs/{id}**: Delete job

### Job Configuration
- **GET /jobs/{id}/config**: Get job configuration
- **PUT /jobs/{id}/config**: Update job configuration
- **GET /jobs/{id}/components**: Get job components
- **PUT /jobs/{id}/components**: Update job components
- **GET /jobs/{id}/dependencies**: Get job dependencies

### Job Monitoring
- **GET /jobs/{id}/status**: Get job status
- **GET /jobs/{id}/health**: Get job health
- **GET /jobs/{id}/performance**: Get job performance
- **GET /jobs/{id}/statistics**: Get job statistics
- **GET /jobs/{id}/timeline**: Get job timeline

## Metrics Router

### Metrics Collection
- **GET /metrics/system**: Get system metrics
- **GET /metrics/jobs**: Get job metrics
- **GET /metrics/components**: Get component metrics
- **GET /metrics/executions**: Get execution metrics
- **GET /metrics/performance**: Get performance metrics

### Performance Monitoring
- **GET /metrics/performance/cpu**: Get CPU metrics
- **GET /metrics/performance/memory**: Get memory metrics
- **GET /metrics/performance/disk**: Get disk metrics
- **GET /metrics/performance/network**: Get network metrics
- **GET /metrics/performance/custom**: Get custom metrics

### Health Monitoring
- **GET /health**: Get system health
- **GET /health/jobs**: Get job health
- **GET /health/components**: Get component health
- **GET /health/database**: Get database health
- **GET /health/external**: Get external service health

## Schemas Router

### Schema Management
- **POST /schemas**: Create new schema
- **GET /schemas**: List all schemas
- **GET /schemas/{id}**: Get schema by ID
- **PUT /schemas/{id}**: Update schema
- **DELETE /schemas/{id}**: Delete schema

### Schema Validation
- **POST /schemas/{id}/validate**: Validate data against schema
- **POST /schemas/validate**: Validate data against multiple schemas
- **GET /schemas/{id}/validation-rules**: Get schema validation rules
- **PUT /schemas/{id}/validation-rules**: Update validation rules

### Schema Documentation
- **GET /schemas/{id}/documentation**: Get schema documentation
- **GET /schemas/{id}/json-schema**: Get JSON schema
- **GET /schemas/{id}/openapi**: Get OpenAPI schema
- **GET /schemas/{id}/examples**: Get schema examples

## Setup Router

### System Setup
- **POST /setup/initialize**: Initialize system
- **GET /setup/status**: Get setup status
- **POST /setup/configure**: Configure system
- **GET /setup/configuration**: Get system configuration
- **PUT /setup/configuration**: Update system configuration

### Health Checks
- **GET /health**: Get system health
- **GET /health/ready**: Get readiness status
- **GET /health/live**: Get liveness status
- **GET /health/detailed**: Get detailed health information

### System Information
- **GET /info**: Get system information
- **GET /info/version**: Get system version
- **GET /info/features**: Get system features
- **GET /info/capabilities**: Get system capabilities

## Request/Response Patterns

### Request Validation
- **Pydantic Models**: Use Pydantic models for validation
- **Type Checking**: Automatic type checking
- **Required Fields**: Required field validation
- **Format Validation**: Format validation
- **Custom Validation**: Custom validation rules

### Response Formatting
- **JSON Responses**: Consistent JSON response format
- **Error Responses**: Standardized error response format
- **Success Responses**: Standardized success response format
- **Pagination**: Pagination support
- **Metadata**: Response metadata

### Error Handling
- **HTTP Status Codes**: Appropriate HTTP status codes
- **Error Messages**: Clear error messages
- **Error Details**: Detailed error information
- **Request ID**: Request tracking for debugging
- **Error Logging**: Comprehensive error logging

## Authentication and Authorization

### Authentication
- **API Keys**: API key authentication
- **JWT Tokens**: JWT token authentication
- **OAuth2**: OAuth2 authentication
- **Session Authentication**: Session-based authentication
- **Custom Authentication**: Custom authentication methods

### Authorization
- **Role-Based Access**: Role-based access control
- **Permission-Based Access**: Permission-based access control
- **Resource-Based Access**: Resource-based access control
- **API Key Permissions**: API key permission management
- **Audit Logging**: Access audit logging

## Performance and Monitoring

### Performance Optimization
- **Response Caching**: Response caching
- **Database Optimization**: Database query optimization
- **Connection Pooling**: Connection pooling
- **Async Processing**: Asynchronous processing
- **Load Balancing**: Load balancing support

### Monitoring
- **Request Metrics**: Request performance metrics
- **Response Metrics**: Response performance metrics
- **Error Metrics**: Error rate metrics
- **Resource Metrics**: Resource usage metrics
- **Custom Metrics**: Custom application metrics

## Best Practices

### API Design
- **RESTful Design**: Follow REST principles
- **Consistent Naming**: Use consistent naming conventions
- **Versioning**: Implement API versioning
- **Documentation**: Comprehensive API documentation
- **Error Handling**: Consistent error handling

### Security
- **Input Validation**: Validate all inputs
- **Authentication**: Implement proper authentication
- **Authorization**: Implement proper authorization
- **Rate Limiting**: Implement rate limiting
- **Audit Logging**: Maintain audit logs

### Performance
- **Caching**: Implement appropriate caching
- **Optimization**: Optimize for performance
- **Monitoring**: Monitor API performance
- **Scaling**: Design for scalability
- **Load Testing**: Regular load testing
