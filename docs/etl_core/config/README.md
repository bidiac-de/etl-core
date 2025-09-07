# Configuration

This directory contains configuration files and settings for the ETL Core system.

## Overview

The configuration directory provides centralized configuration management for the ETL system, including API settings, logging configuration, and environment-specific configurations.

## Configuration Files

### [API Configuration](./api_config.yaml)
- **File**: `api_config.yaml`
- **Purpose**: API server configuration
- **Features**: Server settings, CORS configuration, authentication settings

### [Logging Configuration](./logging_config.yaml)
- **File**: `logging_config.yaml`
- **Purpose**: Logging system configuration
- **Features**: Log levels, handlers, formatters, rotation settings

## Configuration Management

### Configuration Loading
- **YAML Support**: YAML configuration file support
- **Environment Variables**: Environment variable override support
- **Default Values**: Sensible default configurations
- **Validation**: Configuration validation and error handling

### Configuration Hierarchy
1. **Default Configuration**: Built-in default values
2. **File Configuration**: Configuration file values
3. **Environment Variables**: Environment variable overrides
4. **Runtime Configuration**: Runtime configuration updates

## API Configuration

### Server Settings
- **Host**: Server host configuration
- **Port**: Server port configuration
- **Debug Mode**: Debug mode settings
- **Reload**: Auto-reload settings for development

### CORS Configuration
- **Allowed Origins**: Allowed CORS origins
- **Allowed Methods**: Allowed HTTP methods
- **Allowed Headers**: Allowed request headers
- **Credentials**: Credential handling settings

### Authentication
- **Authentication Methods**: Supported authentication methods
- **Token Settings**: Token configuration
- **Session Settings**: Session configuration
- **Security Settings**: Security-related settings

## Logging Configuration

### Log Levels
- **Root Level**: Root logger level
- **Component Levels**: Component-specific log levels
- **Module Levels**: Module-specific log levels
- **Custom Levels**: Custom log level definitions

### Log Handlers
- **Console Handler**: Console output configuration
- **File Handler**: File output configuration
- **Rotating Handler**: Log rotation configuration
- **Network Handler**: Network logging configuration

### Log Formatters
- **Standard Format**: Standard log message format
- **Detailed Format**: Detailed log message format
- **JSON Format**: JSON log message format
- **Custom Format**: Custom log message format

## Environment Configuration

### Environment-Specific Settings
- **Development**: Development environment settings
- **Testing**: Testing environment settings
- **Staging**: Staging environment settings
- **Production**: Production environment settings

### Environment Variables
- **ETL_COMPONENT_MODE**: Component registry mode
- **ETL_LOG_LEVEL**: Logging level
- **ETL_DB_URL**: Database connection URL
- **ETL_API_HOST**: API host configuration
- **ETL_API_PORT**: API port configuration

## Configuration Validation

### Schema Validation
- **YAML Schema**: YAML configuration schema validation
- **Type Validation**: Configuration type validation
- **Range Validation**: Configuration value range validation
- **Required Fields**: Required configuration field validation

### Error Handling
- **Validation Errors**: Configuration validation error handling
- **Missing Files**: Missing configuration file handling
- **Invalid Values**: Invalid configuration value handling
- **Format Errors**: Configuration format error handling

## Configuration Examples

### API Configuration Example
```yaml
api:
  host: "0.0.0.0"
  port: 8000
  debug: false
  reload: false
  cors:
    allowed_origins: ["*"]
    allowed_methods: ["GET", "POST", "PUT", "DELETE"]
    allowed_headers: ["*"]
    allow_credentials: true
```

### Logging Configuration Example
```yaml
logging:
  version: 1
  disable_existing_loggers: false
  formatters:
    standard:
      format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    detailed:
      format: "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
  handlers:
    console:
      class: logging.StreamHandler
      level: INFO
      formatter: standard
      stream: ext://sys.stdout
    file:
      class: logging.handlers.RotatingFileHandler
      level: DEBUG
      formatter: detailed
      filename: logs/etl_core.log
      maxBytes: 10485760
      backupCount: 5
  loggers:
    etl_core:
      level: DEBUG
      handlers: [console, file]
      propagate: false
```

## Best Practices

### Configuration Design
- **Centralized Configuration**: Centralize configuration management
- **Environment Separation**: Separate configurations by environment
- **Default Values**: Provide sensible default values
- **Validation**: Implement configuration validation

### Security Considerations
- **Sensitive Data**: Handle sensitive configuration data securely
- **Access Control**: Control access to configuration files
- **Encryption**: Encrypt sensitive configuration values
- **Audit Logging**: Log configuration changes

### Maintenance
- **Version Control**: Version control configuration files
- **Documentation**: Document configuration options
- **Testing**: Test configuration changes
- **Monitoring**: Monitor configuration usage
