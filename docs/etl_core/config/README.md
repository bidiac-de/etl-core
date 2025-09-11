# Configuration

Configuration components provide **centralized configuration management** and **environment-specific settings** for ETL pipeline data with comprehensive support for API settings and logging configuration.

## Overview

Configuration components define the structure, settings, and environment-specific configurations for data flowing through the ETL pipeline, ensuring proper system configuration and type safety. They provide **comprehensive configuration support** and **validation capabilities** for robust system management.

### Key Concepts
- **Configuration Management**: Centralized configuration management and validation
- **Environment Settings**: Environment-specific configuration handling
- **API Configuration**: API server and authentication settings
- **Logging Configuration**: Comprehensive logging system configuration

## Components

### [API Configuration](./api_config.yaml)
- **File**: `api_config.yaml`
- **Purpose**: API server configuration
- **Features**: Server settings, CORS configuration, authentication settings

### [Logging Configuration](./logging_config.yaml)
- **File**: `logging_config.yaml`
- **Purpose**: Logging system configuration
- **Features**: Log levels, handlers, formatters, rotation settings

## Configuration Types

### API Configuration
- **Server Settings**: Host, port, debug mode, reload settings
- **CORS Configuration**: Allowed origins, methods, headers, credentials
- **Authentication**: Authentication methods, token settings, security settings
- **Security**: Security-related configuration and policies

### Logging Configuration
- **Log Levels**: Root, component, module, and custom log levels
- **Log Handlers**: Console, file, rotating, and network handlers
- **Log Formatters**: Standard, detailed, JSON, and custom formats
- **Log Rotation**: Size-based, time-based, and hybrid rotation strategies

### Features
- **YAML Support**: YAML configuration file support
- **Environment Variables**: Environment variable override support
- **Validation**: Configuration validation and error handling
- **Hierarchy**: Configuration hierarchy with overrides

## JSON Configuration Examples

### API Configuration
```json
{
  "api": {
    "host": "0.0.0.0",
    "port": 8000,
    "debug": false,
    "reload": false,
    "cors": {
      "allowed_origins": ["*"],
      "allowed_methods": ["GET", "POST", "PUT", "DELETE"],
      "allowed_headers": ["*"],
      "allow_credentials": true
    }
  }
}
```

### Logging Configuration
```json
{
  "logging": {
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {
      "standard": {
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
      }
    },
    "handlers": {
      "console": {
        "class": "logging.StreamHandler",
        "level": "INFO",
        "formatter": "standard"
      }
    },
    "loggers": {
      "etl_core": {
        "level": "DEBUG",
        "handlers": ["console"],
        "propagate": false
      }
    }
  }
}
```

## Configuration Features

### Configuration Management
- **YAML Support**: YAML configuration file support
- **Environment Variables**: Environment variable override support
- **Default Values**: Sensible default configurations
- **Validation**: Configuration validation and error handling

### Configuration Hierarchy
- **Default Configuration**: Built-in default values
- **File Configuration**: Configuration file values
- **Environment Variables**: Environment variable overrides
- **Runtime Configuration**: Runtime configuration updates

## Error Handling

### Configuration Errors
- **Clear Messages**: Descriptive error messages for configuration issues
- **Validation Errors**: Path-based error reporting for configuration problems
- **Type Information**: Detailed type mismatch information
- **Context**: Configuration and validation context in error messages

### Error Types
- **Missing Files**: Required configuration files not found
- **Invalid Values**: Configuration values outside valid ranges
- **Type Mismatches**: Configuration types incompatible with schema
- **Format Errors**: Invalid configuration file formats

### Error Reporting
```json
{
  "configuration_error": {
    "file_path": "api_config.yaml",
    "field_path": "api.port",
    "expected_type": "integer",
    "actual_type": "string",
    "error_type": "type_mismatch",
    "message": "Port must be an integer value"
  }
}
```

## Performance Considerations

### Configuration Loading
- **Lazy Loading**: Load configurations only when needed
- **Caching**: Cache frequently used configurations
- **Validation**: Optimize configuration validation performance
- **Memory**: Minimize memory usage for large configurations

### Environment Management
- **Environment Detection**: Efficient environment detection
- **Configuration Override**: Optimize configuration override mechanisms
- **Variable Resolution**: Efficient environment variable resolution
- **Performance**: Balance configuration flexibility with performance

## Configuration

### Configuration Options
- **API Settings**: Configure API server and authentication settings
- **Logging Settings**: Set logging levels, handlers, and formatters
- **Environment Settings**: Configure environment-specific settings
- **Security Settings**: Configure security and access control

### Environment Configuration
- **Environment Variables**: Set environment-specific variables
- **Configuration Override**: Override configurations per environment
- **Validation**: Validate configurations per environment
- **Performance Tuning**: Optimize configuration performance

## Best Practices

### Configuration Design
- **Centralized Management**: Centralize configuration management
- **Environment Separation**: Separate configurations by environment
- **Default Values**: Provide sensible default values
- **Validation**: Implement comprehensive configuration validation

### Security
- **Sensitive Data**: Handle sensitive configuration data securely
- **Access Control**: Control access to configuration files
- **Encryption**: Encrypt sensitive configuration values
- **Audit Logging**: Log configuration changes and access

### Maintenance
- **Version Control**: Version control configuration files
- **Documentation**: Document configuration options and requirements
- **Testing**: Test configuration changes thoroughly
- **Monitoring**: Monitor configuration usage and performance
