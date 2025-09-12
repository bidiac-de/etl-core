# Port Components

Port components provide **declarative specifications** for component input and output connections with comprehensive support for connection constraints and validation.

## Overview

Port components define how components connect to each other in the ETL pipeline, specifying input and output ports with their characteristics and constraints. They provide **type-safe port definitions** and **connection validation** for robust pipeline construction.

## Components

### Port Functions
- **InPortSpec**: Input port specifications with fanin control
- **OutPortSpec**: Output port specifications with fanout control
- **EdgeRef**: Edge routing specifications for component connections
- **Port Validation**: Comprehensive port configuration validation

## Port Types

### Input Ports
Define how components receive data from upstream components.

#### Features
- **Port Naming**: Descriptive port names (e.g., 'in', 'left', 'right')
- **Required Ports**: Specify whether ports must be connected
- **Fanin Control**: Control upstream connection constraints
- **Connection Validation**: Validate port connections

### Output Ports
Define how components send data to downstream components.

#### Features
- **Port Naming**: Descriptive port names (e.g., 'out', 'pass', 'fail')
- **Required Ports**: Specify whether ports must be connected
- **Fanout Control**: Control downstream connection constraints
- **Connection Validation**: Validate port connections

### Edge References
Define routing from output ports to input ports.

#### Features
- **Target Specification**: Specify target component names
- **Port Routing**: Route to specific input ports
- **Connection Management**: Manage port connections
- **Routing Validation**: Validate edge routing

## JSON Configuration Examples

### Input Port Specification

```json
{
  "name": "in",
  "required": true,
  "fanin": "one"
}
```

### Multiple Input Ports

```json
{
  "input_ports": [
    {
      "name": "left",
      "required": true,
      "fanin": "one"
    },
    {
      "name": "right",
      "required": true,
      "fanin": "one"
    },
    {
      "name": "config",
      "required": false,
      "fanin": "one"
    }
  ]
}
```

### Output Port Specification

```json
{
  "name": "out",
  "required": true,
  "fanout": "many"
}
```

### Multiple Output Ports

```json
{
  "output_ports": [
    {
      "name": "pass",
      "required": true,
      "fanout": "many"
    },
    {
      "name": "fail",
      "required": false,
      "fanout": "many"
    }
  ]
}
```

### Edge Routing

```json
{
  "edges": [
    {
      "to": "filter_component",
      "in_port": "in"
    },
    {
      "to": "aggregation_component"
    }
  ]
}
```

## Port Constraints

### Required Ports
- **Required Input**: Must have at least one upstream connection
- **Required Output**: Must have at least one downstream connection
- **Optional Ports**: Can be left unconnected

### Connection Constraints
- **Fanin 'one'**: Enforces exactly one upstream connection
- **Fanin 'many'**: Allows multiple upstream connections
- **Fanout 'one'**: Enforces exactly one downstream connection
- **Fanout 'many'**: Allows multiple downstream connections

## Port Features

### Port Registration
- **Automatic Detection**: Automatic port detection and management
- **Port Validation**: Ensure proper port configuration
- **Port Statistics**: Track port usage and performance metrics
- **Port Management**: Comprehensive port management capabilities

### Connection Management
- **Connection Validation**: Ensure proper port compatibility
- **Connection Monitoring**: Track connection health and performance
- **Error Handling**: Handle connection errors gracefully
- **Connection Recovery**: Implement connection recovery mechanisms

### Edge Routing
- **Flexible Routing**: Route from output ports to input ports
- **Routing Validation**: Ensure proper edge configuration
- **Routing Optimization**: Optimize data flow through pipeline
- **Connection Management**: Manage port connections efficiently

## Error Handling

### Port Errors
- **Clear Messages**: Descriptive error messages for port issues
- **Port Validation**: Path-based error reporting for port problems
- **Connection Validation**: Detailed connection validation information
- **Context**: Port and connection context in error messages

### Error Types
- **Missing Required Ports**: Required ports not connected
- **Invalid Port Names**: Invalid or duplicate port names
- **Connection Violations**: Fanin/fanout constraint violations
- **Port Existence**: Referenced ports don't exist

### Error Reporting
```json
{
  "port_error": {
    "error_type": "missing_required_port",
    "port_name": "input_port",
    "component": "filter_component",
    "message": "Required port 'input_port' not connected"
  }
}
```

## Performance Considerations

### Port Management
- **Efficient Registration**: Optimize port registration and detection
- **Connection Pooling**: Use connection pooling for better performance
- **Port Statistics**: Track port usage and performance metrics
- **Connection Optimization**: Optimize port connections for performance

### Edge Routing
- **Routing Efficiency**: Optimize edge routing for data flow
- **Connection Validation**: Efficient connection validation
- **Routing Optimization**: Optimize data flow through pipeline
- **Connection Management**: Efficient connection management

### Validation
- **Early Validation**: Validate ports and connections early
- **Efficient Validation**: Optimize validation performance
- **Error Handling**: Handle validation errors efficiently
- **Performance**: Balance validation thoroughness with performance

## Configuration

### Port Options
- **Port Configuration**: Configure port specifications and constraints
- **Connection Settings**: Set connection parameters and limits
- **Validation Settings**: Configure validation options
- **Performance Tuning**: Optimize port performance

### Edge Options
- **Routing Configuration**: Configure edge routing parameters
- **Connection Limits**: Set connection limits and constraints
- **Validation Options**: Configure edge validation settings
- **Performance Settings**: Optimize edge routing performance

## Best Practices

### Port Naming
- **Descriptive Names**: Use descriptive names (e.g., 'data_in', 'error_out')
- **Consistent Conventions**: Follow consistent naming conventions
- **Standard Names**: Use standard names for common ports ('in', 'out', 'pass', 'fail')
- **Documentation**: Document port purposes and constraints

### Port Design
- **Optional Ports**: Make ports optional when possible
- **Flexible Connections**: Use 'many' fanin/fanout for flexibility
- **Strict Requirements**: Use 'one' fanin/fanout for strict requirements
- **Error Handling**: Implement robust error handling

### Edge Routing
- **Explicit Targets**: Be explicit about target ports
- **Descriptive Names**: Use descriptive component names
- **Validation**: Validate connections during pipeline construction
- **Documentation**: Document edge routing and connections

### Performance
- **Port Monitoring**: Monitor port usage and performance
- **Connection Health**: Track connection health and errors
- **Error Recovery**: Implement graceful error recovery
- **Optimization**: Optimize port connections for performance
