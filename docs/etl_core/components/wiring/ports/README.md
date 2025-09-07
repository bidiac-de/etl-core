# Port Components

Port components provide declarative specifications for component input and output connections.

## Overview

Port components define how components connect to each other in the ETL pipeline, specifying input and output ports with their characteristics and constraints.

## Components

### Port Specifications
- **InPortSpec**: Input port specifications
- **OutPortSpec**: Output port specifications
- **EdgeRef**: Edge routing specifications

## Port Types

### Input Ports (InPortSpec)
Define how components receive data from upstream components.

#### Required Fields
- `name`: Port name (e.g., 'in', 'left', 'right')
- `required`: Whether the port must be connected (default: False)
- `fanin`: Connection constraint ('one' or 'many', default: 'many')

#### Fanin Options
- **'one'**: Exactly one upstream connection allowed
- **'many'**: Multiple upstream connections allowed

### Output Ports (OutPortSpec)
Define how components send data to downstream components.

#### Required Fields
- `name`: Port name (e.g., 'out', 'pass', 'fail')
- `required`: Whether the port must be connected (default: False)
- `fanout`: Connection constraint ('one' or 'many', default: 'many')

#### Fanout Options
- **'one'**: Exactly one downstream connection allowed
- **'many'**: Multiple downstream connections allowed

### Edge References (EdgeRef)
Define routing from output ports to input ports.

#### Required Fields
- `to`: Target component name
- `in_port`: Target input port name (optional if target has only one input port)

## Example Usage

### Input Port Specification
```python
from etl_core.components.wiring.ports import InPortSpec

# Single required input
input_port = InPortSpec(
    name="in",
    required=True,
    fanin="one"
)

# Multiple optional inputs
input_ports = (
    InPortSpec(name="left", required=True, fanin="one"),
    InPortSpec(name="right", required=True, fanin="one"),
    InPortSpec(name="config", required=False, fanin="one")
)
```

### Output Port Specification
```python
from etl_core.components.wiring.ports import OutPortSpec

# Single output
output_port = OutPortSpec(
    name="out",
    required=True,
    fanout="many"
)

# Multiple outputs
output_ports = (
    OutPortSpec(name="pass", required=True, fanout="many"),
    OutPortSpec(name="fail", required=False, fanout="many")
)
```

### Edge Routing
```python
from etl_core.components.wiring.ports import EdgeRef

# Route to specific input port
edge = EdgeRef(
    to="filter_component",
    in_port="in"
)

# Route to component with single input
edge = EdgeRef(
    to="aggregation_component"
)
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

## Validation

### Port Validation
- **Name Uniqueness**: Port names must be unique within a component
- **Required Ports**: Required ports must be connected
- **Connection Limits**: Fanin/fanout constraints are enforced
- **Port Existence**: Referenced ports must exist

### Edge Validation
- **Target Existence**: Target components must exist
- **Port Compatibility**: Input/output port types must match
- **Connection Limits**: Fanin/fanout constraints are enforced

## Error Handling

### Port Errors
- **Missing Required Ports**: Clear error messages for unconnected required ports
- **Invalid Port Names**: Error messages for invalid port names
- **Connection Violations**: Clear messages for fanin/fanout violations

### Edge Errors
- **Missing Targets**: Error messages for non-existent target components
- **Invalid Ports**: Error messages for non-existent target ports
- **Connection Conflicts**: Error messages for connection constraint violations

## Best Practices

### Port Naming
- Use descriptive names (e.g., 'data_in', 'error_out')
- Follow consistent naming conventions
- Use standard names for common ports ('in', 'out', 'pass', 'fail')

### Port Design
- Make ports optional when possible
- Use 'many' fanin/fanout for flexibility
- Use 'one' fanin/fanout for strict requirements

### Edge Routing
- Be explicit about target ports
- Use descriptive component names
- Validate connections during pipeline construction
