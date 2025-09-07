# ETL Core Components

This directory contains all the core components of the ETL system, organized by functionality. The ETL Core Components provide a comprehensive framework for building scalable, maintainable, and performant data processing pipelines.

## Overview

The ETL Core Components are designed around a modular, component-based architecture that enables:
- **Flexible Data Processing**: Support for row, bulk, and bigdata processing strategies
- **Extensible Design**: Easy addition of new components and functionality
- **Type Safety**: Full type checking and validation throughout the system
- **Performance Optimization**: Efficient processing for various data scales
- **Error Handling**: Robust error handling and recovery mechanisms
- **Monitoring**: Comprehensive metrics and logging capabilities

## Component Architecture

### Base Component Hierarchy

```
BaseComponent (base_component.py)
├── DatabaseComponent (databases/database.py)
│   ├── SQLDatabaseComponent (databases/sql_database.py)
│   │   ├── PostgreSQLComponent (databases/postgresql/)
│   │   ├── MariaDBComponent (databases/mariadb/)
│   │   └── SQLServerComponent (databases/sqlserver/)
│   └── MongoDBComponent (databases/mongodb/)
├── FileComponent (file_components/file_component.py)
│   ├── CSVComponent (file_components/csv/)
│   ├── ExcelComponent (file_components/excel/)
│   └── JSONComponent (file_components/json/)
└── DataOperationComponent (data_operations/)
    ├── AggregationComponent (data_operations/aggregation/)
    ├── FilterComponent (data_operations/filter/)
    ├── MergeComponent (data_operations/merge/)
    ├── SplitComponent (data_operations/split/)
    └── SchemaMappingComponent (data_operations/schema_mapping/)
```

### Component Registration System

```python
@register_component("component_type")
class MyComponent(BaseComponent):
    """Component implementation."""
    
    # Component configuration
    config_field: str = Field(..., description="Configuration field")
    
    # Port definitions
    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)
    
    # Processing methods
    async def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> AsyncIterator[Out]:
        """Process individual rows."""
        pass
    
    async def process_bulk(self, dataframe: pd.DataFrame, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        """Process pandas DataFrames."""
        pass
    
    async def process_bigdata(self, ddf: dd.DataFrame, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        """Process Dask DataFrames."""
        pass
```

## Component Categories

### Data Operations Components

Advanced data transformation and manipulation components:

#### [Aggregation Component](./data_operations/aggregation/README.md)
**Purpose**: Group-by operations with various aggregation functions
- **Features**: Sum, count, mean, min, max, std, custom functions
- **Strategies**: Row buffering, bulk processing, bigdata distributed
- **Use Cases**: Data summarization, analytics, reporting
- **Advanced**: Nested field grouping, conditional aggregations, custom functions

#### [Filter Component](./data_operations/filter/README.md)
**Purpose**: Data filtering based on comparison rules
- **Features**: Simple comparisons, complex logical expressions, nested rules
- **Operators**: ==, !=, >, <, >=, <=, contains, custom operators
- **Strategies**: Row-by-row, bulk DataFrame, bigdata lazy evaluation
- **Use Cases**: Data quality, conditional processing, data routing

#### [Merge Component](./data_operations/merge/README.md)
**Purpose**: Combine data from multiple input sources
- **Features**: Multi-input routing, payload combination, fan-in operations
- **Strategies**: Row merging, bulk DataFrame concatenation, bigdata union
- **Use Cases**: Data consolidation, multi-source integration, data joining

#### [Split Component](./data_operations/split/README.md)
**Purpose**: Distribute data to multiple output destinations
- **Features**: Multi-output routing, payload duplication, fan-out operations
- **Strategies**: Row splitting, bulk DataFrame partitioning, bigdata distribution
- **Use Cases**: Data distribution, parallel processing, multi-destination routing

#### [Schema Mapping Component](./data_operations/schema_mapping/README.md)
**Purpose**: Transform data schemas and structure
- **Features**: Field mapping, data type conversion, nested transformations
- **Strategies**: Row transformation, bulk DataFrame operations, bigdata lazy evaluation
- **Use Cases**: Data standardization, format conversion, schema evolution

### Database Components

Database connectivity and operations with enterprise features:

#### [PostgreSQL Component](./databases/postgresql/README.md)
**Purpose**: PostgreSQL database operations with advanced SQL features
- **Features**: JSON/JSONB support, array operations, range types, full-text search
- **Strategies**: Row streaming, bulk processing, bigdata distributed
- **Advanced**: PostGIS, custom data types, extensions, window functions
- **Performance**: Connection pooling, query optimization, batch processing

#### [MariaDB Component](./databases/mariadb/README.md)
**Purpose**: MariaDB database operations with MySQL compatibility
- **Features**: UTF8MB4 support, window functions, CTEs, recursive queries
- **Strategies**: Row processing, bulk operations, bigdata lazy evaluation
- **Advanced**: Storage engines, JSON functions, parallel replication
- **Performance**: Query cache, buffer pool optimization, thread pooling

#### [MongoDB Component](./databases/mongodb/README.md)
**Purpose**: MongoDB document database operations
- **Features**: Document queries, aggregation pipelines, indexing
- **Strategies**: Document streaming, bulk operations, bigdata sharding
- **Advanced**: GridFS, change streams, transactions, replica sets
- **Performance**: Connection pooling, query optimization, batch operations

#### [SQL Server Component](./databases/sqlserver/README.md)
**Purpose**: Microsoft SQL Server database operations
- **Features**: T-SQL support, stored procedures, functions, views
- **Strategies**: Row processing, bulk operations, bigdata distributed
- **Advanced**: CLR integration, XML support, spatial data types
- **Performance**: Connection pooling, query optimization, batch processing

### File Components

File format support with comprehensive I/O capabilities:

#### [CSV Component](./file_components/csv/README.md)
**Purpose**: CSV file reading and writing operations
- **Features**: Custom delimiters, encoding support, header handling
- **Strategies**: Row streaming, bulk DataFrame operations, bigdata chunked processing
- **Advanced**: Schema inference, data type detection, compression support
- **Performance**: Streaming I/O, memory optimization, parallel processing

#### [Excel Component](./file_components/excel/README.md)
**Purpose**: Excel file reading and writing operations
- **Features**: Multi-sheet support, cell formatting, formula handling
- **Strategies**: Row processing, bulk operations, bigdata sheet-wise processing
- **Advanced**: Chart support, pivot tables, macro handling
- **Performance**: Streaming I/O, memory management, parallel sheet processing

#### [JSON Component](./file_components/json/README.md)
**Purpose**: JSON file reading and writing operations
- **Features**: Nested object support, array handling, schema validation
- **Strategies**: Document streaming, bulk operations, bigdata chunked processing
- **Advanced**: JSONPath queries, schema evolution, compression support
- **Performance**: Streaming I/O, memory optimization, parallel processing

### Wiring Components

Core system wiring and data flow management:

#### [Ports Component](./wiring/ports/README.md)
**Purpose**: Port definitions and connection management
- **Features**: Input/output port specifications, fan-in/fan-out support
- **Types**: InPortSpec, OutPortSpec, EdgeRef, PortConnection
- **Advanced**: Dynamic port creation, port validation, connection routing
- **Performance**: Connection pooling, port caching, efficient routing

#### [Schema Component](./wiring/schema/README.md)
**Purpose**: Schema definitions and data structure management
- **Features**: Field definitions, data type specifications, validation rules
- **Types**: Schema, FieldDef, DataType, ColumnDefinition
- **Advanced**: Nested schemas, schema evolution, type inference
- **Performance**: Schema caching, validation optimization, type checking

#### [Validation Component](./wiring/validation/README.md)
**Purpose**: Data validation and quality assurance
- **Features**: Schema validation, data type checking, constraint validation
- **Strategies**: Row validation, bulk DataFrame validation, bigdata lazy validation
- **Advanced**: Custom validators, conditional validation, error reporting
- **Performance**: Validation caching, parallel validation, error batching

## Base Components

### Core Foundation Classes

#### Base Component (`base_component.py`)
**Purpose**: Foundation class for all ETL components
- **Features**: Common interface, lifecycle management, error handling
- **Methods**: process_row, process_bulk, process_bigdata, validate, configure
- **Advanced**: Metrics collection, state management, configuration validation
- **Performance**: Method caching, lazy initialization, resource management

#### Component Registry (`component_registry.py`)
**Purpose**: Component discovery and registration system
- **Features**: Auto-discovery, type registration, dependency management
- **Advanced**: Plugin system, hot-reloading, version management
- **Performance**: Registry caching, lazy loading, efficient lookup

#### Runtime State (`runtime_state.py`)
**Purpose**: Component execution state management
- **Features**: State tracking, lifecycle management, error recovery
- **Advanced**: State persistence, checkpointing, rollback support
- **Performance**: State caching, efficient serialization, memory management

#### Envelopes (`envelopes.py`)
**Purpose**: Data envelope system for component communication
- **Features**: Data packaging, metadata handling, routing information
- **Types**: InEnvelope, OutEnvelope, TaggedEnvelope, FlushEnvelope
- **Advanced**: Compression, encryption, routing metadata
- **Performance**: Efficient serialization, memory optimization, batch processing

#### Dataclasses (`dataclasses.py`)
**Purpose**: Common data structures and utilities
- **Features**: Type definitions, validation schemas, utility functions
- **Types**: ComponentConfig, PortSpec, Metrics, ErrorInfo
- **Advanced**: Custom types, validation rules, serialization support
- **Performance**: Efficient serialization, type checking, memory optimization

#### Stub Components (`stubcomponents.py`)
**Purpose**: Placeholder components for testing and development
- **Features**: Mock implementations, test data generation, debugging support
- **Types**: StubComponent, TestComponent, DebugComponent
- **Advanced**: Configurable behavior, test scenarios, performance testing
- **Performance**: Minimal overhead, fast execution, memory efficiency

## Processing Strategies

### Row Processing
- **Use Case**: Real-time streaming data processing
- **Characteristics**: Low latency, memory efficient, immediate processing
- **Components**: All components support row processing
- **Performance**: Optimized for streaming, minimal buffering

### Bulk Processing
- **Use Case**: Medium-sized datasets, batch processing
- **Characteristics**: Higher throughput, pandas DataFrames, batch operations
- **Components**: All components support bulk processing
- **Performance**: Optimized for batch operations, memory management

### BigData Processing
- **Use Case**: Large datasets, distributed processing
- **Characteristics**: Dask DataFrames, lazy evaluation, distributed computing
- **Components**: All components support bigdata processing
- **Performance**: Scalable to large datasets, distributed execution

## Configuration and Deployment

### Component Configuration
```python
# Component configuration example
component_config = {
    "name": "my_component",
    "type": "filter",
    "config": {
        "rule": {
            "column": "age",
            "operator": ">=",
            "value": 18
        }
    },
    "ports": {
        "input": ["in"],
        "output": ["pass", "fail"]
    },
    "strategy": "bulk"
}
```

### Pipeline Definition
```python
# Pipeline configuration example
pipeline_config = {
    "name": "data_processing_pipeline",
    "components": [
        {
            "name": "csv_reader",
            "type": "csv_read",
            "config": {"filepath": "input.csv"}
        },
        {
            "name": "age_filter",
            "type": "filter",
            "config": {"rule": {"column": "age", "operator": ">=", "value": 18}}
        },
        {
            "name": "postgresql_writer",
            "type": "postgresql_write",
            "config": {"table": "filtered_data"}
        }
    ],
    "connections": [
        {"from": "csv_reader", "to": "age_filter", "port": "out"},
        {"from": "age_filter", "to": "postgresql_writer", "port": "pass"}
    ]
}
```

## Performance Optimization

### Memory Management
- **Buffer Management**: Configurable buffer sizes for different strategies
- **Memory Monitoring**: Real-time memory usage tracking and alerts
- **Garbage Collection**: Efficient memory cleanup and resource management
- **Memory Pooling**: Reuse of memory objects for better performance

### Processing Optimization
- **Parallel Processing**: Multi-threaded and multi-process execution
- **Lazy Evaluation**: Deferred computation for bigdata strategies
- **Caching**: Intelligent caching of frequently used data and computations
- **Batch Optimization**: Optimal batch sizes for different data types

### Query Optimization
- **Index Usage**: Automatic index utilization for database components
- **Query Planning**: Optimized query execution plans
- **Connection Pooling**: Efficient database connection management
- **Prepared Statements**: Security and performance optimization

## Error Handling and Monitoring

### Error Handling
- **Graceful Degradation**: Continue processing despite component failures
- **Error Recovery**: Automatic retry mechanisms with exponential backoff
- **Error Reporting**: Detailed error information and stack traces
- **Circuit Breaker**: Prevent cascade failures in distributed systems

### Monitoring and Metrics
- **Performance Metrics**: Throughput, latency, resource utilization
- **Quality Metrics**: Data completeness, accuracy, error rates
- **Health Monitoring**: Component health checks and status reporting
- **Alerting**: Real-time alerts for critical issues and performance degradation

### Logging
- **Structured Logging**: JSON-formatted logs for easy parsing and analysis
- **Log Levels**: Configurable logging levels for different environments
- **Log Aggregation**: Centralized logging for distributed systems
- **Audit Trails**: Complete audit trails for compliance and debugging

## Best Practices

### Component Design
- **Single Responsibility**: Each component should have a single, well-defined purpose
- **Loose Coupling**: Minimize dependencies between components
- **High Cohesion**: Related functionality should be grouped together
- **Interface Segregation**: Use specific interfaces rather than general ones

### Performance
- **Profile Early**: Identify performance bottlenecks early in development
- **Optimize Strategically**: Focus optimization efforts on critical paths
- **Monitor Continuously**: Implement comprehensive monitoring and alerting
- **Test Performance**: Include performance testing in your test suite

### Error Handling
- **Fail Fast**: Detect and report errors as early as possible
- **Graceful Degradation**: Provide fallback mechanisms for critical failures
- **Comprehensive Logging**: Log all errors with sufficient context
- **User-Friendly Messages**: Provide clear, actionable error messages

### Testing
- **Unit Testing**: Test individual components in isolation
- **Integration Testing**: Test component interactions and data flow
- **Performance Testing**: Validate performance under various load conditions
- **End-to-End Testing**: Test complete data processing pipelines

## Development and Maintenance

### Development Workflow
- **Component Development**: Follow the established component architecture patterns
- **Testing**: Implement comprehensive test coverage for all components
- **Documentation**: Maintain up-to-date documentation for all components
- **Version Control**: Use semantic versioning for component releases

### Maintenance
- **Regular Updates**: Keep components updated with latest dependencies
- **Security Patches**: Apply security patches promptly
- **Performance Monitoring**: Continuously monitor and optimize performance
- **Documentation Updates**: Keep documentation current with code changes

### Extension and Customization
- **Custom Components**: Create custom components following the established patterns
- **Plugin System**: Use the component registry for plugin-based extensions
- **Configuration**: Use flexible configuration systems for component customization
- **API Design**: Design clean, intuitive APIs for component interaction
