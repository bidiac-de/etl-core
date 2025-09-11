# ETL Core

The ETL Core provides a comprehensive framework for building scalable, maintainable, and performant data processing pipelines. It offers a modular, component-based architecture that enables flexible data processing across different scales and strategies.

## Architecture Overview

The ETL Core is built around a hierarchical component system that supports three processing strategies:
- **Row Processing**: Real-time streaming data processing
- **Bulk Processing**: Medium-sized datasets with pandas DataFrames  
- **BigData Processing**: Large-scale distributed processing with Dask DataFrames

## Component Categories

### [Components](./components/README.md)
Core ETL components organized by functionality:
- **Data Operations**: Filtering, aggregation, merging, splitting, and schema mapping
- **Database Components**: Connectivity and operations for various database systems
- **File Components**: Reading and writing different file formats
- **Wiring Components**: Port definitions, schema management, and validation

### [API](./api/README.md)
REST API endpoints for component management and pipeline execution.

### [Configuration](./config/README.md)
Configuration management and validation systems.

### [Context](./context/README.md)
Runtime context and credential management.

### [Job Execution](./job_execution/README.md)
Job execution engine and workflow management.

### [Logging](./logging/README.md)
Comprehensive logging and monitoring capabilities.

### [Metrics](./metrics/README.md)
Performance metrics and monitoring systems.

### [Persistence](./persistance/README.md)
Data persistence and state management.

### [Receivers](./receivers/README.md)
Data receiver implementations for different component types.

### [Strategies](./strategies/README.md)
Processing strategy implementations and configurations.

### [Utils](./utils/README.md)
Utility functions and helper classes.

### [Main Application](./main.md)
Main application entry point and FastAPI setup.

### [Singletons](./singletons.md)
Singleton pattern implementation for core system services.

## Getting Started

All components follow a consistent interface pattern and can be used independently or in combination to build complex ETL pipelines. For detailed information about specific components, refer to the individual component documentation.
