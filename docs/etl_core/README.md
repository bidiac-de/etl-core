# ETL Core Components

This directory contains the core components of the ETL (Extract, Transform, Load) system.

## Components Overview

The ETL Core is organized into several main component categories:

### [Data Operations](./components/data_operations/README.md)
Components for data transformation and manipulation operations including filtering, aggregation, merging, and schema mapping.

### [Databases](./components/databases/README.md)
Database connectivity and operations for various database systems including MariaDB, MongoDB, PostgreSQL, and SQL Server.

### [File Components](./components/file_components/README.md)
Components for reading and writing various file formats including CSV, Excel, and JSON.

### [Wiring](./components/wiring/README.md)
Core wiring and configuration components for ports, schema definitions, and validation.

## Base Components

- **Base Component**: The foundational component class that all other components inherit from
- **Component Registry**: Registry system for managing and discovering components
- **Runtime State**: State management for component execution
- **Envelopes**: Data envelope system for component communication

## Usage

All components follow a consistent interface pattern and can be used independently or in combination to build complex ETL pipelines.
