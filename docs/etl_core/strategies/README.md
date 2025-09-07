# Execution Strategies

This directory contains execution strategies that define how components process data in different modes.

## Overview

Execution strategies determine the processing approach for ETL components, supporting different data processing patterns optimized for various use cases and data sizes.

## Components

### [Base Strategy](./base_strategy.py)
- **File**: `base_strategy.py`
- **Purpose**: Abstract base class for all execution strategies
- **Features**: Common interface, async execution, metrics integration

### [Row Strategy](./row_strategy.py)
- **File**: `row_strategy.py`
- **Purpose**: Row-by-row processing strategy
- **Features**: Streaming processing, memory efficient, real-time processing

### [Bulk Strategy](./bulk_strategy.py)
- **File**: `bulk_strategy.py`
- **Purpose**: Batch processing strategy for pandas DataFrames
- **Features**: Batch processing, higher performance, memory intensive

### [BigData Strategy](./bigdata_strategy.py)
- **File**: `bigdata_strategy.py`
- **Purpose**: Distributed processing strategy for Dask DataFrames
- **Features**: Distributed processing, scalable, handles large datasets

## Strategy Types

### Row Processing
- **Use Case**: Real-time processing, memory-constrained environments
- **Data Type**: Individual records/rows
- **Memory Usage**: Low
- **Performance**: Slower but memory efficient
- **Streaming**: Yes, immediate processing

### Bulk Processing
- **Use Case**: Medium-sized datasets, batch processing
- **Data Type**: pandas DataFrames
- **Memory Usage**: Medium to high
- **Performance**: Fast for smaller datasets
- **Streaming**: No, batch processing

### BigData Processing
- **Use Case**: Large datasets, distributed processing
- **Data Type**: Dask DataFrames
- **Memory Usage**: Distributed
- **Performance**: Fast for large datasets
- **Streaming**: Yes, distributed streaming

## Implementation

### Base Strategy Interface
```python
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator
from etl_core.components.envelopes import Out
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics

class ExecutionStrategy(ABC):
    @abstractmethod
    async def execute(
        self,
        component: Component,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        """Stream through the component logic, yielding outputs."""
        raise NotImplementedError
```

### Strategy Selection
Strategies are selected based on:
- **Data Size**: Small (row), medium (bulk), large (bigdata)
- **Memory Constraints**: Available memory for processing
- **Performance Requirements**: Speed vs. memory trade-offs
- **Processing Pattern**: Real-time vs. batch processing

## Configuration

### Strategy Configuration
- **Strategy Type**: Row, bulk, or bigdata
- **Component Compatibility**: Not all components support all strategies
- **Resource Requirements**: Memory and CPU requirements
- **Performance Tuning**: Strategy-specific optimization options

### Component Integration
- **Strategy Interface**: Components implement strategy-specific methods
- **Metrics Integration**: Strategies collect performance metrics
- **Error Handling**: Strategy-specific error handling
- **Resource Management**: Memory and resource cleanup

## Performance Characteristics

### Row Strategy
- **Memory**: Minimal memory usage
- **CPU**: Low to medium CPU usage
- **Latency**: Low latency per record
- **Throughput**: Lower overall throughput
- **Scalability**: Limited by single-threaded processing

### Bulk Strategy
- **Memory**: High memory usage
- **CPU**: Medium to high CPU usage
- **Latency**: Higher latency per batch
- **Throughput**: High throughput for batches
- **Scalability**: Limited by available memory

### BigData Strategy
- **Memory**: Distributed memory usage
- **CPU**: High CPU usage across cores
- **Latency**: Variable latency depending on data size
- **Throughput**: Very high throughput
- **Scalability**: Highly scalable across multiple cores/nodes

## Error Handling

### Strategy-Specific Errors
- **Row Strategy**: Individual record errors
- **Bulk Strategy**: Batch processing errors
- **BigData Strategy**: Distributed processing errors

### Error Recovery
- **Retry Logic**: Strategy-specific retry mechanisms
- **Error Propagation**: How errors are handled and reported
- **Resource Cleanup**: Cleanup on error conditions
- **Graceful Degradation**: Fallback strategies

## Best Practices

### Strategy Selection
- **Choose Row**: For real-time processing, memory constraints
- **Choose Bulk**: For medium datasets, batch processing
- **Choose BigData**: For large datasets, distributed processing

### Performance Optimization
- **Memory Management**: Monitor memory usage per strategy
- **Resource Allocation**: Allocate resources based on strategy
- **Monitoring**: Monitor strategy performance metrics
- **Tuning**: Tune strategy parameters for optimal performance

### Error Handling
- **Strategy-Specific Handling**: Implement strategy-specific error handling
- **Resource Cleanup**: Ensure proper cleanup on errors
- **Error Reporting**: Provide clear error messages
- **Recovery**: Implement appropriate recovery mechanisms
