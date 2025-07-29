from src.strategies.base_strategy import ExecutionStrategy


class BulkExecutionStrategy(ExecutionStrategy):
    def execute(self, component, inputs, metrics):
        return component.process_bulk(inputs, metrics)
