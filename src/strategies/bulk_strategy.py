from src.strategies.base_strategy import ExecutionStrategy


class BulkExecutionStrategy(ExecutionStrategy):
    def execute(self, component, inputs):
        return component.process_bulk(inputs)
