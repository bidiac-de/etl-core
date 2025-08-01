from src.strategies.base_strategy import ExecutionStrategy


class BigDataExecutionStrategy(ExecutionStrategy):
    def execute(self, component, inputs):
        return component.process_bigdata(inputs)
