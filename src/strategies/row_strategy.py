from src.strategies.base_strategy import ExecutionStrategy


class RowExecutionStrategy(ExecutionStrategy):
    """
    Strategy that determines HOW a component executes.
    For serial strategy, it processes data row by row.
    """

    def execute(self, component, inputs, metrics):
        return component.process_row(inputs, metrics)
