from src.components.base_component import Component


class DataOperationComponent(Component):
    """
    Base class for data operations.
    All data operations should inherit from this class.
    """

    def execute(self, data, **kwargs):
        """
        Execute the data operation.
        This method should be overridden by subclasses.
        :param data: Input data for the operation.
        :param kwargs: Additional parameters for the operation.
        :return: Result of the operation.
        """
        raise NotImplementedError("Subclasses must implement this method.")
