from src.components.base_component import Component
from abc import ABC


class DataOperationComponent(Component, ABC):
    """
    Base class for data operations.
    All data operations should inherit from this class.
    """
