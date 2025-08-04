from typing import List, Dict
from src.context.environment import Environment
from src.context.context_parameter import ContextParameter
from src.context.context_provider import IContextProvider


class Context(IContextProvider):
    def __init__(self, id: int, name: str, environment: Environment, parameters: List[ContextParameter]):
        self._id = id
        self._name = name
        self._environment = environment
        self._parameters: Dict[str, ContextParameter] = {p.key: p for p in parameters}

    @property
    def id(self) -> int:
        """Get the context ID."""
        return self._id

    @id.setter
    def id(self, value: int):
        """Set the context ID."""
        self._id = value

    @property
    def name(self) -> str:
        """Get the context name."""
        return self._name

    @name.setter
    def name(self, value: str):
        """Set the context name."""
        self._name = value

    @property
    def environment(self) -> Environment:
        """Get the associated environment."""
        return self._environment

    @environment.setter
    def environment(self, value: Environment):
        """Set the associated environment."""
        self._environment = value

    @property
    def parameters(self) -> List[ContextParameter]:
        """Return all context parameters as a list."""
        return list(self._parameters.values())

    @parameters.setter
    def parameters(self, params: List[ContextParameter]):
        """Replace context parameters with a new list."""
        self._parameters = {p.key: p for p in params}

    def get_parameter(self, key: str) -> str:
        """
        Retrieve the value of a parameter by its key.

        :param key: The parameter key to look up
        :return: The value of the parameter
        :raises KeyError: If the parameter does not exist
        """
        try:
            return self._parameters[key].value
        except KeyError:
            raise KeyError(f"Parameter with key '{key}' not found in context '{self._name}'")

    def set_parameter(self, key: str, value: str):
        """
        Update the value of an existing parameter.

        :param key: The parameter key to update
        :param value: The new value for the parameter
        :raises KeyError: If the parameter does not exist
        """
        if key in self._parameters:
            self._parameters[key].value = value
        else:
            raise KeyError(f"Cannot set value, parameter with key '{key}' not found.")