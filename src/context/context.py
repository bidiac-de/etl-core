from typing import List
from src.context.environment import Environment
from src.context.context_parameter import ContextParameter
from src.context.context_provider import IContextProvider


class Context(IContextProvider):
    def __init__(self, id: int, name: str, environment: Environment, parameters: List[ContextParameter]):
        self.id = id
        self.name = name
        self.environment = environment
        self.parameters = parameters

    def get_parameter(self, key: str) -> str:
        for param in self.parameters:
            if param.key == key:
                return param.value
        raise KeyError(f"Parameter with key '{key}' not found in context '{self.name}'")