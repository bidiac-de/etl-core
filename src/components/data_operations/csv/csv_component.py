from src.receivers.data_operations_receiver import DataOperationsReceiver
from abc import ABC
from enum import Enum

class Delimiter(Enum):
    COMMA = ','
    SEMICOLON = ';'
    TAB = '\t'

class CSV(DataOperationsReceiver, ABC):
    def __init__(self, id: int = 0, separator: Delimiter = Delimiter.COMMA):
        super().__init__(id)
        self.separator = separator