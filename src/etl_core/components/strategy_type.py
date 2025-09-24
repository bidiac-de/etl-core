from enum import Enum


class StrategyType(str, Enum):
    """
    Enum for different strategy types
    """

    ROW = "row"
    BULK = "bulk"
    BIGDATA = "bigdata"
