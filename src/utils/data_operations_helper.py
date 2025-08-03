from typing import Any, Dict
from src.components.data_operations.filter_component import ComparisonRule


class DataOperationsHelper:
    """Helper class providing shared logic for data operations."""

    @staticmethod
    def matches(row: Dict[str, Any], rule: ComparisonRule) -> bool:
        """
        Check if a row matches the given comparison rule.
        """
        value = row.get(rule.column)
        target = rule.value
        op = rule.operator

        if op == "==":
            return value == target
        elif op == "!=":
            return value != target
        elif op == ">":
            return value > target
        elif op == "<":
            return value < target
        elif op == ">=":
            return value >= target
        elif op == "<=":
            return value <= target
        return False