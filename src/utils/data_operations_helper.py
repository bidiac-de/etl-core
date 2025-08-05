from typing import Any, Dict
from src.components.data_operations.comparison_rule import ComparisonRule


class DataOperationsHelper:
    """Helper class providing shared logic for data operations."""

    @staticmethod
    def matches(row: Dict[str, Any], rule: ComparisonRule) -> bool:
        if rule.logical_operator:
            if rule.logical_operator == "AND":
                return all(DataOperationsHelper.matches(row, r) for r in rule.rules or [])
            if rule.logical_operator == "OR":
                return any(DataOperationsHelper.matches(row, r) for r in rule.rules or [])
            if rule.logical_operator == "NOT":
                return not any(DataOperationsHelper.matches(row, r) for r in rule.rules or [])


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
        elif op == "contains" and isinstance(value, str) and isinstance(target, str):
            return target in value

        return False