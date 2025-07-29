from typing import TypeVar, Dict

T = TypeVar("T", bound=object)


def get_by_temp_id(mapping: Dict[str, T], user_assigned_id: str) -> T:
    """
    Look up the one object in `mapping.values()` whose user_assigned_id matches
    """
    for obj in mapping.values():
        if getattr(obj, "id", None) == user_assigned_id:
            return obj
    raise KeyError(f"no object with user_assigned_id = {user_assigned_id}")
