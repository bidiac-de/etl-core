from typing import TypeVar, Dict

T = TypeVar("T", bound=object)


def get_by_temp_id(mapping: Dict[str, T], temp_id: int) -> T:
    """
    Look up the one object in `mapping.values()` whose `temp_id` matches.
    """
    for obj in mapping.values():
        if getattr(obj, "temp_id", None) == temp_id:
            return obj
    raise KeyError(f"no object with temp_id={temp_id}")
