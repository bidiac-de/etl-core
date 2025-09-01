import importlib
import pkgutil
from typing import Any, Dict, List, Optional, Tuple
import copy

_DEFS = "$defs"


def _sanitize_errors(errors: List[Dict]) -> List[Dict]:
    """
    Sanitize error details for API responses by filtering typical keys
     from error dicts.
    """
    sanitized: List[Dict] = []
    for err in errors:
        filtered: Dict = {}
        for key in ("type", "loc", "msg", "url"):
            if key in err:
                filtered[key] = err[key]
        sanitized.append(filtered)
    return sanitized


LocalRef = Tuple[str, ...]  # tuple of $defs names forming the ref stack


def inline_defs(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inline local JSON Schema $ref entries (#/$defs/...) safely.

    - Expands $defs references while avoiding infinite recursion on cycles.
    - If any local $ref remain (because of a cycle), it keeps $defs.
    - Otherwise $defs are removed from the final schema.

    This function is intentionally split into small helpers to keep
    complexity down and readability high.
    """
    root = copy.deepcopy(schema)
    defs = _extract_defs(root)
    if not defs:
        return root

    inlined = _inline_node(root, defs, path_stack=())
    if _contains_local_ref(inlined):
        # Keep $defs if any local refs remain (e.g., because of cycles).
        inlined[_DEFS] = defs
    else:
        inlined.pop(_DEFS, None)
    return inlined


def _extract_defs(schema: Dict[str, Any]) -> Dict[str, Any]:
    defs = schema.get(_DEFS)
    return defs if isinstance(defs, dict) else {}


def _inline_node(
    node: Any,
    defs: Dict[str, Any],
    path_stack: LocalRef,
) -> Any:
    if isinstance(node, dict):
        return _inline_dict(node, defs, path_stack)
    if isinstance(node, list):
        return [_inline_node(item, defs, path_stack) for item in node]
    return node


def _inline_dict(
    node: Dict[str, Any],
    defs: Dict[str, Any],
    path_stack: LocalRef,
) -> Dict[str, Any]:
    ref_name = _local_ref_name(node.get("$ref"))
    if ref_name is not None:
        return _expand_local_ref(node, ref_name, defs, path_stack)

    # Regular dict: recurse into values
    return {k: _inline_node(v, defs, path_stack) for k, v in node.items()}


def _expand_local_ref(
    node: Dict[str, Any],
    ref_name: str,
    defs: Dict[str, Any],
    path_stack: LocalRef,
) -> Dict[str, Any]:
    # Cycle? keep the $ref to avoid infinite recursion.
    if ref_name in path_stack:
        return node

    target = defs.get(ref_name)
    if not isinstance(target, dict):
        # Unknown or malformed target; keep the original $ref.
        return node

    # Expand the target, then merge any sibling keys on the $ref site.
    expanded = _inline_node(copy.deepcopy(target), defs, path_stack + (ref_name,))
    if not isinstance(expanded, dict):
        # Defensive: if target isn't an object, we can't merge.
        return node

    extras = {k: v for k, v in node.items() if k != "$ref"}
    if extras:
        expanded = _merge_object(expanded, _inline_node(extras, defs, path_stack))
    return expanded


def _merge_object(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    """
    Shallow merge two JSON objects. Right-hand keys override left. The right
    side is assumed to have been recursively inlined already.
    """
    merged = dict(left)
    merged.update(right)
    return merged


def _local_ref_name(ref: Any) -> Optional[str]:
    """
    Return the $defs entry name if `ref` is a local defs ref ("#/$defs/Name"),
    otherwise None.
    """
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        return ref.split("/")[-1]
    return None


def _contains_local_ref(node: Any) -> bool:
    """
    Detect if any local $defs ref remain in the tree. We keep this simple to
    avoid adding complexity to the main algorithm.
    """
    if isinstance(node, dict):
        ref_name = _local_ref_name(node.get("$ref"))
        if ref_name is not None:
            return True
        return any(_contains_local_ref(v) for v in node.values())
    if isinstance(node, list):
        return any(_contains_local_ref(it) for it in node)
    return False


def autodiscover_components(package_name: str) -> None:
    """
    Recursively import all components in the given package, so that they
    are registered by the decorator from the registry.
    """
    pkg = importlib.import_module(package_name)
    for finder, mod_name, is_pkg in pkgutil.walk_packages(
        pkg.__path__, pkg.__name__ + "."
    ):
        importlib.import_module(mod_name)
        if is_pkg:
            autodiscover_components(mod_name)


def _error_payload(code: str, message: str, **ctx: Any) -> Dict[str, Any]:
    """
    Create a consistent error body with a stable machine-readable code and
    optional context fields. Keep this tiny to satisfy flake8/cognitive limits.
    """
    payload: Dict[str, Any] = {"code": code, "message": message}
    if ctx:
        payload["context"] = ctx
    return payload


def _exc_meta(exc: BaseException) -> Dict[str, Optional[str]]:
    """
    Best-effort, safe metadata from an exception (no stack traces).
    """
    return {
        "type": exc.__class__.__name__,
        "cause": str(exc.__cause__) if exc.__cause__ else None,
        "context": str(exc.__context__) if exc.__context__ else None,
    }
