import importlib
import pkgutil
from typing import Any, Dict, List, Optional


def _sanitize_errors(errors: List[Dict]) -> List[Dict]:
    sanitized: List[Dict] = []
    for err in errors:
        filtered: Dict = {}
        for key in ("type", "loc", "msg", "url"):
            if key in err:
                filtered[key] = err[key]
        sanitized.append(filtered)
    return sanitized


def inline_defs(schema: dict) -> dict:
    """
    Recursively replace all $ref to #/$defs/... by
    inlining the corresponding definition, then drop $defs.
    """
    defs = schema.pop("$defs", {})

    def _walk(node):
        if isinstance(node, dict):
            # replace any $ref
            if "$ref" in node:
                ref = node["$ref"]
                # expect "#/$defs/ModelName"
                name = ref.rsplit("/", 1)[-1]
                definition = defs.get(name)
                if definition:
                    # inline its contents (and recurse)
                    return _walk(definition)
            # otherwise recurse into all values
            return {k: _walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_walk(v) for v in node]
        return node

    return _walk(schema)


def autodiscover_components(package_name: str) -> None:
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
