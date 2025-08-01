import importlib
import pkgutil

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
    for finder, mod_name, is_pkg in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        importlib.import_module(mod_name)
        if is_pkg:
            autodiscover_components(mod_name)