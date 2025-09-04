from typing import Iterable
from etl_core.components.wiring.schema import FieldDef, Schema


def fd(
    name: str,
    dtype: str,
    *,
    children: Iterable[FieldDef] | None = None,
) -> FieldDef:
    """Convenience factory for FieldDef with optional children."""
    return FieldDef(name=name, data_type=dtype, children=list(children or []))


def schema_from_fields(fields: Iterable[FieldDef]) -> Schema:
    """Build Schema from iterable of FieldDefs."""
    return Schema(fields=list(fields))
