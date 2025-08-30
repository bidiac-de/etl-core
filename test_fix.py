#!/usr/bin/env python3

from etl_core.components.databases.mariadb.mariadb_write import MariaDBWrite
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType

# Test the fix
mock_schema = Schema(fields=[
    FieldDef(name="id", data_type=DataType.INTEGER),
    FieldDef(name="name", data_type=DataType.STRING),
    FieldDef(name="email", data_type=DataType.STRING),
])

print("Creating component...")
try:
    write_comp = MariaDBWrite(
        name="test_write",
        description="Test write component",
        comp_type="write_mariadb",
        entity_name="users",
        credentials_id=1,
        in_port_schemas={"in": mock_schema}
    )
    print("Component created successfully!")
    print(f"Entity name: {write_comp.entity_name}")
    print(f"Query: {getattr(write_comp, '_query', 'Not set')}")
    print(f"In port schemas: {write_comp.in_port_schemas}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
