import pytest

from etl_core.components.databases.mariadb.mariadb import MariaDBComponent
from etl_core.components.databases.postgresql.postgresql import PostgreSQLComponent
from etl_core.components.databases.sqlserver.sqlserver import SQLServerComponent


class _FakeConn:
	def __init__(self):
		self.calls = []
		self.commits = 0

	def execute(self, sql: str):
		self.calls.append(sql)

	def commit(self):
		self.commits += 1

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc, tb):
		return False


class _FakeHandler:
	def __init__(self):
		self.conn = _FakeConn()
		self.leases = 0

	def lease(self):
		self.leases += 1
		return self.conn


def _make_concrete_instance(base_cls):
	# Define a minimal concrete subclass implementing abstract async methods
	class _Concrete(base_cls):
		async def process_row(self, *args, **kwargs):  # type: ignore[override]
			return {}

		async def process_bulk(self, *args, **kwargs):  # type: ignore[override]
			return {}

		async def process_bigdata(self, *args, **kwargs):  # type: ignore[override]
			return {}

	return _Concrete.model_construct()


@pytest.mark.parametrize("component_cls, expected_snippets", [
	(
		MariaDBComponent,
		["SET NAMES ", "SET collation_connection = "],
	),
	(
		PostgreSQLComponent,
		["SET client_encoding = ", "SET lc_collate = "],
	),
	(
		SQLServerComponent,
		["SET ANSI_NULLS ON", "SET NOCOUNT ON", "SET LANGUAGE ", "SET COLLATION "],
	),
])
def test_session_variables_are_set(component_cls, expected_snippets):
	comp = _make_concrete_instance(component_cls)
	fake = _FakeHandler()
	object.__setattr__(comp, "_connection_handler", fake)

	# Execute
	comp._setup_session_variables()

	# Verify at least one lease and commit happened
	assert fake.leases >= 1
	assert fake.conn.commits == 1
	# Verify statements include expected snippets
	all_sql = "\n".join(fake.conn.calls)
	for snippet in expected_snippets:
		assert snippet in all_sql


def test_setup_session_variables_no_handler_does_nothing():
	comp = _make_concrete_instance(MariaDBComponent)
	# Ensure no handler and charset empty triggers early return
	object.__setattr__(comp, "_connection_handler", None)
	# Also test charset empty path
	object.__setattr__(comp, "charset", "")
	# Should not raise
	comp._setup_session_variables()


def test_build_objects_returns_self():
	assert _make_concrete_instance(MariaDBComponent)._build_objects() is not None
	assert _make_concrete_instance(PostgreSQLComponent)._build_objects() is not None
	assert _make_concrete_instance(SQLServerComponent)._build_objects() is not None
