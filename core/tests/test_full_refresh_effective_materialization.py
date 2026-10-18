"""Regression tests for full-refresh materialization column alignment."""

from types import SimpleNamespace

from metadata.rendering import load_sql
from metadata.rendering.dialects.mssql import MssqlDialect


def test_mssql_effective_target_columns_exclude_retired_columns():
  td = SimpleNamespace(
    target_columns=[
      SimpleNamespace(id=1, target_column_name="bc_dim_customer_key", ordinal_position=1, active=False),
      SimpleNamespace(id=2, target_column_name="customer", ordinal_position=2, active=False),
      SimpleNamespace(id=3, target_column_name="person_type_code", ordinal_position=3, active=True),
      SimpleNamespace(id=4, target_column_name="customer_cnt", ordinal_position=4, active=True),
    ],
  )

  columns = MssqlDialect().get_effective_target_columns(td)

  assert [c.target_column_name for c in columns] == [
    "person_type_code",
    "customer_cnt",
  ]


def test_full_refresh_insert_uses_real_mssql_effective_target_columns(monkeypatch):
  td = SimpleNamespace(
    target_schema=SimpleNamespace(schema_name="bizcore"),
    target_dataset_name="bc_dim_customer_2",
    target_columns=[
      SimpleNamespace(id=1, target_column_name="bc_dim_customer_key", ordinal_position=1, active=False),
      SimpleNamespace(id=2, target_column_name="customer", ordinal_position=2, active=False),
      SimpleNamespace(id=3, target_column_name="person_type_code", ordinal_position=3, active=True),
      SimpleNamespace(id=4, target_column_name="customer_cnt", ordinal_position=4, active=True),
    ],
  )
  dialect = MssqlDialect()

  monkeypatch.setattr(
    load_sql,
    "render_select_for_target",
    lambda _td, _dialect: "SELECT person_type_code, customer_cnt",
  )

  sql = load_sql.render_full_refresh_sql(td, dialect)

  assert sql.startswith(
    "INSERT INTO bizcore.bc_dim_customer_2 (person_type_code, customer_cnt)\n"
  )
  assert "bc_dim_customer_key" not in sql
  assert "customer, person_type_code" not in sql
