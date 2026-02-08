"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

This file is part of elevata.

elevata is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

elevata is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with elevata. If not, see <https://www.gnu.org/licenses/>.

Contact: <https://github.com/elevata-labs/elevata>.
"""

import uuid
import pytest

from metadata.models import TargetDataset, TargetSchema, TargetColumn
from metadata.rendering.dialects.bigquery import BigQueryDialect
from metadata.rendering.dialects.databricks import DatabricksDialect
from metadata.rendering.dialects.duckdb import DuckDBDialect
from metadata.rendering.dialects.fabric_warehouse import FabricWarehouseDialect
from metadata.rendering.dialects.mssql import MssqlDialect
from metadata.rendering.dialects.postgres import PostgresDialect
from metadata.rendering.dialects.snowflake import SnowflakeDialect

from metadata.materialization.planner import build_materialization_plan
from metadata.materialization.policy import MaterializationPolicy

@pytest.mark.parametrize(
  "dialect_cls, expected",
  [
    (MssqlDialect, {
      "load_run_id": "nvarchar(64)",
      "version_state": "nvarchar(20)",
      "row_hash": "nvarchar(64)",
    }),
    (PostgresDialect, {
      "load_run_id": "varchar(64)",
      "version_state": "varchar(20)",
      "row_hash": "varchar(64)",
    }),
    (DuckDBDialect, {
      "load_run_id": "varchar(64)",
      "version_state": "varchar(20)",
      "row_hash": "varchar(64)",
    }),
    (BigQueryDialect, {
      "load_run_id": "string",
      "version_state": "string",
      "row_hash": "string",
    }),
    (DatabricksDialect, {
      "load_run_id": "string",
      "version_state": "string",
      "row_hash": "string",
    }),
    (SnowflakeDialect, {
      "load_run_id": "varchar(64)",
      "version_state": "varchar(20)",
      "row_hash": "varchar(64)",
    }),
    (FabricWarehouseDialect, {
      "load_run_id": "varchar(64)",
      "version_state": "varchar(20)",
      "row_hash": "varchar(64)",
    }),
  ],
)
@pytest.mark.django_db
def test_render_create_table_respects_tech_column_lengths(dialect_cls, expected):
  # Reuse the same schema across param runs (short_name is UNIQUE).
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )

  # Make dataset name unique per param run (safer if (schema, dataset_name) is unique).
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_test_{dialect_cls.__name__.lower()}",
    is_system_managed=True,
  )

  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="load_run_id",
    datatype="STRING",
    max_length=64,
    nullable=False,
    system_role="load_run_id",
    ordinal_position=1,
  )

  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="version_state",
    datatype="STRING",
    max_length=20,
    nullable=False,
    system_role="version_state",
    ordinal_position=2,
  )

  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="row_hash",
    datatype="STRING",
    max_length=64,
    nullable=False,
    system_role="row_hash",
    ordinal_position=3,
  )

  dialect = dialect_cls()
  ddl = (dialect.render_create_table_if_not_exists(td) or "").lower()

  for col, type_snippet in expected.items():
    assert f"{col} {type_snippet}" in ddl


@pytest.mark.parametrize(
  "dialect_cls",
  [
    MssqlDialect, PostgresDialect, DuckDBDialect,
    BigQueryDialect, DatabricksDialect, SnowflakeDialect, FabricWarehouseDialect,
  ],
)

@pytest.mark.django_db
def test_render_create_table_types_match_map_logical_type(dialect_cls):
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )

  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name=f"rc_test_{dialect_cls.__name__.lower()}_{uuid.uuid4().hex[:8]}",
    is_system_managed=True,
  )

  # Representative tech-ish columns where we previously saw drift.
  cols = [
    ("load_run_id", "STRING", 64, False, "load_run_id", 1),
    ("version_state", "STRING", 20, False, "version_state", 2),
    ("row_hash", "STRING", 64, False, "row_hash", 3),
  ]

  for name, dtype, max_len, nullable, role, ord_pos in cols:
    TargetColumn.objects.create(
      target_dataset=td,
      target_column_name=name,
      datatype=dtype,
      max_length=max_len,
      nullable=nullable,
      system_role=role,
      ordinal_position=ord_pos,
    )

  dialect = dialect_cls()

  ddl = (dialect.render_create_table_if_not_exists(td) or "").lower()

  # Assert: the CREATE TABLE renderer uses the same type mapping as map_logical_type().
  for c in td.target_columns.filter(active=True).order_by("ordinal_position", "id"):
    expected = (dialect.map_logical_type(
      datatype=c.datatype,
      max_length=getattr(c, "max_length", None),
      precision=getattr(c, "decimal_precision", None),
      scale=getattr(c, "decimal_scale", None),
      strict=True,
    ) or "").lower()

    assert expected, f"map_logical_type returned empty type for {dialect_cls.__name__}.{c.target_column_name}"
    assert f"{c.target_column_name.lower()} {expected}" in ddl, (
      f"DDL type drift for {dialect_cls.__name__}.{c.target_column_name}: "
      f"expected '{expected}' to appear in DDL.\nDDL:\n{ddl}"
    )


@pytest.mark.django_db
def test_postgres_timestamp_vs_timestamptz_is_not_reported_as_mismatch(monkeypatch):
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={"schema_name": "rawcore"},
  )
  # In case an earlier test created it without schema_name
  if not schema.schema_name:
    schema.schema_name = "rawcore"
    schema.save(update_fields=["schema_name"])

  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="rc_test_tz_equivalence",
    is_system_managed=True,
  )

  # Metadata says TIMESTAMP -> desired=timestamptz
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="loaded_at",
    datatype="TIMESTAMP",
    nullable=False,
    system_role="loaded_at",
    ordinal_position=1,
    active=True,
  )

  dialect = PostgresDialect()

  # Fake introspection via the new dialect hook: actual column type is "timestamp"
  def fake_introspect_table(
    self,
    *,
    schema_name: str,
    table_name: str,
    introspection_engine,
    exec_engine=None,
    debug_plan: bool = False,
  ):
    return {
      "table_exists": True,
      "physical_table": table_name,
      "actual_cols_by_norm_name": {"loaded_at": {"name": "loaded_at", "type": "timestamp"}},
    }

  monkeypatch.setattr(PostgresDialect, "introspect_table", fake_introspect_table, raising=True)
 
  policy = MaterializationPolicy(
    sync_schema_shorts={"rawcore"},
    allow_auto_drop_columns=False,
    allow_type_alter=False,
  )

  plan = build_materialization_plan(
    td=td,
    introspection_engine=object(),
    dialect=dialect,
    policy=policy,
  )

  assert not any(
    "Type mismatch" in w and "loaded_at" in w
    for w in (plan.warnings or [])
  )
