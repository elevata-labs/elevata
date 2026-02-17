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

import pytest

from tests._dialect_test_mixin import DialectTestMixin


@pytest.mark.django_db
def test_missing_table_does_not_plan_add_column(monkeypatch):
  """
  Smoke test: if introspection says table does not exist, the planner must NOT emit
  NO_COLUMNS_RETURNED / ADD_COLUMN spam (which would crash on MSSQL/Fabric).
  Instead it should warn MISSING_TABLE and return without column DDL.
  """
  # Imports adjusted to your project structure (update if needed)
  from metadata.materialization.planner import build_materialization_plan
  from metadata.materialization.policy import MaterializationPolicy
  from metadata.models import TargetDataset, TargetSchema, TargetColumn

  # Create a minimal dataset with desired columns
  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="smoke_missing_table",
    incremental_strategy="full",
    is_system_managed=False,
  )
  # One desired column is enough
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="id",
    datatype="integer",
    active=True,
    ordinal_position=1,
    is_system_managed=False,
  )


  class DummyDialect(DialectTestMixin):
    def introspect_table(self, *, schema_name, table_name, introspection_engine, exec_engine=None, debug_plan=False):
      # The crux: missing table must be reported as table_exists=False
      return {"table_exists": False, "actual_cols_by_norm_name": {}}

  dialect = DummyDialect()

  policy = MaterializationPolicy(
    sync_schema_shorts=set(["rawcore"]),  # allow planning
    debug_plan=False,
    allow_auto_drop_columns=False,
    allow_type_alter=False,
  )

  plan = build_materialization_plan(
    td=td,
    introspection_engine=object(),
    exec_engine=None,
    dialect=dialect,
    policy=policy,
  )

  # Planner should detect missing table and avoid ADD_COLUMN plans
  steps_ops = [getattr(s, "op", None) for s in (plan.steps or [])]
  warnings = list(plan.warnings or [])

  assert not any(op == "ADD_COLUMN" for op in steps_ops), f"Unexpected ADD_COLUMN steps: {steps_ops}"
  assert not any(str(w).startswith("NO_COLUMNS_RETURNED:") for w in warnings), f"Unexpected NO_COLUMNS_RETURNED: {warnings}"
  assert any(str(w).startswith("MISSING_TABLE:") for w in warnings), f"Expected MISSING_TABLE warning, got: {warnings}"


import pytest


@pytest.mark.django_db
def test_existing_table_with_no_columns_plans_add_column(monkeypatch):
  """
  Smoke test: if introspection says table exists but returns no columns,
  the planner should emit NO_COLUMNS_RETURNED and plan ADD_COLUMN for desired columns.
  This is the intended best-effort behavior for edge-case introspection.
  """
  from metadata.materialization.planner import build_materialization_plan
  from metadata.materialization.policy import MaterializationPolicy
  from metadata.models import TargetDataset, TargetSchema, TargetColumn

  schema = TargetSchema.objects.get_or_create(short_name="rawcore", schema_name="rawcore")[0]
  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="smoke_existing_no_cols",
    incremental_strategy="full",
    is_system_managed=False,
  )
  TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="id",
    datatype="integer",
    active=True,
    ordinal_position=1,
    is_system_managed=False,
  )

  class DummyDialect(DialectTestMixin):
    def introspect_table(self, *, schema_name, table_name, introspection_engine, exec_engine=None, debug_plan=False):
      # The crux: table exists, but engine returns no columns (quirk / permission / bug)
      return {"table_exists": True, "actual_cols_by_norm_name": {}}

  dialect = DummyDialect()

  policy = MaterializationPolicy(
    sync_schema_shorts=set(["rawcore"]),
    debug_plan=False,
    allow_auto_drop_columns=False,
    allow_type_alter=False,    
  )

  plan = build_materialization_plan(
    td=td,
    introspection_engine=object(),
    exec_engine=None,
    dialect=dialect,
    policy=policy,
  )

  steps_ops = [getattr(s, "op", None) for s in (plan.steps or [])]
  warnings = list(plan.warnings or [])

  assert any(str(w).startswith("NO_COLUMNS_RETURNED:") for w in warnings), warnings
  assert any(op == "ADD_COLUMN" for op in steps_ops), steps_ops
