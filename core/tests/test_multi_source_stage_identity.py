"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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

from metadata.models import (
  System,
  SourceDataset,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
  TargetSchema,
  TargetDataset,
  TargetDatasetInput,
  TargetColumn,
)
from metadata.rendering.builder import build_logical_select_for_target
from metadata.rendering.logical_plan import LogicalUnion, LogicalSelect, SubquerySource


@pytest.mark.django_db
def test_stage_union_injects_source_identity_id_per_upstream():
  """
  For a stage dataset with two RAW upstream datasets that each belong to a
  SourceDatasetGroupMembership with a configured source_identity_id, the
  logical plan must:

    - be a LogicalUnion with two SELECT branches
    - emit the correct literal value for source_identity_id per branch
      (e.g. 'SAP' for the SAP RAW, 'NAV_01' for the NAV RAW).
  """

  # ---------------------------------------------------------------------------
  # 1) Target schemas: raw + stage
  # ---------------------------------------------------------------------------
  raw_schema, _ = TargetSchema.objects.get_or_create(
    short_name="raw",
    defaults={
      "display_name": "Raw",
      "database_name": "dw",
      "schema_name": "raw",
    },
  )
  stage_schema, _ = TargetSchema.objects.get_or_create(
    short_name="stage",
    defaults={
      "display_name": "Stage",
      "database_name": "dw",
      "schema_name": "stage",
    },
  )

  # ---------------------------------------------------------------------------
  # 2) Source systems + source datasets (SAP + NAV)
  # ---------------------------------------------------------------------------
  sap_system = System.objects.create(
    short_name="sap",
    name="SAP ERP",
    type="db",           # choices are not enforced at save(), only by validation
    target_short_name="sap",
  )
  nav_system = System.objects.create(
    short_name="nav",
    name="Navision",
    type="db",
    target_short_name="nav",
  )

  sap_src = SourceDataset.objects.create(
    source_system=sap_system,
    schema_name="sap",
    source_dataset_name="customer",
  )
  nav_src = SourceDataset.objects.create(
    source_system=nav_system,
    schema_name="nav",
    source_dataset_name="customer",
  )

  # ---------------------------------------------------------------------------
  # 3) Group both sources into one SourceDatasetGroup with identity ids
  # ---------------------------------------------------------------------------
  group = SourceDatasetGroup.objects.create(
    target_short_name="cust",
    unified_source_dataset_name="customer",
  )

  sap_member = SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=sap_src,
    is_primary_system=True,
  )
  nav_member = SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=nav_src,
    is_primary_system=False,
  )

  # These attributes must exist in your updated model
  sap_member.source_identity_id = "SAP"
  sap_member.save()

  nav_member.source_identity_id = "NAV_01"
  nav_member.save()

  # ---------------------------------------------------------------------------
  # 4) RAW target datasets fed by the source datasets
  # ---------------------------------------------------------------------------
  sap_raw = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="sap_customer_raw",
  )
  nav_raw = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="nav_customer_raw",
  )

  # RAW input links: RAW <- SourceDataset
  TargetDatasetInput.objects.create(
    target_dataset=sap_raw,
    source_dataset=sap_src,
    role="primary",
  )
  TargetDatasetInput.objects.create(
    target_dataset=nav_raw,
    source_dataset=nav_src,
    role="primary",
  )

  # RAW columns (same shape in both systems for this test)
  for raw_ds in (sap_raw, nav_raw):
    TargetColumn.objects.create(
      target_dataset=raw_ds,
      target_column_name="customer_id",
      ordinal_position=1,
      business_key_column=True,
    )
    TargetColumn.objects.create(
      target_dataset=raw_ds,
      target_column_name="name",
      ordinal_position=2,
    )
    TargetColumn.objects.create(
      target_dataset=raw_ds,
      target_column_name="updated_at",
      ordinal_position=3,
    )

  # ---------------------------------------------------------------------------
  # 5) Stage dataset combining both RAWs via UNION ALL
  # ---------------------------------------------------------------------------
  stage_ds = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_customer",
    combination_mode="union",  # builder relies on upstreams + schema_short
  )

  # Stage input links: STAGE <- RAW
  TargetDatasetInput.objects.create(
    target_dataset=stage_ds,
    upstream_target_dataset=sap_raw,
    role="primary",
  )
  TargetDatasetInput.objects.create(
    target_dataset=stage_ds,
    upstream_target_dataset=nav_raw,
    role="primary",
  )

  # Stage columns: identity + business key + payload columns
  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="source_identity_id",
    ordinal_position=1,
    business_key_column=True,
  )
  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="customer_id",
    ordinal_position=2,
    business_key_column=True,
  )
  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="name",
    ordinal_position=3,
  )
  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="updated_at",
    ordinal_position=4,
  )

  # ---------------------------------------------------------------------------
  # 6) Build logical plan and assert behavior
  # ---------------------------------------------------------------------------
  plan = build_logical_select_for_target(stage_ds)

  # Top-level plan must now be a LogicalUnion
  assert isinstance(plan, LogicalUnion)
  assert len(plan.selects) == 2
  assert all(isinstance(sel, LogicalSelect) for sel in plan.selects)

  # Helper to find the expression for source_identity_id in a branch SELECT
  def get_identity_expr(logical_select: LogicalSelect):
    items = [
      item
      for item in logical_select.select_list
      if item.alias == "source_identity_id"
    ]
    assert len(items) == 1, (
      "Each UNION branch must have exactly one select item aliased as "
      "'source_identity_id'."
    )
    return items[0].expr

  first_expr = get_identity_expr(plan.selects[0])
  second_expr = get_identity_expr(plan.selects[1])

  first_sql = getattr(first_expr, "sql", None)
  second_sql = getattr(second_expr, "sql", None)

  # We do not rely on the order of selects being SAP first / NAV second,
  # so we compare as an unordered set of expected literals.
  actual_identities = {first_sql, second_sql}
  expected_identities = {"'SAP'", "'NAV_01'"}

  assert actual_identities == expected_identities
