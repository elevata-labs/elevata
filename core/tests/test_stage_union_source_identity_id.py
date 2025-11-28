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

"""
Test for multi-source STAGE UNION:

- A STAGE TargetDataset with two RAW upstream datasets should produce
  a LogicalUnion.
- Each branch in the UNION must populate the column `source_identity_id`
  with a source-specific literal, e.g. 'aw1' and 'aw2'.
- This ensures that stage-union handling provides source identity lineage
  across multiple upstream systems.
"""

import pytest

from metadata.models import (
  System,
  SourceDataset,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
  SourceColumn,
  TargetSchema,
  TargetDataset,
  TargetDatasetInput,
  TargetColumn,
)
from metadata.rendering.builder import build_logical_select_for_target
from metadata.rendering.logical_plan import LogicalUnion


@pytest.mark.django_db
def test_stage_union_sets_source_identity_id_literal_per_branch():
  """
  A STAGE dataset fed by two RAW datasets should produce a LogicalUnion,
  and each branch must contain a literal expression for `source_identity_id`
  corresponding to the source's identity id (e.g. 'aw1', 'aw2').
  """

  # ---------------------------------------------------------------------------
  # 1) Target schemas for RAW and STAGE
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
  # 2) Source system and two source datasets
  # ---------------------------------------------------------------------------
  system = System.objects.create(
    short_name="aw",
    name="AdventureWorks",
    type="db",
    is_source=True,
    is_target=False,
    target_short_name="aw",
  )

  src1 = SourceDataset.objects.create(
    source_system=system,
    schema_name="Person",
    source_dataset_name="Person_aw1",
    integrate=True,
  )

  src2 = SourceDataset.objects.create(
    source_system=system,
    schema_name="Person",
    source_dataset_name="Person_aw2",
    integrate=True,
  )

  # Add two business key source columns so the RAW datasets receive columns
  SourceColumn.objects.create(
    source_dataset=src1,
    source_column_name="BusinessEntityID",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    primary_key_column=True,
  )
  SourceColumn.objects.create(
    source_dataset=src2,
    source_column_name="BusinessEntityID",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    primary_key_column=True,
  )

  # ---------------------------------------------------------------------------
  # 3) Group with two memberships and identity ids
  # ---------------------------------------------------------------------------
  group = SourceDatasetGroup.objects.create(
    target_short_name="aw_person",
    unified_source_dataset_name="Person",
  )

  m1 = SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=src1,
    is_primary_system=True,
  )
  m1.source_identity_id = "aw1"
  m1.save()

  m2 = SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=src2,
    is_primary_system=False,
  )
  m2.source_identity_id = "aw2"
  m2.save()

  # ---------------------------------------------------------------------------
  # 4) RAW TargetDatasets + columns + lineage links to SourceDataset
  # ---------------------------------------------------------------------------
  raw1 = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_aw1_person",
  )
  raw2 = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_aw2_person",
  )

  # Map BK column to both RAW datasets
  TargetColumn.objects.create(
    target_dataset=raw1,
    target_column_name="businessentityid",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    business_key_column=True,
  )
  TargetColumn.objects.create(
    target_dataset=raw2,
    target_column_name="businessentityid",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    business_key_column=True,
  )

  # Connect RAW → Source
  TargetDatasetInput.objects.create(
    target_dataset=raw1,
    source_dataset=src1,
    role="primary",
  )
  TargetDatasetInput.objects.create(
    target_dataset=raw2,
    source_dataset=src2,
    role="primary",
  )

  # ---------------------------------------------------------------------------
  # 5) STAGE TargetDataset with both RAW datasets as upstream inputs
  # ---------------------------------------------------------------------------
  stage_ds = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_aw_person",
    combination_mode="union",
  )

  TargetDatasetInput.objects.create(
    target_dataset=stage_ds,
    upstream_target_dataset=raw1,
    role="primary",
  )
  TargetDatasetInput.objects.create(
    target_dataset=stage_ds,
    upstream_target_dataset=raw2,
    role="primary",
  )

  # STAGE columns including artificial source_identity_id
  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="source_identity_id",
    ordinal_position=1,
    datatype="STRING",
    max_length=30,
    nullable=False,
    business_key_column=True,
    artificial_column=True,
  )
  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="businessentityid",
    ordinal_position=2,
    datatype="STRING",
    max_length=50,
    nullable=False,
    business_key_column=True,
  )

  # ---------------------------------------------------------------------------
  # 6) Execute builder: expect a LogicalUnion with two branches
  # ---------------------------------------------------------------------------
  logical = build_logical_select_for_target(stage_ds)

  assert isinstance(logical, LogicalUnion), (
    "Expected build_logical_select_for_target to return a LogicalUnion "
    "for multi-source STAGE datasets."
  )
  assert len(logical.selects) == 2, (
    f"Expected exactly 2 UNION branches, got {len(logical.selects)}"
  )

  # ---------------------------------------------------------------------------
  # 7) Check source_identity_id literal per branch
  # ---------------------------------------------------------------------------
  branch_literals = []

  for sel in logical.selects:
    # Locate SelectItem with alias 'source_identity_id'
    items = [
      si for si in sel.select_list
      if si.alias == "source_identity_id"
    ]
    assert items, (
      "Each UNION branch must provide a select item for 'source_identity_id'"
    )

    expr = items[0].expr
    sql_literal = getattr(expr, "sql", None)

    assert sql_literal, (
      "source_identity_id expression must be a literal SQL string, "
      f"got: {expr!r}"
    )

    branch_literals.append(sql_literal)

  # Validate that each branch has the correct literal identity id
  assert any("aw1" in lit for lit in branch_literals), (
    f"Expected one branch to use identity 'aw1', got: {branch_literals}"
  )
  assert any("aw2" in lit for lit in branch_literals), (
    f"Expected one branch to use identity 'aw2', got: {branch_literals}"
  )
  assert branch_literals[0] != branch_literals[1], (
    f"Expected different identity literals per UNION branch, got: {branch_literals}"
  )
