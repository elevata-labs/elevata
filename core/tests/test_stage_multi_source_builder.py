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
Tests for multi-source STAGE behavior in the logical plan builder:

- Multi-source STAGE via RAW upstream target datasets (identity-mode)
- Multi-source STAGE via direct SourceDataset inputs (identity-mode)
- Single-source STAGE remains a simple LogicalSelect
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
from metadata.rendering.logical_plan import LogicalSelect, LogicalUnion, SubquerySource
from metadata.rendering.expr import RawSql


@pytest.mark.django_db
def test_stage_multi_source_via_raw_identity_mode_produces_union():
  """
  STAGE with two RAW upstream target datasets and source_identity_id configured
  per source should result in a LogicalUnion with two branches, and each branch
  must provide a literal expression for source_identity_id.
  """

  # --- Target schemas ---
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

  # --- System and source datasets ---
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

  # Minimal source columns so RAW has something to project
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

  # --- Group with identities ---
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

  # --- RAW target datasets and lineage to source ---
  raw1 = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_aw1_person",
  )
  raw2 = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_aw2_person",
  )

  TargetColumn.objects.create(
    target_dataset=raw1,
    target_column_name="businessentityid",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    system_role="business_key",
  )
  TargetColumn.objects.create(
    target_dataset=raw2,
    target_column_name="businessentityid",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    system_role="business_key",
  )

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

  # --- STAGE dataset with two RAW upstreams ---
  stage_ds = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_aw_person",
    combination_mode="union",
  )

  # Inputs: STAGE reads from both RAW datasets
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

  # STAGE columns including source_identity_id
  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="source_identity_id",
    ordinal_position=1,
    datatype="STRING",
    max_length=30,
    nullable=False,
    system_role="business_key",
    artificial_column=True,
  )
  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="businessentityid",
    ordinal_position=2,
    datatype="STRING",
    max_length=50,
    nullable=False,
    system_role="business_key",
  )

  logical = build_logical_select_for_target(stage_ds)

  assert isinstance(logical, LogicalUnion), (
    "Expected a LogicalUnion for STAGE with two RAW upstream datasets "
    "in identity mode."
  )
  assert len(logical.selects) == 2, (
    f"Expected two UNION branches, got {len(logical.selects)}"
  )

  literals = []
  for sel in logical.selects:
    items = [si for si in sel.select_list if si.alias == "source_identity_id"]
    assert items, "Each branch must project source_identity_id"
    expr = items[0].expr
    assert isinstance(expr, RawSql), (
      "source_identity_id expression must be a RawSql literal expression."
    )
    literals.append(expr.sql)

  assert any("aw1" in lit for lit in literals), (
    f"Expected one branch to use 'aw1', got {literals}"
  )
  assert any("aw2" in lit for lit in literals), (
    f"Expected one branch to use 'aw2', got {literals}"
  )
  assert literals[0] != literals[1], (
    f"Both branches use the same literal for source_identity_id: {literals}"
  )


@pytest.mark.django_db
def test_stage_multi_source_via_sources_identity_mode_produces_union():
  """
  STAGE with two direct SourceDataset inputs (no RAW layer) and
  source_identity_id configured via group memberships must also produce
  a LogicalUnion with literal expressions for source_identity_id.
  """

  stage_schema, _ = TargetSchema.objects.get_or_create(
    short_name="stage",
    defaults={
      "display_name": "Stage",
      "database_name": "dw",
      "schema_name": "stage",
    },
  )

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

  stage_ds = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_aw_person",
    combination_mode="union",
  )

  # Direct source inputs (no RAW)
  TargetDatasetInput.objects.create(
    target_dataset=stage_ds,
    source_dataset=src1,
    role="primary",
  )
  TargetDatasetInput.objects.create(
    target_dataset=stage_ds,
    source_dataset=src2,
    role="primary",
  )

  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="source_identity_id",
    ordinal_position=1,
    datatype="STRING",
    max_length=30,
    nullable=False,
    system_role="business_key",
    artificial_column=True,
  )
  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="BusinessEntityID",
    ordinal_position=2,
    datatype="STRING",
    max_length=50,
    nullable=False,
    system_role="business_key",
  )

  logical = build_logical_select_for_target(stage_ds)

  assert isinstance(logical, LogicalUnion), (
    "Expected a LogicalUnion for STAGE with multiple direct SourceDataset "
    "inputs in identity mode."
  )
  assert len(logical.selects) == 2, (
    f"Expected two UNION branches, got {len(logical.selects)}"
  )

  literals = []
  for sel in logical.selects:
    items = [si for si in sel.select_list if si.alias == "source_identity_id"]
    assert items, "Each branch must project source_identity_id"
    expr = items[0].expr
    assert isinstance(expr, RawSql), (
      "source_identity_id expression must be a RawSql literal expression."
    )
    literals.append(expr.sql)

  assert any("aw1" in lit for lit in literals), (
    f"Expected one branch to use 'aw1', got {literals}"
  )
  assert any("aw2" in lit for lit in literals), (
    f"Expected one branch to use 'aw2', got {literals}"
  )


@pytest.mark.django_db
def test_stage_single_source_returns_logical_select():
  """
  STAGE with exactly one upstream (RAW or SourceDataset) must fall back
  to the single-path logic and return a LogicalSelect rather than a UNION.
  """

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

  system = System.objects.create(
    short_name="aw",
    name="AdventureWorks",
    type="db",
    is_source=True,
    is_target=False,
    target_short_name="aw",
  )

  src = SourceDataset.objects.create(
    source_system=system,
    schema_name="Person",
    source_dataset_name="Person_single",
    integrate=True,
  )

  SourceColumn.objects.create(
    source_dataset=src,
    source_column_name="BusinessEntityID",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    primary_key_column=True,
  )

  raw_ds = TargetDataset.objects.create(
    target_schema=raw_schema,
    target_dataset_name="raw_single_person",
  )

  TargetColumn.objects.create(
    target_dataset=raw_ds,
    target_column_name="businessentityid",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    system_role="business_key",
  )

  TargetDatasetInput.objects.create(
    target_dataset=raw_ds,
    source_dataset=src,
    role="primary",
  )

  stage_ds = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_single_person",
    combination_mode="union",
  )

  TargetDatasetInput.objects.create(
    target_dataset=stage_ds,
    upstream_target_dataset=raw_ds,
    role="primary",
  )

  TargetColumn.objects.create(
    target_dataset=stage_ds,
    target_column_name="businessentityid",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    system_role="business_key",
  )

  logical = build_logical_select_for_target(stage_ds)

  assert isinstance(logical, LogicalSelect), (
    "Expected a LogicalSelect for STAGE with only one upstream dataset."
  )
