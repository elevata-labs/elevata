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
  TargetSchema,
  TargetDataset,
  TargetDatasetInput,
  TargetColumn,
  TargetColumnInput,
  TargetDatasetReference,
  TargetDatasetReferenceComponent,
)
from metadata.rendering.builder import build_logical_select_for_target
from metadata.rendering.expr import Concat, ColumnRef, Literal


@pytest.mark.django_db
def test_fk_hash_matches_parent_surrogate_key():
  """
  Ensure that the FK hash expression on the child RawCore dataset
  is a rewritten version of the parent SK expression:

    parent SK: concat(col("bk1"), "~", col("bk1"), "|", col("bk2"), "~", col("bk2"))
    child FK:  concat(col("bk1"), "~", col("child_bk1"), "|", col("bk2"), "~", col("child_bk2"))

  This guarantees:
    - left component (BK name) is always the parent Stage BK column name
    - right component is the child Stage expression
    - component order is identical to the parent SK
  """

  # ---------------------------------------------------------------------------
  # 1) Create TargetSchemas for stage and rawcore
  # ---------------------------------------------------------------------------
  stage_schema, _ = TargetSchema.objects.get_or_create(
    short_name="stage",
    defaults={
      "display_name": "Stage",
      "database_name": "dw",
      "schema_name": "stage",
    },
  )

  rawcore_schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "display_name": "Raw Core",
      "database_name": "dw",
      "schema_name": "rawcore",
    },
  )

  # ---------------------------------------------------------------------------
  # 2) Parent datasets: Stage + RawCore
  # ---------------------------------------------------------------------------
  parent_stage = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_parent",
  )
  parent_rawcore = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rawcore_parent",
  )

  # RawCore parent is built from Stage parent
  TargetDatasetInput.objects.create(
    target_dataset=parent_rawcore,
    upstream_target_dataset=parent_stage,
    role="primary",
  )

  # Parent Stage BK columns
  parent_bk1_stage = TargetColumn.objects.create(
    target_dataset=parent_stage,
    target_column_name="bk1",
    ordinal_position=1,
    system_role="business_key",
  )
  parent_bk2_stage = TargetColumn.objects.create(
    target_dataset=parent_stage,
    target_column_name="bk2",
    ordinal_position=2,
    system_role="business_key",
  )

  # Parent RawCore BK columns (same names, so SK expression can reference "bk1", "bk2")
  parent_bk1_raw = TargetColumn.objects.create(
    target_dataset=parent_rawcore,
    target_column_name="bk1",
    ordinal_position=1,
    system_role="business_key",
  )
  parent_bk2_raw = TargetColumn.objects.create(
    target_dataset=parent_rawcore,
    target_column_name="bk2",
    ordinal_position=2,
    system_role="business_key",
  )

  # Parent SK column: uses Stage BK names in the expression
  parent_sk = TargetColumn.objects.create(
    target_dataset=parent_rawcore,
    target_column_name="parent_key",
    ordinal_position=3,
    system_role = "surrogate_key",
    surrogate_expression=(
      'concat(col("bk1"), "~", col("bk1"), "|", col("bk2"), "~", col("bk2"))'
    ),
  )

  # ---------------------------------------------------------------------------
  # 3) Child datasets: Stage + RawCore
  # ---------------------------------------------------------------------------
  child_stage = TargetDataset.objects.create(
    target_schema=stage_schema,
    target_dataset_name="stg_child",
  )
  child_rawcore = TargetDataset.objects.create(
    target_schema=rawcore_schema,
    target_dataset_name="rawcore_child",
  )

  # RawCore child is built from Stage child
  TargetDatasetInput.objects.create(
    target_dataset=child_rawcore,
    upstream_target_dataset=child_stage,
    role="primary",
  )

  # Child Stage BK columns (different names on purpose)
  child_bk1_stage = TargetColumn.objects.create(
    target_dataset=child_stage,
    target_column_name="child_bk1",
    ordinal_position=1,
  )
  child_bk2_stage = TargetColumn.objects.create(
    target_dataset=child_stage,
    target_column_name="child_bk2",
    ordinal_position=2,
  )

  # Child RawCore BK columns fed from Stage (lineage for FK hashing)
  child_bk1_raw = TargetColumn.objects.create(
    target_dataset=child_rawcore,
    target_column_name="child_bk1",
    ordinal_position=1,
  )
  child_bk2_raw = TargetColumn.objects.create(
    target_dataset=child_rawcore,
    target_column_name="child_bk2",
    ordinal_position=2,
  )

  TargetColumnInput.objects.create(
    target_column=child_bk1_raw,
    upstream_target_column=child_bk1_stage,
  )
  TargetColumnInput.objects.create(
    target_column=child_bk2_raw,
    upstream_target_column=child_bk2_stage,
  )

  # Child FK column uses standard naming convention
  from metadata.generation.naming import build_surrogate_key_name
  fk_name = build_surrogate_key_name(parent_rawcore.target_dataset_name)

  child_fk = TargetColumn.objects.create(
      target_dataset=child_rawcore,
      target_column_name=fk_name,
      ordinal_position=3,
  )

  # ---------------------------------------------------------------------------
  # 4) Reference + components: child BKs → parent BKs
  # ---------------------------------------------------------------------------
  ref = TargetDatasetReference.objects.create(
    referencing_dataset=child_rawcore,
    referenced_dataset=parent_rawcore,
  )

  TargetDatasetReferenceComponent.objects.create(
    reference=ref,
    from_column=child_bk1_raw,
    to_column=parent_bk1_raw,
    ordinal_position=1,
  )
  TargetDatasetReferenceComponent.objects.create(
    reference=ref,
    from_column=child_bk2_raw,
    to_column=parent_bk2_raw,
    ordinal_position=2,
  )

  # ---------------------------------------------------------------------------
  # 5) Build logical select for the child RawCore dataset
  # ---------------------------------------------------------------------------
  plan = build_logical_select_for_target(child_rawcore)

  from metadata.generation.naming import build_surrogate_key_name
  fk_name = build_surrogate_key_name(parent_rawcore.target_dataset_name)

  fk_items = [item for item in plan.select_list if item.alias == fk_name]
  assert fk_items, f"Expected FK select item '{fk_name}' in logical plan"

  fk_expr = fk_items[0].expr

  # We expect a Concat AST with the structure:
  #   col("bk1"), "~", col("child_bk1"), "|", col("bk2"), "~", col("child_bk2")
  assert isinstance(fk_expr, Concat)
  parts = fk_expr.parts

  assert len(parts) == 7

  assert isinstance(parts[0], ColumnRef)
  assert parts[0].column_name == "bk1"

  assert isinstance(parts[1], Literal)
  assert parts[1].value == "~"

  assert isinstance(parts[2], ColumnRef)
  assert parts[2].column_name == "child_bk1"

  assert isinstance(parts[3], Literal)
  assert parts[3].value == "|"

  assert isinstance(parts[4], ColumnRef)
  assert parts[4].column_name == "bk2"

  assert isinstance(parts[5], Literal)
  assert parts[5].value == "~"

  assert isinstance(parts[6], ColumnRef)
  assert parts[6].column_name == "child_bk2"