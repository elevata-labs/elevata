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
Integration-like tests for automatic source_identity_id column generation:

- For a SourceDataset that is member of a SourceDatasetGroup with
  source_identity_id configured, the TargetGenerationService must add
  a synthetic business key column 'source_identity_id' to the drafts
  for both STAGE and RAWCORE schemas.

- The column must be a STRING, non-nullable, business_key_column=True
  and appear as the first natural key (ordinal_position=1) in the drafts.
"""

import pytest
import re

from metadata.models import (
  System,
  SourceDataset,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
  SourceColumn,
  TargetSchema,
)
from metadata.generation.target_generation_service import TargetGenerationService


@pytest.mark.django_db
def test_auto_identity_column_in_stage_and_rawcore_bundles():
  """
  For a source dataset that belongs to a group with source_identity_id,
  build_dataset_bundle must add a synthetic 'source_identity_id' column
  as the first natural key for both STAGE and RAWCORE.
  """

  # ---------------------------------------------------------------------------
  # 1) Basic schemas for stage and rawcore
  # ---------------------------------------------------------------------------
  stage_schema, _ = TargetSchema.objects.get_or_create(
    short_name="stage",
    defaults={
      "display_name": "Stage",
      "database_name": "dw",
      "schema_name": "stage",
      # surrogate keys are usually enabled here; adjust if needed in your model
      "surrogate_keys_enabled": True,
    },
  )

  rawcore_schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "display_name": "Rawcore",
      "database_name": "dw",
      "schema_name": "rawcore",
      "surrogate_keys_enabled": True,
    },
  )

  # ---------------------------------------------------------------------------
  # 2) Source system and dataset
  # ---------------------------------------------------------------------------
  system = System.objects.create(
    short_name="aw",
    name="AdventureWorks",
    type="db",            # adjust if your choices differ
    target_short_name="aw",
  )

  src = SourceDataset.objects.create(
    source_system=system,
    schema_name="Person",
    source_dataset_name="Person",
  )

  # ---------------------------------------------------------------------------
  # 3) Source group with identity id
  # ---------------------------------------------------------------------------
  group = SourceDatasetGroup.objects.create(
    target_short_name="aw_person",
    unified_source_dataset_name="Person",
  )

  membership = SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=src,
    is_primary_system=True,
  )
  membership.source_identity_id = "aw1"
  membership.save()

  # ---------------------------------------------------------------------------
  # 4) Build dataset bundles for STAGE and RAWCORE
  # ---------------------------------------------------------------------------
  svc = TargetGenerationService()

  stage_bundle = svc.build_dataset_bundle(
    source_dataset=src,
    target_schema=stage_schema,
  )
  rawcore_bundle = svc.build_dataset_bundle(
    source_dataset=src,
    target_schema=rawcore_schema,
  )

  stage_cols = stage_bundle["columns"]
  rawcore_cols = rawcore_bundle["columns"]

  # ---------------------------------------------------------------------------
  # 5) Helper to assert presence and properties of source_identity_id
  # ---------------------------------------------------------------------------
  def assert_identity_column(cols, schema_short: str, expected_bk_ordinal: int):
    id_cols = [
      c for c in cols if c.target_column_name == "source_identity_id"
    ]
    assert id_cols, (
      f"source_identity_id column must exist in {schema_short} bundle"
    )

    id_col = id_cols[0]

    # Datatype & length
    assert id_col.datatype.upper() in ("STRING", "VARCHAR", "CHAR"), (
      f"source_identity_id in {schema_short} must be a string type, "
      f"got {id_col.datatype!r}"
    )
    # We expect a reasonable max_length > 0
    assert id_col.max_length is not None and id_col.max_length > 0 and id_col.max_length <= 30, (
      f"source_identity_id in {schema_short} must have a max_length set"
    )

    # Business key and non-nullable
    assert id_col.business_key_column, (
      f"source_identity_id in {schema_short} must be a business key column"
    )
    assert not id_col.nullable, (
      f"source_identity_id in {schema_short} must be non-nullable"
    )

    # Ordinal: in STAGE first BK (1), in RAWCORE first BK after SK (2)
    bk_cols = [c for c in cols if c.business_key_column]
    ordinals = {c.target_column_name: c.ordinal_position for c in bk_cols}
    assert ordinals.get("source_identity_id") == expected_bk_ordinal, (
      f"source_identity_id in {schema_short} must have ordinal_position="
      f"{expected_bk_ordinal} among BK columns, got "
      f"{ordinals.get('source_identity_id')}, BK ordinals: {ordinals}"
    )

  # Assertions for both schemas
  assert_identity_column(stage_cols, "stage", expected_bk_ordinal=1)
  assert_identity_column(rawcore_cols, "rawcore", expected_bk_ordinal=2)


def _order_of_components(expr: str, colnames: list[str]) -> list[int]:
  """
  Return the index positions of each column name within the expression.
  We look for "'<colname>'" or "expr:<colname>".
  """
  positions: list[int] = []

  for name in colnames:
    # 1) Label-Literal, z.B. 'source_identity_id'
    m = re.search(rf"'{re.escape(name)}'", expr)
    if not m:
      # 2) Fallback: expr:source_identity_id
      m = re.search(rf"expr:{re.escape(name)}", expr)
    assert m is not None, f"Column {name!r} not found in expression: {expr}"
    positions.append(m.start())

  return positions


@pytest.mark.django_db
def test_rawcore_sk_expression_contains_identity_and_sorts_bks_alphabetically():
  """
  For a RAWCORE bundle with source_identity_id and at least one more 
  BK column this must be valid:

    - The surrogate expression of the SK column contains source_identity_id.
    - All natural key columns (inc. Identity) appear alphabetically sorted
      in the expression (not in ordinal_position order)
  """

  # ---------------------------------------------------------------------------
  # 1) RAWCORE-Schema
  # ---------------------------------------------------------------------------
  rawcore_schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "display_name": "Rawcore",
      "database_name": "dw",
      "schema_name": "rawcore",
      "surrogate_keys_enabled": True,
    },
  )

  # ---------------------------------------------------------------------------
  # 2) Source system & dataset
  # ---------------------------------------------------------------------------
  system = System.objects.create(
    short_name="aw",
    name="AdventureWorks",
    type="db",
    target_short_name="aw",
  )

  src = SourceDataset.objects.create(
    source_system=system,
    schema_name="Person",
    source_dataset_name="Person",
    integrate=True,
  )

  # Two BK SourceColumns, so that we can test a real order
  # eg. 'BusinessEntityID' and 'CustomerCode'
  SourceColumn.objects.create(
    source_dataset=src,
    source_column_name="BusinessEntityID",
    ordinal_position=1,
    datatype="STRING",
    max_length=50,
    nullable=False,
    primary_key_column=True,
  )
  SourceColumn.objects.create(
    source_dataset=src,
    source_column_name="CustomerCode",
    ordinal_position=2,
    datatype="STRING",
    max_length=50,
    nullable=False,
    primary_key_column=True,
  )

  # ---------------------------------------------------------------------------
  # 3) Group with source_identity_id
  # ---------------------------------------------------------------------------
  group = SourceDatasetGroup.objects.create(
    target_short_name="aw_person",
    unified_source_dataset_name="Person",
  )

  membership = SourceDatasetGroupMembership.objects.create(
    group=group,
    source_dataset=src,
    is_primary_system=True,
  )
  membership.source_identity_id = "aw1"
  membership.save()

  # ---------------------------------------------------------------------------
  # 4) Build bundle for RAWCORE
  # ---------------------------------------------------------------------------
  svc = TargetGenerationService()
  bundle = svc.build_dataset_bundle(src, rawcore_schema)
  cols = bundle["columns"]

  # ---------------------------------------------------------------------------
  # 5) get SK column & BK columns from bundle
  # ---------------------------------------------------------------------------
  sk_cols = [c for c in cols if getattr(c, "surrogate_key_column", False)]
  assert sk_cols, "RAWCORE bundle must contain a surrogate key column"

  sk = sk_cols[0]
  expr = sk.surrogate_expression

  assert isinstance(expr, str) and expr, (
    "surrogate_expression on rawcore SK column must be a non-empty string"
  )

  # Identity has to be part of the expression string
  assert "source_identity_id" in expr, (
    "source_identity_id must be part of the rawcore surrogate key expression, "
    f"got: {expr}"
  )

  # Collect all BK column names from the bundle (inc. source_identity_id)
  bk_names = [c.target_column_name for c in cols if c.business_key_column]
  # Expected alphabetic sort order
  expected_sorted = sorted(bk_names)

  # Determine positions of the BK names in expression
  positions = _order_of_components(expr, expected_sorted)

  # Sort order in expression must fit to alphabetic order
  assert positions == sorted(positions), (
    "Natural key columns must appear alphabetically in the SK expression. "
    f"Expected order {expected_sorted}, but positions are {positions}. "
    f"Expression: {expr}"
  )

