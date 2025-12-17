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
import re

from metadata.models import (
  System,
  SourceDataset,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
  TargetSchema,
)
from metadata.generation.target_generation_service import TargetGenerationService


def _order_of_components(expr: str, colnames: list[str]) -> list[int]:
  """
  Return the index positions of each column name within the expression.
  We look for "'<col>'" or "expr:<col>".
  """
  positions = []

  for name in colnames:
    # Try literal "'name'"
    m = re.search(rf"'{re.escape(name)}'", expr)
    if not m:
      # Fallback: expr:name
      m = re.search(rf"expr:{re.escape(name)}", expr)
    assert m is not None, f"Column {name!r} not found in expression: {expr}"
    positions.append(m.start())

  return positions


@pytest.mark.django_db
def test_rawcore_surrogate_key_expression_contains_identity_and_is_alphabetical():
  """
  The RAWCORE surrogate key expression must include source_identity_id and
  must sort all natural key columns alphabetically inside the hash expression.
  """

  # ---------------------------------------------------------------------------
  # 1) RAWCORE schema
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
  # 2) Source dataset & group
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
  # 3) Create bundle
  # ---------------------------------------------------------------------------
  svc = TargetGenerationService()
  bundle = svc.build_dataset_bundle(src, rawcore_schema)
  cols = bundle["columns"]

  # ---------------------------------------------------------------------------
  # 4) Find SK column
  # ---------------------------------------------------------------------------
  sk_cols = [c for c in cols if getattr(c, "system_role", "") == "surrogate_key"]
  assert sk_cols, "RAWCORE bundle must contain a surrogate key column"
  
  sk = sk_cols[0]
  expr = sk.surrogate_expression

  # ---------------------------------------------------------------------------
  # 5) Verify SK expression
  # ---------------------------------------------------------------------------
  assert isinstance(expr, str) and expr, "SK expression must be non-empty"

  # Identity must be contained
  assert "source_identity_id" in expr, (
    "source_identity_id must appear in the SK expression"
  )

  # Check alphabetic order
  #
  # For this test we assume that our test RAWCORE dataset
  # has exactly 1 real BK column (sourceIdentity) + 0..N others.
  # To make that generic, we read all BKs from bundle:
  bk_cols = [c.target_column_name for c in cols if c.system_role == "business_key"]

  # Alphabetisch sortierte Liste
  expected_sorted = sorted(bk_cols)

  # Determine index position of each BK column in the expression
  positions = _order_of_components(expr, expected_sorted)

  # The order in the expression must fit to the alphabetic order
  assert positions == sorted(positions), (
    "Natural key columns must appear alphabetically in the SK expression. "
    f"Expected order {expected_sorted}, but positions are {positions}. "
    f"Expression: {expr}"
  )
