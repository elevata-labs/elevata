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
Tests for surrogate key hashing with multi-source scenarios:

- Natural key columns must be sorted alphabetically inside the
  surrogate expression, independent of the caller's order.
- source_identity_id, if present, must be part of the expression.
"""

import re

from metadata.generation.hashing import build_surrogate_expression
from metadata.generation.mappers import build_surrogate_key_column_draft


def _order_of_components(expr: str, colnames: list[str]) -> list[int]:
  """
  Return the index positions of each column name within the expression
  string. We look for the label literal '<col>' first, and fall back
  to 'expr:<col>' if needed.
  """
  positions: list[int] = []

  for name in colnames:
    # Try label: '<name>'
    m = re.search(rf"'{re.escape(name)}'", expr)
    if not m:
      # Fallback: expr:<name>
      m = re.search(rf"expr:{re.escape(name)}", expr)

    assert m is not None, f"Column {name!r} not found in expression: {expr}"
    positions.append(m.start())

  return positions


def test_build_surrogate_expression_sorts_natural_keys_alphabetically():
  """
  build_surrogate_expression must sort natural_key_cols alphabetically,
  regardless of the order in which they are passed in.
  """

  cols = ["customer_id", "source_identity_id", "company_code"]
  shuffled = ["company_code", "source_identity_id", "customer_id"]
  expected_sorted = sorted(cols)  # ['company_code', 'customer_id', 'source_identity_id']

  expr = build_surrogate_expression(
    natural_key_cols=shuffled,
    pepper="test_pepper",
    null_token="<NULL>",
    pair_sep="~",
    comp_sep="|",
  )

  # Determine the relative order of components in the expression
  positions = _order_of_components(expr, expected_sorted)

  # Positions must be strictly increasing in the sorted order
  assert positions == sorted(positions), (
    "Natural key columns must appear in alphabetical order in the "
    f"surrogate expression. Expected order {expected_sorted}, got positions "
    f"{positions} in expression: {expr}"
  )


def test_surrogate_key_column_draft_includes_identity_and_respects_sorting():
  """
  build_surrogate_key_column_draft must:

  - include source_identity_id in the surrogate_expression if it is part
    of the natural keys
  - keep the alphabetical ordering from build_surrogate_expression
  """

  cols = ["customer_id", "source_identity_id", "company_code"]
  expected_sorted = sorted(cols)

  sk_draft = build_surrogate_key_column_draft(
    target_dataset_name="customer_stage",
    natural_key_colnames=cols,
    pepper="test_pepper",
    ordinal=1,
    null_token="<NULL>",
    pair_sep="~",
    comp_sep="|",
  )

  expr = sk_draft.surrogate_expression
  assert isinstance(expr, str) and expr, "surrogate_expression must be a non-empty string"

  # 1) identity must be present
  assert "source_identity_id" in expr, (
    "source_identity_id must be part of the surrogate expression when "
    "it is present in the natural keys."
  )

  # 2) components must be in alphabetical order
  positions = _order_of_components(expr, expected_sorted)
  assert positions == sorted(positions), (
    "Natural key columns must appear in alphabetical order in the "
    f"surrogate key column expression. Expected order {expected_sorted}, "
    f"got positions {positions} in expression: {expr}"
  )
