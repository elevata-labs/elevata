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

from __future__ import annotations
from typing import List
import hashlib

import hashlib

def build_surrogate_expression(
  natural_key_cols: list[str],
  pepper: str,
  null_token: str,
  pair_sep: str,
  comp_sep: str,
) -> str:
  """
  Build a generic, dialect-agnostic expression describing how
  the surrogate key should be computed later by the SQL renderer.

  The returned string is a *template* that uses {expr:<col>} placeholders.
  The SQL renderer will bind those to the final value expressions of the
  corresponding business/natural key columns.
  """

  # 1. Ensure deterministic ordering
  ordered_cols = sorted(natural_key_cols)

  # 2. Build per-column components
  # Example: concat('customer_id','~',coalesce({expr:customer_id},'NULLTOKEN'))
  inner_parts: list[str] = []
  for col in ordered_cols:
    part = (
      "CONCAT("
      f"'{col}', '{pair_sep}', "
      f"COALESCE({{expr:{col}}}, '{null_token}')"
      ")"
    )
    inner_parts.append(part)  

  # 3. Join components with comp_sep and append the pepper
  # We pass each concat(...) as separate argument to concat_ws
  # and add the pepper as the last argument.
  inner_expression = ", ".join(inner_parts)

  expression = (
    "HASH256("
    f"CONCAT_WS('{comp_sep}', {inner_expression}, '{pepper}')"
    ")"
  )

  return expression


def demo_python_hash(natural_key_values: list[str], pepper: str) -> str:
  """
  Deterministic reference hash for debugging / preview.
  Not executed in warehouse, only for verification.
  """
  h = hashlib.sha256()
  for v in natural_key_values:
    h.update((v or "").encode("utf-8"))
    h.update(b"||")
  h.update(pepper.encode("utf-8"))
  return h.hexdigest()
