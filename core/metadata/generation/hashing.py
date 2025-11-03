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

  Rules:
  - Columns are sorted alphabetically to ensure deterministic order.
  - Each component has the form "<colname><pair_sep><value_or_null_token>".
  - Components are concatenated using comp_sep and followed by the pepper.
  - The whole string is wrapped in a generic hash256() call.

  This expression is stored as metadata (TargetColumn.surrogate_expression)
  and later rendered into platform-specific SQL.
  """

  # 1. Ensure deterministic ordering
  ordered_cols = sorted(natural_key_cols)

  # 2. Build per-column components
  # Example: concat('customer_id','~',coalesce(customer_id,'NULLTOKEN'))
  inner_parts = []
  for col in ordered_cols:
    part = (
      "concat("
      f"'{col}', '{pair_sep}', "
      f"coalesce({col}, '{null_token}')"
      ")"
    )
    inner_parts.append(part)

  # 3. Join components with comp_sep and append the pepper
  # Example:
  # hash256(concat_ws(' | ',
  #   concat('customer_id','~',coalesce(customer_id,'NULL')),
  #   concat('mandant','~',coalesce(mandant,'NULL')),
  #   'PEPPER'))
  all_components = ", ".join(inner_parts + [f"'{pepper}'"])

  expression = (
    "hash256("
      "concat_ws("
      f"'{comp_sep}', "
      f"{all_components}"
      ")"
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
