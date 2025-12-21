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

from .logical_plan import LogicalSelect, LogicalUnion
from .dialects.base import SqlDialect
from metadata.rendering.builder import build_logical_select_for_target


def get_effective_materialization(td) -> str:
  # Dataset-level override
  mat = getattr(td, "materialization_type", None)
  if mat:
    return mat

  # Schema-level default
  schema = getattr(td, "target_schema", None)
  if schema:
    schema_mat = getattr(schema, "default_materialization_type", None)
    if schema_mat:
      return schema_mat

  # Final fallback (safe default)
  return "table"


def render_sql(plan, dialect: SqlDialect) -> str:
  """
  Render a logical plan (LogicalSelect or LogicalUnion) into a SQL string.
  """

  # Support both LogicalSelect and LogicalUnion for stage models combining multiple sources.
  if isinstance(plan, LogicalSelect):
    return dialect.render_select(plan)

  elif isinstance(plan, LogicalUnion):
    # Render each SELECT separately and combine with UNION ALL or UNION DISTINCT.
    rendered_parts = [dialect.render_select(sel) for sel in plan.selects]
    separator = f"\nUNION {plan.union_type}\n"
    return separator.join(rendered_parts)

  else:
    raise TypeError(
      f"Unsupported logical plan type: {type(plan).__name__}. "
      "Expected LogicalSelect or LogicalUnion."
    )
  

def render_select_for_target(target_ds, dialect: SqlDialect) -> str:
  """
  Build the logical SELECT for a TargetDataset and render it
  using the given dialect.

  This is the core SELECT used by both the SQL preview and
  the Load SQL layer (full refresh).
  """
  plan = build_logical_select_for_target(target_ds)
  return render_sql(plan, dialect)
