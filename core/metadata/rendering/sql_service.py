"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

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

import re
from dataclasses import dataclass

from metadata.models import TargetDataset
from metadata.rendering.dialects.base import SqlDialect
from metadata.rendering.renderer import render_select_for_target
from metadata.rendering.load_sql import (
  render_merge_sql as _render_merge_sql,
  render_delete_missing_rows_sql as _render_delete_missing_rows_sql,
  render_load_sql_for_target as _render_load_sql_for_target,
)


@dataclass(frozen=True)
class SqlPresentationPolicy:
  """Rules for how SQL is presented to humans (UI copy/paste)."""

  # Runtime-only tokens must remain unresolved in UI by default.
  preserve_tokens: set[str]


DEFAULT_PRESENTATION_POLICY = SqlPresentationPolicy(
  preserve_tokens={
    "DELTA_CUTOFF",
    "load_timestamp",
    "load_run_id",
  }
)


def beautify_sql(sql: str) -> str:
  """Simple SQL formatter for UI previews (best-effort)."""
  if not isinstance(sql, str):
    return ""

  # 1) Normalize whitespace
  sql = re.sub(r"\s+", " ", sql).strip()

  # 2) Add line breaks before main clauses
  # Important: handle UNION variants before plain UNION
  for kw in [
    "UNION ALL",
    "UNION DISTINCT",
    "UNION",
    "FROM",
    "WHERE",
    "GROUP BY",
    "ORDER BY",
    "HAVING",
    "LIMIT",
  ]:
    sql = re.sub(rf"\s+{kw}\s+", f"\n{kw}\n", sql, flags=re.IGNORECASE)

  # 3) Put SELECT list on separate lines
  sql = re.sub(r"SELECT\s+", "SELECT\n  ", sql, flags=re.IGNORECASE)
  sql = sql.replace(", ", ",\n  ")

  # 4) Indent lines following FROM / WHERE / GROUP BY / ORDER BY
  sql = re.sub(
    r"\n(FROM|WHERE|GROUP BY|ORDER BY|HAVING)\s+",
    r"\n\1\n  ",
    sql,
    flags=re.IGNORECASE,
  )

  return sql.strip()


def render_select_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
  *,
  presentation: bool = True,
) -> str:
  canonical = render_select_for_target(dataset, dialect)
  return _to_presentation(canonical, policy=DEFAULT_PRESENTATION_POLICY) if presentation else canonical


def render_merge_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
  *,
  presentation: bool = True,
) -> str:
  canonical = _render_merge_sql(dataset, dialect)
  return _to_presentation(canonical, policy=DEFAULT_PRESENTATION_POLICY) if presentation else canonical


def render_delete_detection_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
  *,
  presentation: bool = True,
) -> str:
  canonical = _render_delete_missing_rows_sql(dataset, dialect)
  return _to_presentation(canonical, policy=DEFAULT_PRESENTATION_POLICY) if presentation else canonical


def render_load_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
  *,
  presentation: bool = False,
) -> str:
  # Load/execution should default to canonical.
  canonical = _render_load_sql_for_target(dataset, dialect)
  return _to_presentation(canonical, policy=DEFAULT_PRESENTATION_POLICY) if presentation else canonical


def render_preview_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
) -> str:
  """Render SQL for UI preview (beautified, with helpful comments)."""
  schema_short = getattr(getattr(dataset, "target_schema", None), "schema_short", None)
  name = getattr(dataset, "target_dataset_name", None)

  if (
    schema_short == "rawcore"
    and isinstance(name, str)
    and name.endswith("_hist")
  ):
    raw_sql = (
      f"-- SQL preview for history dataset {name} is not implemented yet.\n"
      f"-- Use the corresponding rawcore dataset for preview and load logic.\n"
    )
    return beautify_sql(raw_sql)

  raw_sql = render_select_sql(dataset, dialect, presentation=True)
  return beautify_sql(raw_sql)


def _to_presentation(sql: str, *, policy: SqlPresentationPolicy) -> str:
  """Convert canonical SQL into human-friendly SQL without changing semantics.

  Today this is intentionally a no-op because the only {{...}} tokens used in
  canonical SQL are runtime-only (e.g. DELTA_CUTOFF, load_timestamp, load_run_id).

  Later we can optionally support a 'sample context' substitution layer here.
  """
  _ = policy
  return sql
