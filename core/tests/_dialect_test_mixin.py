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
from metadata.rendering.dialects.base import SqlDialect


class DialectTestMixin(SqlDialect):
  DIALECT_NAME = "dummy"
  supports_merge = True
  supports_delete_detection = True
  supports_alter_column_type = False

  def __init__(self, *args, engine=None, **kwargs):
    super().__init__(*args, **kwargs)

    # per-instance state
    self.calls: list[dict] = []

    # optional execution engine for tests that need it
    self._engine = engine

  def get_execution_engine(self, system):
    return self._engine

  def quote_ident(self, ident: str) -> str:
    return ident

  def render_identifier(self, ident: str) -> str:
    return ident

  def render_table_identifier(self, schema: str, table: str) -> str:
    if schema:
      return f"{schema}.{table}"
    return table

  def render_create_schema_if_not_exists(self, schema: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {schema}"

  def truncate_string_expression(self, expr: str, length: int) -> str:
    return f"LEFT({expr}, {int(length)})"

  def render_insert_load_run_log(self, *, meta_schema: str, values: dict) -> str | None:
    return None
  
  def map_logical_type(self, *, datatype, max_length=None, precision=None, scale=None, strict=True):
    # Return a "physical" type string
    return "INT"

  def render_physical_type(
    self,
    *,
    canonical: str,
    max_length=None,
    precision=None,
    scale=None,
    strict: bool = True,
  ) -> str:
    t = (canonical or "").strip().upper()

    if t in ("STRING", "VARCHAR", "TEXT"):
      n = int(max_length or 64)
      return f"VARCHAR({n})"
    if t in ("INTEGER", "INT"):
      return "INT"
    if t == "BIGINT":
      return "BIGINT"
    if t in ("DECIMAL", "NUMERIC"):
      if precision is not None and scale is not None:
        return f"DECIMAL({int(precision)},{int(scale)})"
      return "DECIMAL"
    if t in ("BOOLEAN", "BOOL"):
      return "BOOLEAN"
    if t == "TIMESTAMP":
      return "TIMESTAMP"
    if t == "DATE":
      return "DATE"

    return t

  def render_delete_detection_statement(
    self,
    target_schema,
    target_table,
    stage_schema,
    stage_table,
    join_predicates,
    scope_filter=None,
  ):
    # Record call for assertions and return a dummy SQL marker
    self.calls.append(
      {
        "target_schema": target_schema,
        "target_table": target_table,
        "stage_schema": stage_schema,
        "stage_table": stage_table,
        "join_predicates": list(join_predicates),
        "scope_filter": scope_filter,
      }
    )
    return "-- dummy delete detection sql"
  
  # ---------------------------------------------------------------------------
  # Merge spy hook
  # ---------------------------------------------------------------------------
  def render_merge_statement(self, **kwargs):
    """
    Spy wrapper around the real dialect implementation.

    Records semantic merge ingredients passed from load_sql:
      - target_fqn
      - source_select_sql
      - key_columns
      - update_columns
      - insert_columns

    Then delegates to the actual dialect implementation via super().
    """
    # Record semantic contract for assertions
    self.calls.append({
      "merge_kwargs": dict(kwargs)
    })

    # Delegate to real dialect implementation
    return super().render_merge_statement(**kwargs)