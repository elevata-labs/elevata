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

from metadata.materialization.logging import ensure_load_run_log_table
from metadata.materialization.schema import ensure_target_schema


def _dialect_key(dialect) -> str:
  return str(
    getattr(dialect, "DIALECT_NAME", None)
    or getattr(dialect.__class__, "DIALECT_NAME", None)
    or dialect.__class__.__name__
  )


def ensure_load_run_log_table_once(
  *,
  engine,
  dialect,
  meta_schema: str,
  auto_provision: bool,
  ensure_state: set[tuple[str, str]] | None = None,
  ensure_func=None,
) -> None:
  """
  Ensure meta.load_run_log at most once per batch/dialect/schema context.

  The underlying ensure function remains best-effort and dialect-owned for SQL
  rendering. This helper only removes redundant runtime calls across ingestion
  and SQL load paths.
  """
  ensure = ensure_func or ensure_load_run_log_table

  if ensure_state is None:
    ensure(
      engine=engine,
      dialect=dialect,
      meta_schema=meta_schema,
      auto_provision=auto_provision,
    )
    return

  key = (_dialect_key(dialect), str(meta_schema))
  if key in ensure_state:
    return

  ensure(
    engine=engine,
    dialect=dialect,
    meta_schema=meta_schema,
    auto_provision=auto_provision,
  )
  ensure_state.add(key)


def ensure_target_schema_once(
  *,
  engine,
  dialect,
  schema_name: str,
  auto_provision: bool,
  ensure_state: set[tuple[str, str]] | None = None,
) -> None:
  """
  Ensure a target schema at most once per batch/dialect/schema context.
  """
  if ensure_state is None:
    ensure_target_schema(
      engine=engine,
      dialect=dialect,
      schema_name=schema_name,
      auto_provision=auto_provision,
    )
    return

  key = (_dialect_key(dialect), str(schema_name))
  if key in ensure_state:
    return

  ensure_target_schema(
    engine=engine,
    dialect=dialect,
    schema_name=schema_name,
    auto_provision=auto_provision,
  )
  ensure_state.add(key)
