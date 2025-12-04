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

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Iterable

from django.core.management.base import BaseCommand

from metadata.rendering.dialects import get_active_dialect
from metadata.rendering.dialects.dialect_factory import get_available_dialect_names


CheckFunc = Callable[[Any], None]


class Command(BaseCommand):
  help = (
    "Run a small self-diagnostic against all registered SQL dialects.\n\n"
    "Examples:\n"
    "  python manage.py elevata_dialect_check\n"
    "  python manage.py elevata_dialect_check --dialect duckdb\n"
  )

  def add_arguments(self, parser) -> None:
    parser.add_argument(
      "--dialect",
      dest="dialect_name",
      type=str,
      default=None,
      help="Optional dialect name to restrict diagnostics, e.g. 'duckdb', 'postgres', 'mssql'.",
    )

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  def _run_check(self, name: str, fn: CheckFunc) -> tuple[str, str]:
    """
    Run a single check and return (status, details).

    status:
      - OK   -> check succeeded
      - N/I  -> NotImplementedError
      - FAIL -> any other exception
    """
    try:
      fn(None)
      return "OK", ""
    except NotImplementedError as exc:
      return "N/I", str(exc)
    except Exception as exc:  # pragma: no cover - defensive
      return "FAIL", repr(exc)

  def _print_header(self, title: str) -> None:
    self.stdout.write("")
    self.stdout.write(self.style.MIGRATE_HEADING(title))
    self.stdout.write(self.style.HTTP_INFO("-" * len(title)))

  def _print_table(self, rows: Iterable[tuple[str, str, str]]) -> None:
    """
    Simple 3-column table: check, status, details.
    """
    for check, status, details in rows:
      line = f"  {check:<30} {status:<4}"
      if details:
        line += f"  # {details}"
      self.stdout.write(line)

  # ---------------------------------------------------------------------------
  # Main
  # ---------------------------------------------------------------------------

  def handle(self, *args: Any, **options: Any) -> None:
    dialect_name: str | None = options.get("dialect_name")

    # 1) Determine which dialects to check
    if dialect_name:
      dialect_names = [dialect_name]
    else:
      dialect_names = sorted(get_available_dialect_names())

    if not dialect_names:
      self.stdout.write(self.style.WARNING("No SQL dialects are registered."))
      return

    self._print_header("Dialect diagnostics")

    for name in dialect_names:
      dialect = get_active_dialect(name)

      self.stdout.write("")
      self.stdout.write(self.style.HTTP_INFO(f"Dialect: {name} ({dialect.__class__.__name__})"))
      self.stdout.write(f"  supports_merge           = {getattr(dialect, 'supports_merge', False)}")
      self.stdout.write(
        f"  supports_delete_detection = {getattr(dialect, 'supports_delete_detection', False)}"
      )

      # Prepare some sample values
      sample_date = date(2025, 1, 2)
      sample_dt = datetime(2025, 1, 2, 3, 4, 5)
      sample_decimal = Decimal("123.45")

      checks: list[tuple[str, str, str]] = []

      checks.append((
        "quote_ident",
        *self._run_check(
          "quote_ident",
          lambda _: dialect.quote_ident('foo"bar'),
        ),
      ))
      checks.append((
        "literal(str/int/bool)",
        *self._run_check(
          "literal(str/int/bool)",
          lambda _: (
            dialect.render_literal("abc"),
            dialect.render_literal(42),
            dialect.render_literal(True),
          ),
        ),
      ))
      checks.append((
        "literal(date/datetime/decimal)",
        *self._run_check(
          "literal(date/datetime/decimal)",
          lambda _: (
            dialect.render_literal(sample_date),
            dialect.render_literal(sample_dt),
            dialect.render_literal(sample_decimal),
          ),
        ),
      ))
      checks.append((
        "concat_expression",
        *self._run_check(
          "concat_expression",
          lambda _: dialect.concat_expression(["'a'", "'b'", "'c'"]),
        ),
      ))
      checks.append((
        "hash_expression",
        *self._run_check(
          "hash_expression",
          lambda _: dialect.hash_expression("colname"),
        ),
      ))

      # Optional capabilities: only run if method is present
      if hasattr(dialect, "render_create_replace_table"):
        checks.append((
          "create_replace_table",
          *self._run_check(
            "create_replace_table",
            lambda _: dialect.render_create_replace_table(
              schema="dw",
              table="dummy",
              select_sql="SELECT 1 AS x",
            ),
          ),
        ))

      if hasattr(dialect, "render_insert_into_table"):
        checks.append((
          "insert_into_table",
          *self._run_check(
            "insert_into_table",
            lambda _: dialect.render_insert_into_table(
              schema="dw",
              table="dummy",
              select_sql="SELECT 1 AS x",
            ),
          ),
        ))

      if getattr(dialect, "supports_merge", False) and hasattr(dialect, "render_merge_statement"):
        checks.append((
          "merge_statement",
          *self._run_check(
            "merge_statement",
            lambda _: dialect.render_merge_statement(
              schema="dw",
              table="dummy",
              select_sql="SELECT 1 AS id",
              unique_key_columns=["id"],
              update_columns=[],
            ),
          ),
        ))

      if getattr(dialect, "supports_delete_detection", False) and hasattr(
        dialect,
        "render_delete_detection_statement",
      ):
        checks.append((
          "delete_detection",
          *self._run_check(
            "delete_detection",
            lambda _: dialect.render_delete_detection_statement(
              target_schema="dw",
              target_table="dim_dummy",
              stage_schema="stg",
              stage_table="dim_dummy_stage",
              join_predicates=[
                "t.id = s.id",
              ],
              # scope_filter optional, we can omit it
            ),
          ),
        ))

      self._print_table(checks)

    self.stdout.write("")
    self.stdout.write(self.style.SUCCESS("Dialect diagnostics completed."))
