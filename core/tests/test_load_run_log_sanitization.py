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

import re

from metadata.rendering.dialects.databricks import DatabricksDialect


def _sanitize_sql_string(value: str, max_len: int = 1500) -> str:
  # Keep this helper in sync with elevata_load.py.
  s = (value or "")
  s = s.replace("\r\n", "\n").replace("\r", "\n")
  s = s.replace("\n", " ")
  s = s.replace("'", "''")
  s = " ".join(s.split())
  if len(s) > max_len:
    s = s[: max_len - 3] + "..."
  return s


def test_render_insert_load_run_log_escapes_error_message_newlines_and_quotes():
  dialect = DatabricksDialect()

  raw_error = (
    "Non-executable SQL was rendered for bizcore.bc_dim_customer_other.\n"
    "-- Unsupported load mode 'historize' for bc_dim_customer_other.\n"
    "Line with a 'quote' and more text.\r\n"
  )
  safe_error = _sanitize_sql_string(raw_error)

  values = {
    "batch_run_id": "batch-1",
    "load_run_id": "run-1",
    "target_schema": "bizcore",
    "target_dataset": "bc_dim_customer_other",
    "target_system": "dbdwh",
    "profile": "dev",
    "mode": "historize",
    "handle_deletes": False,
    "historize": False,
    "started_at": "2026-02-09 18:56:29+00:00",
    "finished_at": "2026-02-09 18:56:35+00:00",
    "render_ms": 0,
    "execution_ms": 0,
    "sql_length": 126,
    "rows_affected": None,
    "status": "error",
    "error_message": safe_error,
    "attempt_no": 1,
    "status_reason": None,
    "blocked_by": None,
  }

  sql = dialect.render_insert_load_run_log(meta_schema="meta", values=values)

  assert sql is not None
  assert "INSERT INTO meta.load_run_log" in sql

  # The statement may be multi-line formatted; that's fine.
  # We only require the *error_message literal* to be single-line and quote-safe.

  # Extract error_message literal content:
  # ... status, error_message, attempt_no ...
  m = re.search(r"'error'\s*,\s*'(.*?)'\s*,\s*1\s*,", sql, flags=re.DOTALL)
  assert m is not None, "Could not locate error_message literal in INSERT SQL."

  error_literal = m.group(1)

  # No newline characters inside the string literal
  assert "\n" not in error_literal
  assert "\r" not in error_literal

  # Quotes must be doubled inside the literal
  assert "''historize''" in error_literal
  assert "''quote''" in error_literal

  # Sanity: flattened message is present
  assert "Non-executable SQL was rendered" in error_literal
  assert "-- Unsupported load mode" in error_literal
