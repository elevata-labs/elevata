"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

from metadata.materialization.logging import LOAD_RUN_LOG_REGISTRY
from metadata.materialization.logging import build_load_run_log_row

def test_load_run_log_row_keys_match_registry():
  row = build_load_run_log_row(
    batch_run_id="b",
    load_run_id="l",
    target_schema="raw",
    target_dataset="ds",
    target_system="sys",
    profile="default",
    mode="full",
    handle_deletes=False,
    historize=False,
    started_at="2024-01-01T00:00:00",
    finished_at="2024-01-01T00:00:01",
    render_ms=1,
    execution_ms=1,
    sql_length=10,
    rows_affected=1,
    status="success",
    error_message=None,
    attempt_no=1,
    status_reason=None,
    blocked_by=None,
  )

  registry_keys = set(LOAD_RUN_LOG_REGISTRY.keys())
  row_keys = set(row.keys())

  assert row_keys == registry_keys
