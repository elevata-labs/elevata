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

from datetime import datetime, timezone

from metadata.execution.executor import ExecutionPlan, ExecutionPolicy, ExecutionStep
from metadata.execution.snapshot import build_execution_snapshot, render_execution_snapshot_json

# Tests for load_run_snapshot builder (batch-level execution snapshot)

def test_load_run_snapshot_is_json_serializable():
  policy = ExecutionPolicy(continue_on_error=True, max_retries=2)
  plan = ExecutionPlan(
    batch_run_id="batch-1",
    steps=[
      ExecutionStep(dataset_id=1, dataset_key="raw.a", upstream_keys=()),
      ExecutionStep(dataset_id=2, dataset_key="core.b", upstream_keys=("raw.a",)),
    ],
  )

  snapshot = build_execution_snapshot(
    batch_run_id="batch-1",
    policy=policy,
    plan=plan,
    execute=True,
    no_deps=False,
    continue_on_error=True,
    max_retries=2,
    profile_name="test",
    target_system_short="wh",
    target_system_type="duckdb",
    dialect_name="DuckDbDialect",
    root_dataset_key="raw.a",
    created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    results=[
      {"status": "success", "kind": "ok", "dataset": "raw.a"},
      {"status": "skipped", "kind": "blocked", "dataset": "core.b", "blocked_by": "raw.a"},
    ],
    had_error=False,
  )

  s = render_execution_snapshot_json(snapshot)
  assert '"batch_run_id": "batch-1"' in s
  assert '"step_count": 2' in s
  assert '"counts_by_status"' in s
