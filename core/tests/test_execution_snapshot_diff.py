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

from metadata.execution.snapshot_diff import diff_execution_snapshots


def test_snapshot_diff_detects_status_change():
  left = {
    "batch_run_id": "A",
    "policy": {"continue_on_error": False, "max_retries": 0},
    "context": {"execute": True},
    "plan": {"steps": [{"dataset_key": "raw.a", "upstream_keys": []}]},
    "outcome": {"results": [{"dataset": "raw.a", "status": "success", "kind": "ok"}]},
  }

  right = {
    "batch_run_id": "B",
    "policy": {"continue_on_error": False, "max_retries": 0},
    "context": {"execute": True},
    "plan": {"steps": [{"dataset_key": "raw.a", "upstream_keys": []}]},
    "outcome": {"results": [{"dataset": "raw.a", "status": "error", "kind": "exception"}]},
  }

  diff = diff_execution_snapshots(left=left, right=right)

  assert diff["summary"]["outcome_changed"] is True
  assert diff["outcomes"]["status_changes"] == [
    {"dataset": "raw.a", "before": "success", "after": "error"}
  ]

def test_snapshot_diff_detects_plan_dataset_added():
  left = {
    "policy": {"continue_on_error": True, "max_retries": 0},
    "context": {"execute": True},
    "plan": {"steps": [{"dataset_key": "raw.a", "upstream_keys": []}]},
    "outcome": {"results": []},
  }
  right = {
    "policy": {"continue_on_error": True, "max_retries": 0},
    "context": {"execute": True},
    "plan": {"steps": [
      {"dataset_key": "raw.a", "upstream_keys": []},
      {"dataset_key": "core.b", "upstream_keys": ["raw.a"]},
    ]},
    "outcome": {"results": []},
  }

  diff = diff_execution_snapshots(left=left, right=right)
  assert diff["summary"]["plan_changed"] is True
  assert diff["plan"]["datasets_added"] == ["core.b"]
