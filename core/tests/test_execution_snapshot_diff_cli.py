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

from metadata.execution.snapshot_diff import (
  diff_execution_snapshots,
  render_execution_snapshot_diff_text,
)


def test_snapshot_diff_renderer_outputs_sections():
  left = {
    "batch_run_id": "A",
    "policy": {"continue_on_error": False, "max_retries": 0},
    "plan": {"steps": [{"dataset_key": "raw.a", "upstream_keys": []}]},
    "outcome": {"results": [{"dataset": "raw.a", "status": "success", "kind": "ok"}]},
    "context": {"execute": True},
  }

  right = {
    "batch_run_id": "B",
    "policy": {"continue_on_error": True, "max_retries": 2},
    "plan": {"steps": [{"dataset_key": "raw.a", "upstream_keys": []}]},
    "outcome": {"results": [{"dataset": "raw.a", "status": "error", "kind": "exception"}]},
    "context": {"execute": True},
  }

  diff = diff_execution_snapshots(left=left, right=right)
  txt = render_execution_snapshot_diff_text(diff=diff, left_batch_run_id="A", right_batch_run_id="B")

  assert "Execution snapshot diff (A → B)" in txt
  assert "Policy changes:" in txt
  assert "Outcome changes:" in txt
  assert "raw.a: success → error" in txt
