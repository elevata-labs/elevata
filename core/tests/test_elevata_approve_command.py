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

import json
from io import StringIO

from django.core.management import call_command


def test_elevata_approve_store_writes_approval_artifact(tmp_path):
  report_path = tmp_path / "architecture_plan.json"
  approval_dir = tmp_path / "approvals"
  report_path.write_text(
    json.dumps(_sample_report_payload(), ensure_ascii=False, sort_keys=True),
    encoding="utf-8",
  )

  out = StringIO()
  call_command(
    "elevata_approve",
    str(report_path),
    "--approved-by",
    "Ilona Tag",
    "--decided-at",
    "2026-05-18T08:30:00Z",
    "--store",
    "--approval-dir",
    str(approval_dir),
    stdout=out,
  )

  approval_path = approval_dir / "report-fingerprint-123.approval.json"
  approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))

  assert approval_path.exists()
  assert approval_payload["artifact_type"] == "architecture_approval"
  assert approval_payload["report"]["report_fingerprint"] == "report-fingerprint-123"
  assert approval_payload["review"]["decided_by"] == "Ilona Tag"
  assert "Stored architecture approval artifact" in out.getvalue()


def test_elevata_approval_check_accepts_stored_artifact(tmp_path):
  report_path = tmp_path / "architecture_plan.json"
  approval_dir = tmp_path / "approvals"
  report_path.write_text(
    json.dumps(_sample_report_payload(), ensure_ascii=False, sort_keys=True),
    encoding="utf-8",
  )

  call_command(
    "elevata_approve",
    str(report_path),
    "--approved-by",
    "Ilona Tag",
    "--decided-at",
    "2026-05-18T08:30:00Z",
    "--store",
    "--approval-dir",
    str(approval_dir),
  )

  out = StringIO()
  call_command(
    "elevata_approval_check",
    str(report_path),
    str(approval_dir / "report-fingerprint-123.approval.json"),
    stdout=out,
  )

  assert "status: approved" in out.getvalue()
  assert "is_valid: true" in out.getvalue()


def _sample_report_payload():
  """
  Return a compact Architecture Change Report JSON payload.
  """
  return {
    "scope": {
      "mode": "scoped",
      "schema_short": "rawcore",
      "target_name": "rc_aw_customer",
      "dataset_keys": [
        "rawcore.rc_aw_customer",
        "rawcore.rc_aw_customer_hist",
      ],
    },
    "state": {
      "previous_fingerprint": "previous-state-123",
      "current_fingerprint": "current-state-456",
      "has_changes": True,
    },
    "summary": {
      "dataset_change_count": 0,
      "column_change_count": 1,
      "migration_action_count": 1,
      "policy_decision_count": 1,
      "blocking_policy_decision_count": 0,
    },
    "dataset_changes": [],
    "column_changes": [],
    "migration_actions": [],
    "policy_decisions": [],
    "is_blocked": False,
    "report_fingerprint": "report-fingerprint-123",
  }