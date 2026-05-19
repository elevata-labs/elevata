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

import pytest

from metadata.architecture.approval import (
  ArchitectureApprovalError,
  ArchitectureApprovalStore,
  build_architecture_approval_artifact,
  resolve_architecture_approval_dir,
)


def test_resolve_architecture_approval_dir_uses_env(monkeypatch, tmp_path):
  approval_dir = tmp_path / "approvals"
  monkeypatch.setenv("ELEVATA_ARCH_APPROVAL_DIR", str(approval_dir))

  assert resolve_architecture_approval_dir() == approval_dir


def test_approval_store_saves_and_loads_artifact(tmp_path):
  artifact = build_architecture_approval_artifact(
    report_payload=_sample_report_payload(),
    decided_by="Ilona Tag",
    note="Reviewed for deployment.",
    decided_at="2026-05-18T08:30:00Z",
  )
  store = ArchitectureApprovalStore(tmp_path)

  path = store.save(artifact)
  loaded = store.load_for_report_fingerprint("report-fingerprint-123")

  assert path == tmp_path / "report-fingerprint-123.approval.json"
  assert loaded == artifact


def test_approval_store_load_all_returns_valid_artifacts(tmp_path):
  artifact = build_architecture_approval_artifact(
    report_payload=_sample_report_payload(),
    decided_by="Ilona Tag",
    decided_at="2026-05-18T08:30:00Z",
  )
  store = ArchitectureApprovalStore(tmp_path)
  store.save(artifact)

  artifacts = store.load_all()

  assert artifacts == (artifact,)


def test_approval_store_rejects_tampered_fingerprint(tmp_path):
  artifact = build_architecture_approval_artifact(
    report_payload=_sample_report_payload(),
    decided_by="Ilona Tag",
    decided_at="2026-05-18T08:30:00Z",
  )
  store = ArchitectureApprovalStore(tmp_path)
  path = store.save(artifact)

  payload = json.loads(path.read_text(encoding="utf-8"))
  payload["artifact_fingerprint"] = "tampered"
  path.write_text(
    json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
    encoding="utf-8",
  )

  with pytest.raises(ArchitectureApprovalError):
    store.load_for_report_fingerprint("report-fingerprint-123")


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