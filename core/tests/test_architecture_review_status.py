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

from dataclasses import dataclass

from metadata.architecture.approval import (
  ArchitectureApprovalStore,
  build_architecture_approval_artifact,
)
from metadata.architecture.review_status import build_architecture_review_status_for_report


@dataclass(frozen=True)
class _Report:
  report_fingerprint: str
  has_changes: bool
  is_blocked: bool
  payload: dict

  def to_dict(self):
    return self.payload


def test_review_status_reports_no_changes(tmp_path):
  report = _sample_report(
    report_fingerprint="report-fingerprint-123",
    has_changes=False,
    is_blocked=False,
  )

  status = build_architecture_review_status_for_report(
    dataset_key="rawcore.rc_aw_customer",
    report=report,
    approval_store=ArchitectureApprovalStore(tmp_path),
  )

  assert status.status == "no_changes"
  assert status.label == "No architecture changes"
  assert status.approval_id is None


def test_review_status_reports_pending_without_approval(tmp_path):
  report = _sample_report(
    report_fingerprint="report-fingerprint-123",
    has_changes=True,
    is_blocked=False,
  )

  status = build_architecture_review_status_for_report(
    dataset_key="rawcore.rc_aw_customer",
    report=report,
    approval_store=ArchitectureApprovalStore(tmp_path),
  )

  assert status.status == "pending"
  assert status.label == "Pending review"


def test_review_status_reports_approved_matching_artifact(tmp_path):
  report = _sample_report(
    report_fingerprint="report-fingerprint-123",
    has_changes=True,
    is_blocked=False,
  )
  store = ArchitectureApprovalStore(tmp_path)
  artifact = build_architecture_approval_artifact(
    report_payload=report.to_dict(),
    decided_by="Ilona Tag",
    decided_at="2026-05-18T08:30:00Z",
  )
  store.save(artifact)

  status = build_architecture_review_status_for_report(
    dataset_key="rawcore.rc_aw_customer",
    report=report,
    approval_store=store,
  )

  assert status.status == "approved"
  assert status.label == "Approved and matching"
  assert status.approval_id == artifact.approval_id


def test_review_status_reports_drift_for_same_scope_different_report(tmp_path):
  report = _sample_report(
    report_fingerprint="report-fingerprint-new",
    has_changes=True,
    is_blocked=False,
  )
  stale_report_payload = _sample_report_payload("report-fingerprint-old")
  store = ArchitectureApprovalStore(tmp_path)
  store.save(
    build_architecture_approval_artifact(
      report_payload=stale_report_payload,
      decided_by="Ilona Tag",
      decided_at="2026-05-18T08:30:00Z",
    )
  )

  status = build_architecture_review_status_for_report(
    dataset_key="rawcore.rc_aw_customer",
    report=report,
    approval_store=store,
  )

  assert status.status == "drift"
  assert status.label == "Approval drift"
  assert status.approval_id is not None


def test_review_status_reports_blocked_before_pending(tmp_path):
  report = _sample_report(
    report_fingerprint="report-fingerprint-123",
    has_changes=True,
    is_blocked=True,
  )

  status = build_architecture_review_status_for_report(
    dataset_key="rawcore.rc_aw_customer",
    report=report,
    approval_store=ArchitectureApprovalStore(tmp_path),
  )

  assert status.status == "blocked"
  assert status.label == "Blocked by policy"


def _sample_report(
  *,
  report_fingerprint: str,
  has_changes: bool,
  is_blocked: bool,
):
  """
  Return a compact report object compatible with the review status service.
  """
  payload = _sample_report_payload(report_fingerprint)
  payload["state"]["has_changes"] = has_changes
  payload["summary"]["blocking_policy_decision_count"] = 1 if is_blocked else 0
  payload["is_blocked"] = is_blocked

  return _Report(
    report_fingerprint=report_fingerprint,
    has_changes=has_changes,
    is_blocked=is_blocked,
    payload=payload,
  )


def _sample_report_payload(report_fingerprint: str):
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
    "report_fingerprint": report_fingerprint,
  }