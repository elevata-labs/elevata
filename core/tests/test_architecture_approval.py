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

from copy import deepcopy

from metadata.architecture.approval import (
  ArchitectureApprovalArtifact,
  ArchitectureApprovalReview,
  build_architecture_approval_artifact,
  check_architecture_approval,
)
from metadata.architecture.renderers import (
  render_architecture_approval_check_json,
  render_architecture_approval_json,
  render_architecture_approval_text,
)


def test_architecture_approval_artifact_is_deterministic():
  report_payload = _sample_report_payload()

  artifact_a = build_architecture_approval_artifact(
    report_payload=report_payload,
    decided_by="Ilona Tag",
    note="Reviewed for deployment.",
    decided_at="2026-05-18T08:30:00Z",
  )
  artifact_b = build_architecture_approval_artifact(
    report_payload=report_payload,
    decided_by="Ilona Tag",
    note="Reviewed for deployment.",
    decided_at="2026-05-18T08:30:00Z",
  )

  assert artifact_a.to_dict() == artifact_b.to_dict()
  assert artifact_a.approval_id == f"apr_{artifact_a.artifact_fingerprint[:16]}"
  assert artifact_a.to_dict()["artifact_fingerprint"] == artifact_a.artifact_fingerprint


def test_architecture_approval_check_accepts_matching_report():
  report_payload = _sample_report_payload()
  artifact = build_architecture_approval_artifact(
    report_payload=report_payload,
    decided_by="Ilona Tag",
    decided_at="2026-05-18T08:30:00Z",
  )

  result = check_architecture_approval(
    report_payload=report_payload,
    approval_payload=artifact.to_dict(),
  )

  assert result.is_valid is True
  assert result.status == "approved"
  assert result.report_fingerprint == "report-fingerprint-123"
  assert result.approval_id == artifact.approval_id


def test_architecture_approval_check_rejects_changed_report_summary():
  report_payload = _sample_report_payload()
  artifact = build_architecture_approval_artifact(
    report_payload=report_payload,
    decided_by="Ilona Tag",
    decided_at="2026-05-18T08:30:00Z",
  )

  changed_report = deepcopy(report_payload)
  changed_report["summary"]["column_change_count"] = 2

  result = check_architecture_approval(
    report_payload=changed_report,
    approval_payload=artifact.to_dict(),
  )

  assert result.is_valid is False
  assert result.status == "invalid"
  assert "different architecture report" in result.message


def test_architecture_approval_check_rejects_tampered_artifact_fingerprint():
  report_payload = _sample_report_payload()
  artifact = build_architecture_approval_artifact(
    report_payload=report_payload,
    decided_by="Ilona Tag",
    decided_at="2026-05-18T08:30:00Z",
  )
  approval_payload = artifact.to_dict()
  approval_payload["artifact_fingerprint"] = "tampered"

  result = check_architecture_approval(
    report_payload=report_payload,
    approval_payload=approval_payload,
  )

  assert result.is_valid is False
  assert result.status == "invalid"
  assert "fingerprint does not match" in result.message


def test_architecture_approval_check_rejects_rejected_review_decision():
  report_payload = _sample_report_payload()
  rejected_artifact = ArchitectureApprovalArtifact(
    report=build_architecture_approval_artifact(
      report_payload=report_payload,
      decided_by="Ilona Tag",
      decided_at="2026-05-18T08:30:00Z",
    ).report,
    review=ArchitectureApprovalReview(
      decision="rejected",
      decided_by="Ilona Tag",
      decided_at="2026-05-18T08:30:00Z",
      note="Needs clarification.",
    ),
  )

  result = check_architecture_approval(
    report_payload=report_payload,
    approval_payload=rejected_artifact.to_dict(),
  )

  assert result.is_valid is False
  assert result.status == "rejected"
  assert result.message == "Architecture report is not approved."


def test_architecture_approval_rejects_promotion_report_payload():
  promotion_payload = {
    "promotion_fingerprint": "promotion-fingerprint-123",
    "change_report": _sample_report_payload(),
  }

  artifact = build_architecture_approval_artifact(
    report_payload=_sample_report_payload(),
    decided_by="Ilona Tag",
    decided_at="2026-05-18T08:30:00Z",
  )

  result = check_architecture_approval(
    report_payload=promotion_payload,
    approval_payload=artifact.to_dict(),
  )

  assert result.is_valid is False
  assert result.status == "invalid"
  assert "Architecture Change Report JSON" in result.message


def test_architecture_approval_renderers_are_stable():
  report_payload = _sample_report_payload()
  artifact = build_architecture_approval_artifact(
    report_payload=report_payload,
    decided_by="Ilona Tag",
    note="Reviewed for deployment.",
    decided_at="2026-05-18T08:30:00Z",
  )
  result = check_architecture_approval(
    report_payload=report_payload,
    approval_payload=artifact.to_dict(),
  )

  rendered_json = render_architecture_approval_json(artifact)
  rendered_text = render_architecture_approval_text(artifact)
  rendered_check_json = render_architecture_approval_check_json(result)

  assert '"artifact_type": "architecture_approval"' in rendered_json
  assert "Architecture Approval Artifact" in rendered_text
  assert '"status": "approved"' in rendered_check_json


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