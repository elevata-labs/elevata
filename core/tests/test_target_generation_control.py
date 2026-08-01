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

from copy import deepcopy

import pytest

from metadata.generation.target_generation_control import (
  TARGET_GENERATION_APPROVAL_ARTIFACT_TYPE,
  TARGET_GENERATION_REVIEW_ARTIFACT_TYPE,
  TargetGenerationApprovalStore,
  TargetGenerationControlError,
  build_target_generation_approval,
  build_target_generation_review,
  check_target_generation_approval,
  parse_target_generation_approval_json,
  parse_target_generation_review_json,
  render_target_generation_approval_json,
  render_target_generation_review_json,
  render_target_generation_review_text,
  target_generation_approval_from_dict,
)
from metadata.generation.target_generation_plan import (
  TargetGenerationAction,
  build_target_generation_plan,
)


def _plan(*, length: int = 110, actions=True):
  """Return one deterministic reviewed generation plan."""
  planned_actions = ()
  if actions:
    planned_actions = (
      TargetGenerationAction(
        action_type="UPDATE_TARGET_COLUMN",
        dataset_key="raw:source_dataset:23:base",
        object_key=(
          "raw:source_dataset:23:base:column:source_column:158"
        ),
        effect_origin="DIRECT",
        change_classification="BREAKING",
        source_keys=("source_dataset:23",),
        before={
          "target_column_name": "name",
          "max_length": 100,
        },
        after={
          "target_column_name": "name",
          "max_length": length,
        },
        reason="Generated column follows Source Metadata.",
      ),
      TargetGenerationAction(
        action_type="UPDATE_TARGET_COLUMN",
        dataset_key="rawcore:source_dataset:23:hist",
        object_key=(
          "rawcore:source_dataset:23:hist:column:source_column:158"
        ),
        effect_origin="HISTORY_COMPANION",
        change_classification="NEUTRAL",
        source_keys=("source_dataset:23",),
        before={
          "target_column_name": "name",
          "ordinal_position": 4,
        },
        after={
          "target_column_name": "name",
          "ordinal_position": 5,
        },
        reason="History companion follows the generated base contract.",
      ),
      TargetGenerationAction(
        action_type="RETIRE_TARGET_DATASET",
        dataset_key="raw:source_dataset:99:base",
        object_key="raw:source_dataset:99:base",
        effect_origin="GENERATED_LIFECYCLE",
        change_classification="BREAKING",
        source_keys=("source_dataset:99",),
        before={"active": True, "retired_at_state": "unset"},
        after={"active": False, "retired_at_state": "set"},
        reason="Generated dataset left the selected lifecycle scope.",
      ),
    )
  return build_target_generation_plan(
    scope_mode="schema",
    target_schema_short_names=("raw",),
    source_dataset_keys=("source_dataset:23",),
    reconcile_lifecycle=True,
    source_metadata_fingerprint="1" * 64,
    target_metadata_fingerprint="2" * 64,
    actions=planned_actions,
  )


def test_generation_review_exposes_source_to_target_impact_deterministically():
  """Verify review counts and Source-to-Target mappings derive from the plan."""
  review = build_target_generation_review(_plan())
  payload = review.to_dict()

  assert payload["artifact_type"] == TARGET_GENERATION_REVIEW_ARTIFACT_TYPE
  assert payload["summary"] == {
    "action_count": 3,
    "action_counts": {
      "CREATE_TARGET_COLUMN": 0,
      "CREATE_TARGET_DATASET": 0,
      "REACTIVATE_TARGET_COLUMN": 0,
      "REACTIVATE_TARGET_DATASET": 0,
      "RETIRE_TARGET_COLUMN": 0,
      "RETIRE_TARGET_DATASET": 1,
      "SYNC_TARGET_COLUMN_INPUTS": 0,
      "SYNC_TARGET_DATASET_INPUTS": 0,
      "UPDATE_TARGET_COLUMN": 2,
      "UPDATE_TARGET_DATASET": 0,
    },
    "classification_counts": {
      "BREAKING": 2,
      "ADDITIVE": 0,
      "NEUTRAL": 1,
    },
    "effect_origin_counts": {
      "DIRECT": 1,
      "HISTORY_COMPANION": 1,
      "GENERATED_LIFECYCLE": 1,
      "MODEL_SIDE_EFFECT": 0,
    },
    "target_dataset_count": 3,
    "source_count": 2,
    "has_breaking_changes": True,
    "approval_recommended": True,
  }
  assert payload["source_impacts"] == [
    {
      "source_key": "source_dataset:23",
      "target_dataset_keys": [
        "raw:source_dataset:23:base",
        "rawcore:source_dataset:23:hist",
      ],
      "action_count": 2,
      "change_classification": "BREAKING",
    },
    {
      "source_key": "source_dataset:99",
      "target_dataset_keys": ["raw:source_dataset:99:base"],
      "action_count": 1,
      "change_classification": "BREAKING",
    },
  ]
  assert "source_dataset:23 -> raw:source_dataset:23:base" in (
    render_target_generation_review_text(review)
  )


def test_generation_review_json_round_trip_preserves_fingerprint():
  """Verify canonical review parsing preserves the exact public artifact."""
  review = build_target_generation_review(_plan())
  parsed = parse_target_generation_review_json(
    render_target_generation_review_json(review)
  )

  assert parsed.to_dict() == review.to_dict()
  assert parsed.review_fingerprint == review.review_fingerprint


def test_generation_approval_is_distinct_and_binds_plan_and_review(tmp_path):
  """Verify Generation Approval cannot be confused with Execution Approval."""
  review = build_target_generation_review(_plan())
  approval = build_target_generation_approval(
    review=review,
    decided_by="Ilona Tag",
    note="Reviewed Source-to-Target impact.",
    decided_at="2026-07-30T17:30:00Z",
  )

  assert approval.to_dict()["artifact_type"] == (
    TARGET_GENERATION_APPROVAL_ARTIFACT_TYPE
  )
  assert approval.approval_id.startswith("gpa_")
  assert approval.review["plan_fingerprint"] == review.plan_fingerprint
  assert approval.review["review_fingerprint"] == review.review_fingerprint

  path = TargetGenerationApprovalStore(base_path=tmp_path).save(approval)
  assert path.name == (
    f"{review.review_fingerprint}.generation.approval.json"
  )
  loaded = TargetGenerationApprovalStore.load_file(path)
  assert loaded.to_dict() == approval.to_dict()
  assert parse_target_generation_approval_json(
    render_target_generation_approval_json(approval)
  ).to_dict() == approval.to_dict()


def test_generation_approval_check_rejects_review_drift():
  """Verify an approval for one plan cannot authorize another plan."""
  first_review = build_target_generation_review(_plan(length=110))
  approval = build_target_generation_approval(
    review=first_review,
    decided_by="Ilona Tag",
    decided_at="2026-07-30T17:30:00Z",
  )
  changed_review = build_target_generation_review(_plan(length=120))

  result = check_target_generation_approval(
    review=changed_review,
    approval=approval,
  )

  assert result.is_valid is False
  assert result.status == "drift"
  assert "different review or plan" in result.message


def test_generation_approval_parser_rejects_tampering():
  """Verify the approval fingerprint protects its review binding."""
  review = build_target_generation_review(_plan())
  approval = build_target_generation_approval(
    review=review,
    decided_by="Ilona Tag",
    decided_at="2026-07-30T17:30:00Z",
  )
  payload = deepcopy(approval.to_dict())
  payload["review"]["summary"]["action_count"] = 999

  with pytest.raises(
    TargetGenerationControlError,
    match="fingerprint does not match",
  ):
    target_generation_approval_from_dict(payload)


def test_generation_approval_rejects_no_change_plan():
  """Verify no approval workflow is created for a no-op plan."""
  review = build_target_generation_review(_plan(actions=False))

  assert review.approval_recommended is False
  with pytest.raises(
    TargetGenerationControlError,
    match="not required for a plan without actions",
  ):
    build_target_generation_approval(
      review=review,
      decided_by="Ilona Tag",
      decided_at="2026-07-30T17:30:00Z",
    )
