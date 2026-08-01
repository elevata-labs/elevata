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

import pytest

from metadata.generation.target_generation_control import (
  build_target_generation_approval,
  build_target_generation_review,
)
from metadata.generation.target_generation_guarded_apply import (
  TargetGenerationPlanApplyError,
  _validate_generation_approval,
)
from metadata.generation.target_generation_plan import (
  TargetGenerationAction,
  build_target_generation_plan,
)


def _plan(*, length: int = 110):
  """Return one exact plan for approval-guard tests."""
  return build_target_generation_plan(
    scope_mode="schema",
    target_schema_short_names=("raw",),
    source_dataset_keys=("source_dataset:23",),
    reconcile_lifecycle=True,
    source_metadata_fingerprint="1" * 64,
    target_metadata_fingerprint="2" * 64,
    actions=(
      TargetGenerationAction(
        action_type="UPDATE_TARGET_COLUMN",
        dataset_key="raw:source_dataset:23:base",
        object_key=(
          "raw:source_dataset:23:base:column:source_column:158"
        ),
        effect_origin="DIRECT",
        change_classification="BREAKING",
        source_keys=("source_dataset:23",),
        before={"max_length": 100},
        after={"max_length": length},
      ),
    ),
  )


def test_generation_approval_guard_requires_artifact_when_enabled():
  """Verify approval enforcement fails before the guarded transaction starts."""
  with pytest.raises(
    TargetGenerationPlanApplyError,
    match="matching Generation Approval is required",
  ):
    _validate_generation_approval(
      plan=_plan(),
      approval=None,
      require_approval=True,
    )


def test_generation_approval_guard_accepts_exact_review_binding():
  """Verify an exact Generation Approval authorizes the guarded apply path."""
  plan = _plan()
  approval = build_target_generation_approval(
    review=build_target_generation_review(plan),
    decided_by="Ilona Tag",
    decided_at="2026-07-30T17:30:00Z",
  )

  _validate_generation_approval(
    plan=plan,
    approval=approval,
    require_approval=True,
  )


def test_generation_approval_guard_rejects_plan_drift():
  """Verify an approval never authorizes a different generation decision set."""
  approved_plan = _plan(length=110)
  approval = build_target_generation_approval(
    review=build_target_generation_review(approved_plan),
    decided_by="Ilona Tag",
    decided_at="2026-07-30T17:30:00Z",
  )

  with pytest.raises(
    TargetGenerationPlanApplyError,
    match="different review or plan",
  ):
    _validate_generation_approval(
      plan=_plan(length=120),
      approval=approval,
      require_approval=True,
    )
