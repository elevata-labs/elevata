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

from __future__ import annotations

from datetime import datetime, timezone
import json

import pytest

from metadata.promotion.approval import (
  EnvironmentPromotionApprovalError,
  build_environment_promotion_approval,
  check_environment_promotion_approval,
  deserialize_environment_promotion_approval,
  serialize_environment_promotion_approval,
)
from metadata.promotion.approval_store import (
  EnvironmentPromotionApprovalStore,
  EnvironmentPromotionApprovalStoreError,
)
from metadata.promotion.identities import build_metadata_object_identity
from metadata.promotion.model_contracts import METADATA_TRANSPORT_REGISTRY
from metadata.promotion.plan import (
  EnvironmentPromotionAction,
  EnvironmentPromotionActionType,
  EnvironmentPromotionPlan,
  EnvironmentPromotionReadiness,
  EnvironmentPromotionReadinessStatus,
  EnvironmentPromotionSubjectType,
)
from metadata.promotion.snapshot import EnvironmentMetadataObject


UTC = timezone.utc


def test_environment_promotion_approval_roundtrips_and_binds_exact_plan():
  plan = _ready_plan(target_snapshot_fingerprint="target-a")
  approval = build_environment_promotion_approval(
    plan=plan,
    decided_by="reviewer@example.com",
    note="Approved for production metadata apply.",
    decided_at=datetime(2026, 8, 5, 3, 0, tzinfo=UTC),
  )

  check = check_environment_promotion_approval(
    plan=plan,
    approval=approval,
  )
  assert check.is_valid is True
  assert check.status == "approved"
  assert approval.approval_id.startswith("papr-")

  rendered = serialize_environment_promotion_approval(approval)
  restored = deserialize_environment_promotion_approval(rendered)
  assert restored == approval
  assert serialize_environment_promotion_approval(restored) == rendered

  changed_plan = _ready_plan(target_snapshot_fingerprint="target-b")
  changed_check = check_environment_promotion_approval(
    plan=changed_plan,
    approval=approval,
  )
  assert changed_check.is_valid is False
  assert changed_check.status == "plan_mismatch"

  tampered = json.loads(rendered)
  tampered["review"]["note"] = "Changed after approval"
  with pytest.raises(
    EnvironmentPromotionApprovalError,
    match="artifact fingerprint mismatch",
  ):
    deserialize_environment_promotion_approval(json.dumps(tampered))


def test_environment_promotion_approval_store_is_immutable(tmp_path):
  plan = _ready_plan(target_snapshot_fingerprint="target-a")
  approval = build_environment_promotion_approval(
    plan=plan,
    decided_by="reviewer@example.com",
    decided_at=datetime(2026, 8, 5, 3, 0, tzinfo=UTC),
  )
  store = EnvironmentPromotionApprovalStore(tmp_path)

  first_path = store.save(approval)
  second_path = store.save(approval)
  assert second_path == first_path
  assert store.load_for_plan(
    target_environment_label="prod",
    plan_fingerprint=plan.plan_fingerprint,
  ) == approval

  different = build_environment_promotion_approval(
    plan=plan,
    decided_by="other-reviewer@example.com",
    decided_at=datetime(2026, 8, 5, 3, 1, tzinfo=UTC),
  )
  with pytest.raises(
    EnvironmentPromotionApprovalStoreError,
    match="different immutable approval",
  ):
    store.save(different)


def test_environment_promotion_approval_rejects_non_ready_plan():
  ready = _ready_plan(target_snapshot_fingerprint="target-a")
  no_changes = EnvironmentPromotionPlan(
    release_id=ready.release_id,
    bundle_fingerprint=ready.bundle_fingerprint,
    source_environment_label=ready.source_environment_label,
    source_snapshot_fingerprint=ready.source_snapshot_fingerprint,
    source_metadata_fingerprint=ready.source_metadata_fingerprint,
    target_environment_label=ready.target_environment_label,
    target_snapshot_fingerprint=ready.target_snapshot_fingerprint,
    target_metadata_fingerprint=ready.target_metadata_fingerprint,
    created_at=ready.created_at,
    actions=(),
    readiness=EnvironmentPromotionReadiness(
      status=EnvironmentPromotionReadinessStatus.NO_CHANGES,
    ),
  )

  with pytest.raises(
    EnvironmentPromotionApprovalError,
    match="Only a ready",
  ):
    build_environment_promotion_approval(
      plan=no_changes,
      decided_by="reviewer@example.com",
    )


def _ready_plan(*, target_snapshot_fingerprint: str) -> EnvironmentPromotionPlan:
  team = _team_object("analytics")
  action = EnvironmentPromotionAction(
    action_type=EnvironmentPromotionActionType.CREATE,
    subject_type=EnvironmentPromotionSubjectType.OBJECT,
    subject_name="Team",
    dependency_phase=10,
    desired=team.to_dict(),
  )
  return EnvironmentPromotionPlan(
    release_id="rel-0123456789abcdef",
    bundle_fingerprint="bundle-fingerprint",
    source_environment_label="dev",
    source_snapshot_fingerprint="source-snapshot",
    source_metadata_fingerprint="source-metadata",
    target_environment_label="prod",
    target_snapshot_fingerprint=target_snapshot_fingerprint,
    target_metadata_fingerprint="target-metadata",
    created_at=datetime(2026, 8, 5, 2, 0, tzinfo=UTC),
    actions=(action,),
    readiness=EnvironmentPromotionReadiness(
      status=EnvironmentPromotionReadinessStatus.READY,
    ),
  )


def _team_object(name: str) -> EnvironmentMetadataObject:
  contract = METADATA_TRANSPORT_REGISTRY.get_model("Team")
  identity = build_metadata_object_identity(
    model_name="Team",
    contract=contract.identity,
    values={"name": name},
  )
  return EnvironmentMetadataObject(
    model_name="Team",
    identity=identity,
    fields={
      "name": name,
      "description": "Analytics team",
    },
  )
