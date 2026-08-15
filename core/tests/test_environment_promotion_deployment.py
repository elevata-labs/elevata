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

from metadata.promotion.approval import build_environment_promotion_approval
from metadata.promotion.deployment import (
  EnvironmentPromotionDeploymentPackageError,
  build_environment_promotion_deployment_package,
  deserialize_environment_promotion_deployment_package,
  serialize_environment_promotion_deployment_package,
)
from metadata.promotion.identities import build_metadata_object_identity
from metadata.promotion.model_contracts import (
  METADATA_TRANSPORT_REGISTRY,
  REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS,
)
from metadata.promotion.planner import build_environment_promotion_plan
from metadata.promotion.release import ArchitectureReleaseBundle
from metadata.promotion.snapshot import (
  EnvironmentMetadataObject,
  EnvironmentMetadataPayload,
  EnvironmentMetadataSnapshot,
)


UTC = timezone.utc


def test_approved_deployment_package_roundtrips_and_detects_tampering():
  schemas = _required_schema_objects()
  desired_team = _object(
    "Team",
    {"name": "analytics"},
    name="analytics",
    description="Analytics team",
  )
  source_snapshot = _snapshot("dev", objects=(*schemas, desired_team))
  target_snapshot = _snapshot("prod", objects=schemas)
  bundle = _bundle(source_snapshot)
  plan = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=target_snapshot,
    created_at=datetime(2026, 8, 5, 5, 0, tzinfo=UTC),
  )
  approval = build_environment_promotion_approval(
    plan=plan,
    decided_by="reviewer@example.com",
    decided_at=datetime(2026, 8, 5, 5, 15, tzinfo=UTC),
  )
  package = build_environment_promotion_deployment_package(
    bundle=bundle,
    plan=plan,
    approval=approval,
    created_by="packager@example.com",
    created_at=datetime(2026, 8, 5, 5, 30, tzinfo=UTC),
  )

  rendered = serialize_environment_promotion_deployment_package(package)
  restored = deserialize_environment_promotion_deployment_package(rendered)

  assert restored == package
  assert package.package_id.startswith("dpkg-")
  assert package.target_environment_label == "prod"
  assert serialize_environment_promotion_deployment_package(restored) == rendered

  tampered = json.loads(rendered)
  tampered["created_by"] = "other@example.com"
  with pytest.raises(
    EnvironmentPromotionDeploymentPackageError,
    match="deployment package fingerprint mismatch",
  ):
    deserialize_environment_promotion_deployment_package(
      json.dumps(tampered)
    )


def test_deployment_package_rejects_mismatched_approval():
  schemas = _required_schema_objects()
  desired_team = _object(
    "Team",
    {"name": "analytics"},
    name="analytics",
    description="Analytics team",
  )
  bundle = _bundle(_snapshot("dev", objects=(*schemas, desired_team)))
  first_plan = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=_snapshot("prod", objects=schemas),
  )
  approval = build_environment_promotion_approval(
    plan=first_plan,
    decided_by="reviewer@example.com",
  )
  changed_target = _snapshot(
    "prod",
    objects=(
      *schemas,
      _object(
        "Team",
        {"name": "other"},
        name="other",
        description="Other team",
      ),
    ),
  )
  changed_plan = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=changed_target,
  )

  with pytest.raises(
    EnvironmentPromotionDeploymentPackageError,
    match="does not match the exact current plan",
  ):
    build_environment_promotion_deployment_package(
      bundle=bundle,
      plan=changed_plan,
      approval=approval,
      created_by="packager@example.com",
    )


def _required_schema_objects() -> tuple[EnvironmentMetadataObject, ...]:
  return tuple(
    _object(
      "TargetSchema",
      {"short_name": short_name},
      short_name=short_name,
      schema_name=short_name,
      is_system_managed=True,
    )
    for short_name in REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS
  )


def _object(
  model_name: str,
  identity_values: dict,
  **field_values,
) -> EnvironmentMetadataObject:
  contract = METADATA_TRANSPORT_REGISTRY.get_model(model_name)
  fields = {
    field_name: None
    for field_name in contract.transport_fields
  }
  fields.update(field_values)
  identity = build_metadata_object_identity(
    model_name=model_name,
    contract=contract.identity,
    values=identity_values,
  )
  return EnvironmentMetadataObject(
    model_name=model_name,
    identity=identity,
    fields=fields,
  )


def _snapshot(
  environment_label: str,
  *,
  objects=(),
) -> EnvironmentMetadataSnapshot:
  return EnvironmentMetadataSnapshot(
    environment_label=environment_label,
    created_at=datetime(2026, 8, 5, 4, 0, tzinfo=UTC),
    created_by="test@example.com",
    metadata=EnvironmentMetadataPayload(objects=tuple(objects)),
  )


def _bundle(snapshot: EnvironmentMetadataSnapshot) -> ArchitectureReleaseBundle:
  return ArchitectureReleaseBundle(
    release_name="customer-platform",
    release_version="1.0.0",
    created_at=datetime(2026, 8, 5, 4, 30, tzinfo=UTC),
    created_by="release@example.com",
    snapshot=snapshot,
  )
