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
from io import StringIO
import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from metadata.models import Team
from metadata.promotion.approval import (
  build_environment_promotion_approval,
  serialize_environment_promotion_approval,
)
from metadata.promotion.deployment import (
  deserialize_environment_promotion_deployment_package,
)
from metadata.promotion.identities import build_metadata_object_identity
from metadata.promotion.model_contracts import METADATA_TRANSPORT_REGISTRY
from metadata.promotion.plan import serialize_environment_promotion_plan
from metadata.promotion.planner import build_environment_promotion_plan
from metadata.promotion.release import (
  ArchitectureReleaseBundle,
  serialize_architecture_release_bundle,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataObject,
  EnvironmentMetadataPayload,
  EnvironmentMetadataSnapshot,
  serialize_environment_metadata_snapshot,
)
from metadata.promotion.snapshot_builder import (
  build_environment_metadata_snapshot,
)


UTC = timezone.utc


@pytest.mark.django_db
def test_promotion_command_builds_checks_and_applies_approved_package(tmp_path):
  target_snapshot = build_environment_metadata_snapshot(
    environment_label="prod",
    created_by="planner@example.com",
    created_at=datetime(2026, 8, 5, 5, 0, tzinfo=UTC),
  )
  team_name = "promotion-command-team"
  desired_team = _object(
    "Team",
    {"name": team_name},
    name=team_name,
    description="Created through deployment command",
  )
  source_snapshot = EnvironmentMetadataSnapshot(
    environment_label="dev",
    created_at=datetime(2026, 8, 5, 5, 5, tzinfo=UTC),
    created_by="release@example.com",
    metadata=EnvironmentMetadataPayload(
      objects=(*target_snapshot.metadata.objects, desired_team),
      relationships=target_snapshot.metadata.relationships,
    ),
  )
  bundle = ArchitectureReleaseBundle(
    release_name="command-test",
    release_version="1.0.0",
    created_at=datetime(2026, 8, 5, 5, 10, tzinfo=UTC),
    created_by="release@example.com",
    snapshot=source_snapshot,
  )
  plan = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=target_snapshot,
    created_at=datetime(2026, 8, 5, 5, 15, tzinfo=UTC),
  )
  approval = build_environment_promotion_approval(
    plan=plan,
    decided_by="reviewer@example.com",
    decided_at=datetime(2026, 8, 5, 5, 20, tzinfo=UTC),
  )

  bundle_path = tmp_path / "release.json"
  plan_path = tmp_path / "plan.json"
  approval_path = tmp_path / "approval.json"
  package_path = tmp_path / "deployment-package.json"
  bundle_path.write_text(
    serialize_architecture_release_bundle(bundle),
    encoding="utf-8",
  )
  plan_path.write_text(
    serialize_environment_promotion_plan(plan),
    encoding="utf-8",
  )
  approval_path.write_text(
    serialize_environment_promotion_approval(approval),
    encoding="utf-8",
  )

  approval_check_stdout = StringIO()
  call_command(
    "elevata_promotion",
    "check",
    plan_path=str(plan_path),
    approval_path=str(approval_path),
    json_output=True,
    stdout=approval_check_stdout,
  )
  assert '"status": "approved"' in approval_check_stdout.getvalue()

  package_stdout = StringIO()
  call_command(
    "elevata_promotion",
    "package",
    bundle_path=str(bundle_path),
    plan_path=str(plan_path),
    approval_path=str(approval_path),
    created_by="packager@example.com",
    output_path=str(package_path),
    stdout=package_stdout,
  )
  assert package_path.exists()
  assert "Package ID:" in package_stdout.getvalue()

  stored_package = package_path.read_text(encoding="utf-8")
  package = deserialize_environment_promotion_deployment_package(
    stored_package
  )
  assert '"artifact_type": "environment_promotion_deployment_package"' in (
    stored_package
  )

  check_stdout = StringIO()
  call_command(
    "elevata_promotion",
    "check",
    package_path=str(package_path),
    target_environment="prod",
    live_target=True,
    json_output=True,
    stdout=check_stdout,
  )
  assert '"live_target_status": "unchanged"' in check_stdout.getvalue()

  drift_team = Team.objects.create(
    name="promotion-command-drift",
    description="Changed after planning",
  )
  with pytest.raises(CommandError) as drift_error:
    call_command(
      "elevata_promotion",
      "check",
      package_path=str(package_path),
      target_environment="prod",
      live_target=True,
    )
  assert drift_error.value.returncode == 4
  drift_team.delete()

  with pytest.raises(CommandError) as package_error:
    call_command(
      "elevata_promotion",
      "apply",
      package_path=str(package_path),
      confirm_package_id="dpkg-0000000000000000",
      target_environment="prod",
      applied_by="deployer@example.com",
      history_dir=str(tmp_path / "invalid-history"),
    )
  assert package_error.value.returncode == 5
  assert not Team.objects.filter(name=team_name).exists()

  history_dir = tmp_path / "history"
  apply_stdout = StringIO()
  call_command(
    "elevata_promotion",
    "apply",
    package_path=str(package_path),
    confirm_package_id=package.package_id,
    target_environment="prod",
    applied_by="deployer@example.com",
    history_dir=str(history_dir),
    stdout=apply_stdout,
  )

  assert Team.objects.filter(name=team_name).exists()
  assert "applied successfully" in apply_stdout.getvalue()
  assert len(tuple(history_dir.glob("prod/*.promotion.json"))) == 1


@pytest.mark.django_db
def test_promotion_plan_command_uses_ci_exit_codes(tmp_path):
  target_snapshot = build_environment_metadata_snapshot(
    environment_label="prod",
    created_by="planner@example.com",
  )
  desired_team = _object(
    "Team",
    {"name": "ci-change"},
    name="ci-change",
    description="CI change",
  )
  source_snapshot = EnvironmentMetadataSnapshot(
    environment_label="dev",
    created_at=datetime(2026, 8, 5, 5, 0, tzinfo=UTC),
    created_by="release@example.com",
    metadata=EnvironmentMetadataPayload(
      objects=(*target_snapshot.metadata.objects, desired_team),
      relationships=target_snapshot.metadata.relationships,
    ),
  )
  bundle = ArchitectureReleaseBundle(
    release_name="ci-test",
    release_version="1.0.0",
    created_at=datetime(2026, 8, 5, 5, 5, tzinfo=UTC),
    created_by="release@example.com",
    snapshot=source_snapshot,
  )
  bundle_path = tmp_path / "release.json"
  target_path = tmp_path / "target.json"
  plan_path = tmp_path / "plan.json"
  bundle_path.write_text(
    serialize_architecture_release_bundle(bundle),
    encoding="utf-8",
  )
  target_path.write_text(
    serialize_environment_metadata_snapshot(target_snapshot),
    encoding="utf-8",
  )

  with pytest.raises(CommandError) as exc_info:
    call_command(
      "elevata_promotion",
      "plan",
      bundle_path=str(bundle_path),
      target_snapshot_path=str(target_path),
      output_path=str(plan_path),
      fail_on_changes=True,
    )

  assert exc_info.value.returncode == 1
  assert plan_path.exists()

  source_without_changes = EnvironmentMetadataSnapshot(
    environment_label="dev",
    created_at=datetime(2026, 8, 5, 5, 10, tzinfo=UTC),
    created_by="release@example.com",
    metadata=target_snapshot.metadata,
  )
  stable_bundle = ArchitectureReleaseBundle(
    release_name="ci-policy-test",
    release_version="1.0.0",
    created_at=datetime(2026, 8, 5, 5, 15, tzinfo=UTC),
    created_by="release@example.com",
    snapshot=source_without_changes,
  )
  stable_bundle_path = tmp_path / "stable-release.json"
  stable_bundle_path.write_text(
    serialize_architecture_release_bundle(stable_bundle),
    encoding="utf-8",
  )

  blocked_objects = []
  for item in target_snapshot.metadata.objects:
    if (
      item.model_name == "TargetSchema"
      and item.fields.get("short_name") == "raw"
    ):
      fields = dict(item.fields)
      fields["schema_name"] = "wrong_raw"
      item = EnvironmentMetadataObject(
        model_name=item.model_name,
        identity=item.identity,
        fields=fields,
      )
    blocked_objects.append(item)
  blocked_snapshot = EnvironmentMetadataSnapshot(
    environment_label="prod",
    created_at=datetime(2026, 8, 5, 5, 20, tzinfo=UTC),
    created_by="planner@example.com",
    metadata=EnvironmentMetadataPayload(
      objects=tuple(blocked_objects),
      relationships=target_snapshot.metadata.relationships,
    ),
  )
  blocked_path = tmp_path / "blocked-target.json"
  blocked_path.write_text(
    serialize_environment_metadata_snapshot(blocked_snapshot),
    encoding="utf-8",
  )
  with pytest.raises(CommandError) as blocked_error:
    call_command(
      "elevata_promotion",
      "plan",
      bundle_path=str(stable_bundle_path),
      target_snapshot_path=str(blocked_path),
      output_path=str(tmp_path / "blocked-plan.json"),
    )
  assert blocked_error.value.returncode == 2

  obsolete_team = _object(
    "Team",
    {"name": "obsolete-ci-team"},
    name="obsolete-ci-team",
    description="Obsolete",
  )
  destructive_snapshot = EnvironmentMetadataSnapshot(
    environment_label="prod",
    created_at=datetime(2026, 8, 5, 5, 25, tzinfo=UTC),
    created_by="planner@example.com",
    metadata=EnvironmentMetadataPayload(
      objects=(*target_snapshot.metadata.objects, obsolete_team),
      relationships=target_snapshot.metadata.relationships,
    ),
  )
  destructive_path = tmp_path / "destructive-target.json"
  destructive_path.write_text(
    serialize_environment_metadata_snapshot(destructive_snapshot),
    encoding="utf-8",
  )
  with pytest.raises(CommandError) as destructive_error:
    call_command(
      "elevata_promotion",
      "plan",
      bundle_path=str(stable_bundle_path),
      target_snapshot_path=str(destructive_path),
      output_path=str(tmp_path / "destructive-plan.json"),
      fail_on_destructive=True,
    )
  assert destructive_error.value.returncode == 3


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
