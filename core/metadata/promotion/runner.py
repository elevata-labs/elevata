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
from typing import Any

from metadata.promotion.apply import apply_environment_promotion_plan
from metadata.promotion.deployment import (
  EnvironmentPromotionDeploymentPackage,
  EnvironmentPromotionDeploymentPackageError,
  require_valid_environment_promotion_deployment_package,
)
from metadata.promotion.record import EnvironmentPromotionRecord
from metadata.promotion.record_store import EnvironmentPromotionRecordStore
from metadata.promotion.snapshot import EnvironmentMetadataSnapshot
from metadata.promotion.snapshot_builder import build_environment_metadata_snapshot


class EnvironmentPromotionRunnerError(ValueError):
  """Raised when a target runner request violates its deployment contract."""


@dataclass(frozen=True)
class EnvironmentPromotionTargetCheck:
  """Read-only live-target check for one exact deployment package."""

  package_id: str
  package_fingerprint: str
  release_id: str
  plan_id: str
  approval_id: str
  target_environment_label: str
  expected_snapshot_fingerprint: str
  expected_metadata_fingerprint: str
  live_snapshot_fingerprint: str
  live_metadata_fingerprint: str

  @property
  def is_unchanged(self) -> bool:
    return (
      self.expected_snapshot_fingerprint == self.live_snapshot_fingerprint
      and self.expected_metadata_fingerprint == self.live_metadata_fingerprint
    )

  def to_dict(self) -> dict[str, Any]:
    return {
      "status": "unchanged" if self.is_unchanged else "drift",
      "is_unchanged": self.is_unchanged,
      "package_id": self.package_id,
      "package_fingerprint": self.package_fingerprint,
      "release_id": self.release_id,
      "plan_id": self.plan_id,
      "approval_id": self.approval_id,
      "target_environment_label": self.target_environment_label,
      "expected_snapshot_fingerprint": self.expected_snapshot_fingerprint,
      "expected_metadata_fingerprint": self.expected_metadata_fingerprint,
      "live_snapshot_fingerprint": self.live_snapshot_fingerprint,
      "live_metadata_fingerprint": self.live_metadata_fingerprint,
    }


def build_runner_snapshot(
  *,
  runtime_environment_label: str,
  created_by: str = "promotion-runner",
) -> EnvironmentMetadataSnapshot:
  """Build the portable snapshot of the runner's locally bound metadata DB."""
  environment = _require_environment(runtime_environment_label)
  return build_environment_metadata_snapshot(
    environment_label=environment,
    created_by=created_by,
  )


def check_environment_promotion_target(
  *,
  package: EnvironmentPromotionDeploymentPackage,
  runtime_environment_label: str,
  created_by: str = "promotion-runner-check",
) -> EnvironmentPromotionTargetCheck:
  """Validate a package and compare it with the current bound target state."""
  environment = _require_package_target(
    package=package,
    runtime_environment_label=runtime_environment_label,
  )
  snapshot = build_runner_snapshot(
    runtime_environment_label=environment,
    created_by=created_by,
  )
  return EnvironmentPromotionTargetCheck(
    package_id=package.package_id,
    package_fingerprint=package.package_fingerprint,
    release_id=package.bundle.release_id,
    plan_id=package.plan.plan_id,
    approval_id=package.approval.approval_id,
    target_environment_label=environment,
    expected_snapshot_fingerprint=package.plan.target_snapshot_fingerprint,
    expected_metadata_fingerprint=package.plan.target_metadata_fingerprint,
    live_snapshot_fingerprint=snapshot.snapshot_fingerprint,
    live_metadata_fingerprint=snapshot.metadata_fingerprint,
  )


def apply_environment_promotion_package(
  *,
  package: EnvironmentPromotionDeploymentPackage,
  runtime_environment_label: str,
  confirm_package_id: str,
  applied_by: str,
  record_store: EnvironmentPromotionRecordStore | None = None,
) -> EnvironmentPromotionRecord:
  """Apply one exact approved package to the runner's locally bound target DB."""
  environment = _require_package_target(
    package=package,
    runtime_environment_label=runtime_environment_label,
  )
  confirmation = str(confirm_package_id or "").strip()
  if confirmation != package.package_id:
    raise EnvironmentPromotionRunnerError(
      "Package confirmation does not match the deployment package ID."
    )
  actor = str(applied_by or "").strip()
  if not actor:
    raise EnvironmentPromotionRunnerError("Promotion apply actor is required.")

  return apply_environment_promotion_plan(
    plan=package.plan,
    bundle=package.bundle,
    approval=package.approval,
    applied_by=actor,
    runtime_environment_label=environment,
    record_store=record_store or EnvironmentPromotionRecordStore(),
  )


def _require_package_target(
  *,
  package: EnvironmentPromotionDeploymentPackage,
  runtime_environment_label: str,
) -> str:
  try:
    require_valid_environment_promotion_deployment_package(package)
  except EnvironmentPromotionDeploymentPackageError as exc:
    raise EnvironmentPromotionRunnerError(str(exc)) from exc
  environment = _require_environment(runtime_environment_label)
  if package.target_environment_label != environment:
    raise EnvironmentPromotionRunnerError(
      "Deployment package target does not match the promotion target runtime."
    )
  return environment


def _require_environment(value: str) -> str:
  environment = str(value or "").strip()
  if not environment:
    raise EnvironmentPromotionRunnerError(
      "Promotion target runtime environment is required."
    )
  return environment
