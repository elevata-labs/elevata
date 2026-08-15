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

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from json import JSONDecodeError
import json
from typing import Any

from metadata.promotion.approval import (
  EnvironmentPromotionApprovalArtifact,
  EnvironmentPromotionApprovalError,
  check_environment_promotion_approval,
)
from metadata.promotion.canonical import (
  canonical_json,
  canonical_sha256,
  canonicalize_metadata_value,
)
from metadata.promotion.plan import (
  EnvironmentPromotionPlan,
  EnvironmentPromotionPlanError,
  EnvironmentPromotionReadinessStatus,
)
from metadata.promotion.release import (
  ArchitectureReleaseBundle,
  ArchitectureReleaseBundleError,
)
from metadata.promotion.release_validation import (
  ArchitectureReleaseValidationError,
  require_valid_architecture_release_bundle,
)


ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_TYPE = (
  "environment_promotion_deployment_package"
)
ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_VERSION = 1


class EnvironmentPromotionDeploymentPackageError(ValueError):
  """
  Raised when an approved deployment package is structurally invalid.
  """


@dataclass(frozen=True)
class EnvironmentPromotionDeploymentPackage:
  """
  Immutable release, plan and approval payload for one guarded metadata apply.
  """
  bundle: ArchitectureReleaseBundle
  plan: EnvironmentPromotionPlan
  approval: EnvironmentPromotionApprovalArtifact
  created_at: datetime
  created_by: str
  artifact_type: str = (
    ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_TYPE
  )
  artifact_version: int = (
    ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_VERSION
  )

  def __post_init__(self) -> None:
    if self.artifact_type != (
      ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_TYPE
    ):
      raise EnvironmentPromotionDeploymentPackageError(
        f"Unsupported deployment package artifact type: {self.artifact_type}."
      )
    if self.artifact_version != (
      ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_VERSION
    ):
      raise EnvironmentPromotionDeploymentPackageError(
        "Unsupported deployment package artifact version: "
        f"{self.artifact_version}."
      )
    if self.created_at.tzinfo is None:
      raise EnvironmentPromotionDeploymentPackageError(
        "Deployment package created_at must be timezone-aware."
      )
    object.__setattr__(
      self,
      "created_by",
      _require_text(self.created_by, "created_by"),
    )
    _require_valid_components(
      bundle=self.bundle,
      plan=self.plan,
      approval=self.approval,
    )

  @property
  def package_fingerprint(self) -> str:
    return canonical_sha256(self.to_dict(include_identifiers=False))

  @property
  def package_id(self) -> str:
    return f"dpkg-{self.package_fingerprint[:16]}"

  @property
  def target_environment_label(self) -> str:
    return self.plan.target_environment_label

  def to_dict(self, *, include_identifiers: bool = True) -> dict[str, Any]:
    data = {
      "artifact_type": self.artifact_type,
      "artifact_version": self.artifact_version,
      "created_at": _format_datetime(self.created_at),
      "created_by": self.created_by,
      "bundle": self.bundle.to_dict(),
      "plan": self.plan.to_dict(),
      "approval": self.approval.to_dict(),
    }
    if include_identifiers:
      data["package_id"] = self.package_id
      data["package_fingerprint"] = self.package_fingerprint
    return data

  @classmethod
  def from_dict(
    cls,
    data: Mapping[str, Any],
  ) -> EnvironmentPromotionDeploymentPackage:
    _require_exact_keys(
      data,
      expected={
        "artifact_type",
        "artifact_version",
        "package_id",
        "package_fingerprint",
        "created_at",
        "created_by",
        "bundle",
        "plan",
        "approval",
      },
      label="Environment Promotion Deployment Package",
    )
    artifact_version = data.get("artifact_version")
    if not isinstance(artifact_version, int):
      raise EnvironmentPromotionDeploymentPackageError(
        "Deployment package artifact_version must be an integer."
      )
    try:
      package = cls(
        artifact_type=_require_text(
          data.get("artifact_type"),
          "artifact_type",
        ),
        artifact_version=artifact_version,
        created_at=_parse_datetime(data.get("created_at")),
        created_by=_require_text(data.get("created_by"), "created_by"),
        bundle=ArchitectureReleaseBundle.from_dict(
          _require_mapping(data.get("bundle"), "bundle")
        ),
        plan=EnvironmentPromotionPlan.from_dict(
          _require_mapping(data.get("plan"), "plan")
        ),
        approval=EnvironmentPromotionApprovalArtifact.from_dict(
          _require_mapping(data.get("approval"), "approval")
        ),
      )
    except (
      ArchitectureReleaseBundleError,
      ArchitectureReleaseValidationError,
      EnvironmentPromotionPlanError,
      EnvironmentPromotionApprovalError,
    ) as exc:
      raise EnvironmentPromotionDeploymentPackageError(str(exc)) from exc

    _require_match(
      actual=_require_text(
        data.get("package_fingerprint"),
        "package_fingerprint",
      ),
      expected=package.package_fingerprint,
      label="deployment package fingerprint",
    )
    _require_match(
      actual=_require_text(data.get("package_id"), "package_id"),
      expected=package.package_id,
      label="deployment package ID",
    )
    return package


def build_environment_promotion_deployment_package(
  *,
  bundle: ArchitectureReleaseBundle,
  plan: EnvironmentPromotionPlan,
  approval: EnvironmentPromotionApprovalArtifact,
  created_by: str,
  created_at: datetime | None = None,
) -> EnvironmentPromotionDeploymentPackage:
  """
  Build one immutable approved deployment package.
  """
  return EnvironmentPromotionDeploymentPackage(
    bundle=bundle,
    plan=plan,
    approval=approval,
    created_at=created_at or datetime.now(timezone.utc),
    created_by=created_by,
  )


def require_valid_environment_promotion_deployment_package(
  package: EnvironmentPromotionDeploymentPackage,
) -> EnvironmentPromotionDeploymentPackage:
  """
  Return one package after re-validating all exact artifact bindings.
  """
  if not isinstance(package, EnvironmentPromotionDeploymentPackage):
    raise EnvironmentPromotionDeploymentPackageError(
      "package must be an EnvironmentPromotionDeploymentPackage."
    )
  _require_valid_components(
    bundle=package.bundle,
    plan=package.plan,
    approval=package.approval,
  )
  return package


def serialize_environment_promotion_deployment_package(
  package: EnvironmentPromotionDeploymentPackage,
  *,
  pretty: bool = True,
) -> str:
  require_valid_environment_promotion_deployment_package(package)
  if not pretty:
    return canonical_json(package.to_dict())
  return json.dumps(
    canonicalize_metadata_value(package.to_dict()),
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
    allow_nan=False,
  ) + "\n"


def deserialize_environment_promotion_deployment_package(
  payload: str,
) -> EnvironmentPromotionDeploymentPackage:
  try:
    data = json.loads(payload)
  except JSONDecodeError as exc:
    raise EnvironmentPromotionDeploymentPackageError(
      f"Environment Promotion Deployment Package is not valid JSON: {exc}."
    ) from exc
  return EnvironmentPromotionDeploymentPackage.from_dict(
    _require_mapping(data, "Environment Promotion Deployment Package")
  )


def _require_valid_components(
  *,
  bundle: ArchitectureReleaseBundle,
  plan: EnvironmentPromotionPlan,
  approval: EnvironmentPromotionApprovalArtifact,
) -> None:
  try:
    require_valid_architecture_release_bundle(bundle)
  except ArchitectureReleaseValidationError as exc:
    raise EnvironmentPromotionDeploymentPackageError(str(exc)) from exc

  expected_bindings = {
    "release_id": (plan.release_id, bundle.release_id),
    "bundle_fingerprint": (
      plan.bundle_fingerprint,
      bundle.bundle_fingerprint,
    ),
    "source_environment_label": (
      plan.source_environment_label,
      bundle.source_environment_label,
    ),
    "source_snapshot_fingerprint": (
      plan.source_snapshot_fingerprint,
      bundle.source_snapshot_fingerprint,
    ),
    "source_metadata_fingerprint": (
      plan.source_metadata_fingerprint,
      bundle.metadata_fingerprint,
    ),
  }
  mismatches = [
    field_name
    for field_name, (actual, expected) in expected_bindings.items()
    if actual != expected
  ]
  if mismatches:
    raise EnvironmentPromotionDeploymentPackageError(
      "Deployment package plan does not match its release bundle: "
      + ", ".join(sorted(mismatches))
      + "."
    )

  if plan.readiness.status != EnvironmentPromotionReadinessStatus.READY:
    raise EnvironmentPromotionDeploymentPackageError(
      "Deployment package requires a ready Environment Promotion Plan."
    )
  if not plan.mutating_actions:
    raise EnvironmentPromotionDeploymentPackageError(
      "Deployment package requires at least one mutating promotion action."
    )
  if any(item.blocked for item in plan.actions):
    raise EnvironmentPromotionDeploymentPackageError(
      "Deployment package cannot contain blocked promotion actions."
    )

  check = check_environment_promotion_approval(
    plan=plan,
    approval=approval,
  )
  if not check.is_valid:
    raise EnvironmentPromotionDeploymentPackageError(check.message)


def _format_datetime(value: datetime) -> str:
  return (
    value.astimezone(timezone.utc)
    .isoformat(timespec="microseconds")
    .replace("+00:00", "Z")
  )


def _parse_datetime(value: Any) -> datetime:
  text = _require_text(value, "datetime")
  try:
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
  except ValueError as exc:
    raise EnvironmentPromotionDeploymentPackageError(
      f"Invalid deployment package datetime: {value}."
    ) from exc
  if parsed.tzinfo is None:
    raise EnvironmentPromotionDeploymentPackageError(
      "Deployment package datetime must include a timezone."
    )
  return parsed


def _require_text(value: Any, label: str) -> str:
  if not isinstance(value, str):
    raise EnvironmentPromotionDeploymentPackageError(
      f"{label} must be a string."
    )
  normalized = value.strip()
  if not normalized:
    raise EnvironmentPromotionDeploymentPackageError(
      f"{label} must not be empty."
    )
  return normalized


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
  if not isinstance(value, Mapping):
    raise EnvironmentPromotionDeploymentPackageError(
      f"{label} must be an object."
    )
  return value


def _require_exact_keys(
  data: Mapping[str, Any],
  *,
  expected: set[str],
  label: str,
) -> None:
  actual = {str(key) for key in data}
  if actual == expected:
    return
  missing = sorted(expected - actual)
  unexpected = sorted(actual - expected)
  details = []
  if missing:
    details.append("missing: " + ", ".join(missing))
  if unexpected:
    details.append("unexpected: " + ", ".join(unexpected))
  raise EnvironmentPromotionDeploymentPackageError(
    f"{label} fields are invalid: {'; '.join(details)}."
  )


def _require_match(*, actual: Any, expected: Any, label: str) -> None:
  if actual != expected:
    raise EnvironmentPromotionDeploymentPackageError(f"{label} mismatch.")
