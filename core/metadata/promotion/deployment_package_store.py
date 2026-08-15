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

import os
from pathlib import Path
import re

from metadata.promotion.deployment import (
  EnvironmentPromotionDeploymentPackage,
  EnvironmentPromotionDeploymentPackageError,
  deserialize_environment_promotion_deployment_package,
  serialize_environment_promotion_deployment_package,
)


ENVIRONMENT_PROMOTION_PACKAGE_DIR_ENV = "ELEVATA_PROMOTION_PACKAGE_DIR"
DEFAULT_ENVIRONMENT_PROMOTION_PACKAGE_DIR = Path(".elevata/promotion/packages")
_ENVIRONMENT_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_APPROVAL_ID_RE = re.compile(r"^papr-[0-9a-f]{16}$")


class EnvironmentPromotionDeploymentPackageStoreError(ValueError):
  """Raised when an immutable deployment package cannot be stored or loaded."""


def resolve_environment_promotion_package_dir(
  default: str | Path = DEFAULT_ENVIRONMENT_PROMOTION_PACKAGE_DIR,
) -> Path:
  """Resolve the authoring-side immutable deployment package directory."""
  value = os.getenv(ENVIRONMENT_PROMOTION_PACKAGE_DIR_ENV)
  if value and value.strip():
    return Path(value.strip())
  return Path(default)


class EnvironmentPromotionDeploymentPackageStore:
  """Immutable target-scoped store for approved deployment packages."""

  def __init__(self, base_path: str | Path | None = None) -> None:
    self.base_path = (
      Path(base_path)
      if base_path is not None
      else resolve_environment_promotion_package_dir()
    )

  def package_file(
    self,
    *,
    target_environment_label: str,
    approval_id: str,
  ) -> Path:
    """Return the unique immutable package path for one exact approval."""
    environment = _validate_environment_label(target_environment_label)
    approval = _validate_approval_id(approval_id)
    return self.base_path / environment / f"{approval}.deployment.json"

  def save(self, package: EnvironmentPromotionDeploymentPackage) -> Path:
    """Store one deployment package once for its exact immutable approval."""
    path = self.package_file(
      target_environment_label=package.target_environment_label,
      approval_id=package.approval.approval_id,
    )
    if path.exists():
      existing = self.load_file(path)
      if existing.package_fingerprint == package.package_fingerprint:
        return path
      raise EnvironmentPromotionDeploymentPackageStoreError(
        "The exact Environment Promotion Approval already has a different "
        f"immutable deployment package: {path}"
      )
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_new_text(
      path,
      serialize_environment_promotion_deployment_package(package),
    )
    return path

  def load_for_approval(
    self,
    *,
    target_environment_label: str,
    approval_id: str,
  ) -> EnvironmentPromotionDeploymentPackage | None:
    """Load the immutable deployment package bound to one exact approval."""
    path = self.package_file(
      target_environment_label=target_environment_label,
      approval_id=approval_id,
    )
    if not path.exists():
      return None
    return self.load_file(path)

  def load_file(self, path: str | Path) -> EnvironmentPromotionDeploymentPackage:
    """Load and validate one deployment package file."""
    candidate = Path(path)
    try:
      payload = candidate.read_text(encoding="utf-8")
      package = deserialize_environment_promotion_deployment_package(payload)
    except (OSError, EnvironmentPromotionDeploymentPackageError) as exc:
      raise EnvironmentPromotionDeploymentPackageStoreError(
        f"Cannot load Environment Promotion Deployment Package {candidate}: {exc}"
      ) from exc

    expected = self.package_file(
      target_environment_label=package.target_environment_label,
      approval_id=package.approval.approval_id,
    )
    try:
      if candidate.resolve() != expected.resolve():
        raise EnvironmentPromotionDeploymentPackageStoreError(
          "Deployment package path does not match its immutable target/approval "
          f"identity: {candidate}"
        )
    except OSError as exc:
      raise EnvironmentPromotionDeploymentPackageStoreError(
        f"Cannot validate deployment package path {candidate}: {exc}"
      ) from exc
    return package


def _validate_environment_label(value: str) -> str:
  label = str(value or "").strip()
  if not _ENVIRONMENT_LABEL_RE.fullmatch(label):
    raise EnvironmentPromotionDeploymentPackageStoreError(
      f"Promotion target environment label is invalid: {label or '<empty>'}."
    )
  return label


def _validate_approval_id(value: str) -> str:
  approval_id = str(value or "").strip()
  if not _APPROVAL_ID_RE.fullmatch(approval_id):
    raise EnvironmentPromotionDeploymentPackageStoreError(
      f"Environment Promotion Approval ID is invalid: {approval_id or '<empty>'}."
    )
  return approval_id


def _write_new_text(path: Path, payload: str) -> None:
  """Write one immutable text artifact without replacing existing content."""
  try:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
      handle.write(payload)
  except FileExistsError as exc:
    raise EnvironmentPromotionDeploymentPackageStoreError(
      f"Deployment package path already exists: {path}"
    ) from exc
  except OSError as exc:
    raise EnvironmentPromotionDeploymentPackageStoreError(
      f"Cannot write deployment package {path}: {exc}"
    ) from exc
