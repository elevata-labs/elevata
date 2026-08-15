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

from metadata.promotion.approval import (
  EnvironmentPromotionApprovalArtifact,
  EnvironmentPromotionApprovalError,
  deserialize_environment_promotion_approval,
  serialize_environment_promotion_approval,
)


ENVIRONMENT_PROMOTION_APPROVAL_DIR_ENV = "ELEVATA_PROMOTION_APPROVAL_DIR"
DEFAULT_ENVIRONMENT_PROMOTION_APPROVAL_DIR = Path(
  ".elevata/promotion/approvals"
)
_PLAN_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
_ENVIRONMENT_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class EnvironmentPromotionApprovalStoreError(ValueError):
  """
  Raised when an immutable promotion approval cannot be stored or loaded.
  """


def resolve_environment_promotion_approval_dir(
  default: str | Path = DEFAULT_ENVIRONMENT_PROMOTION_APPROVAL_DIR,
) -> Path:
  value = os.getenv(ENVIRONMENT_PROMOTION_APPROVAL_DIR_ENV)
  if value and value.strip():
    return Path(value.strip())
  return Path(default)


class EnvironmentPromotionApprovalStore:
  """
  Immutable target-environment-scoped approval artifact store.
  """

  def __init__(self, base_path: str | Path | None = None) -> None:
    self.base_path = (
      Path(base_path)
      if base_path is not None
      else resolve_environment_promotion_approval_dir()
    )

  def approval_file(
    self,
    *,
    target_environment_label: str,
    plan_fingerprint: str,
  ) -> Path:
    environment = _validate_environment_label(target_environment_label)
    fingerprint = _validate_plan_fingerprint(plan_fingerprint)
    return self.base_path / environment / f"{fingerprint}.approval.json"

  def save(self, approval: EnvironmentPromotionApprovalArtifact) -> Path:
    path = self.approval_file(
      target_environment_label=approval.plan.target_environment_label,
      plan_fingerprint=approval.plan.plan_fingerprint,
    )
    if path.exists():
      existing = self.load_file(path)
      if existing.artifact_fingerprint == approval.artifact_fingerprint:
        return path
      raise EnvironmentPromotionApprovalStoreError(
        "The exact Environment Promotion Plan already has a different "
        f"immutable approval artifact: {path}"
      )
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_new_text(path, serialize_environment_promotion_approval(approval))
    return path

  def load_for_plan(
    self,
    *,
    target_environment_label: str,
    plan_fingerprint: str,
  ) -> EnvironmentPromotionApprovalArtifact | None:
    path = self.approval_file(
      target_environment_label=target_environment_label,
      plan_fingerprint=plan_fingerprint,
    )
    if not path.exists():
      return None
    return self.load_file(path)

  def load_all(
    self,
    *,
    target_environment_label: str | None = None,
  ) -> tuple[EnvironmentPromotionApprovalArtifact, ...]:
    if not self.base_path.exists():
      return ()
    if target_environment_label is None:
      paths = sorted(self.base_path.glob("*/*.approval.json"))
    else:
      environment = _validate_environment_label(target_environment_label)
      paths = sorted((self.base_path / environment).glob("*.approval.json"))
    approvals = tuple(self.load_file(path) for path in paths)
    return tuple(sorted(
      approvals,
      key=lambda item: (
        item.plan.target_environment_label,
        item.review.decided_at,
        item.approval_id,
      ),
    ))

  @classmethod
  def load_file(
    cls,
    path: str | Path,
  ) -> EnvironmentPromotionApprovalArtifact:
    artifact_path = Path(path)
    try:
      payload = artifact_path.read_text(encoding="utf-8")
    except OSError as exc:
      raise EnvironmentPromotionApprovalStoreError(
        f"Environment Promotion Approval Artifact could not be read: "
        f"{artifact_path}"
      ) from exc
    try:
      return deserialize_environment_promotion_approval(payload)
    except EnvironmentPromotionApprovalError as exc:
      raise EnvironmentPromotionApprovalStoreError(
        f"Environment Promotion Approval Artifact is invalid: "
        f"{artifact_path}: {exc}"
      ) from exc


def _validate_plan_fingerprint(value: str) -> str:
  normalized = str(value or "").strip()
  if not _PLAN_FINGERPRINT_RE.fullmatch(normalized):
    raise EnvironmentPromotionApprovalStoreError(
      "Promotion plan fingerprint must contain 64 lowercase hex characters."
    )
  return normalized


def _validate_environment_label(value: str) -> str:
  normalized = str(value or "").strip()
  if not _ENVIRONMENT_LABEL_RE.fullmatch(normalized):
    raise EnvironmentPromotionApprovalStoreError(
      "Environment label may contain letters, numbers, dots, dashes and "
      "underscores only."
    )
  return normalized


def _write_new_text(path: Path, content: str) -> None:
  try:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
      handle.write(content)
      handle.flush()
      os.fsync(handle.fileno())
  except FileExistsError as exc:
    raise EnvironmentPromotionApprovalStoreError(
      f"Immutable approval artifact already exists: {path}"
    ) from exc
  except OSError as exc:
    raise EnvironmentPromotionApprovalStoreError(
      f"Immutable approval artifact could not be written: {path}"
    ) from exc
