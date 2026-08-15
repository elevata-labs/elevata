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

from metadata.promotion.release import (
  ArchitectureReleaseBundle,
  ArchitectureReleaseBundleError,
  deserialize_architecture_release_bundle,
  serialize_architecture_release_bundle,
)
from metadata.promotion.release_validation import (
  ArchitectureReleaseValidationError,
  require_valid_architecture_release_bundle,
)


ARCHITECTURE_RELEASE_DIR_ENV = "ELEVATA_PROMOTION_RELEASE_DIR"
DEFAULT_ARCHITECTURE_RELEASE_DIR = Path(".elevata/promotion/releases")
_RELEASE_ID_RE = re.compile(r"^rel-[0-9a-f]{16}$")


class ArchitectureReleaseStoreError(ValueError):
  """
  Raised when an immutable release cannot be stored or loaded.
  """


def resolve_architecture_release_dir(
  default: str | Path = DEFAULT_ARCHITECTURE_RELEASE_DIR,
) -> Path:
  """
  Resolve the Architecture Release Store directory.
  """
  value = os.getenv(ARCHITECTURE_RELEASE_DIR_ENV)
  if value and value.strip():
    return Path(value.strip())
  return Path(default)


class ArchitectureReleaseStore:
  """
  File-based immutable store for Architecture Release Bundles.
  """

  def __init__(self, base_path: str | Path | None = None) -> None:
    self.base_path = (
      Path(base_path)
      if base_path is not None
      else resolve_architecture_release_dir()
    )

  def release_file(self, release_id: str) -> Path:
    """
    Return the safe store path for one release identifier.
    """
    normalized = _validate_release_id(release_id)
    return self.base_path / f"{normalized}.release.json"

  def save(self, bundle: ArchitectureReleaseBundle) -> Path:
    """
    Store a validated bundle without permitting coordinate reuse.
    """
    try:
      require_valid_architecture_release_bundle(bundle)
    except ArchitectureReleaseValidationError as exc:
      raise ArchitectureReleaseStoreError(str(exc)) from exc

    for existing in self.load_all():
      if existing.coordinate != bundle.coordinate:
        continue
      if existing.bundle_fingerprint == bundle.bundle_fingerprint:
        return self.release_file(existing.release_id)
      raise ArchitectureReleaseStoreError(
        "Architecture release coordinate is already used by a different "
        f"immutable bundle: {bundle.release_name} {bundle.release_version}."
      )

    path = self.release_file(bundle.release_id)
    if path.exists():
      existing = self.load_file(path)
      if existing.bundle_fingerprint == bundle.bundle_fingerprint:
        return path
      raise ArchitectureReleaseStoreError(
        f"Architecture release path already contains different content: {path}"
      )

    self.base_path.mkdir(parents=True, exist_ok=True)
    _write_new_text(path, serialize_architecture_release_bundle(bundle))
    return path

  def load(self, release_id: str) -> ArchitectureReleaseBundle | None:
    """
    Load one release by ID, returning None when it is not stored.
    """
    path = self.release_file(release_id)
    if not path.exists():
      return None
    return self.load_file(path)

  def require(self, release_id: str) -> ArchitectureReleaseBundle:
    """
    Load one release or raise a deterministic not-found error.
    """
    bundle = self.load(release_id)
    if bundle is None:
      raise ArchitectureReleaseStoreError(
        f"Architecture release is not stored: {release_id}."
      )
    return bundle

  def load_all(self) -> tuple[ArchitectureReleaseBundle, ...]:
    """
    Load all valid stored releases in deterministic order.
    """
    if not self.base_path.exists():
      return ()
    bundles = []
    for path in sorted(self.base_path.glob("rel-*.release.json")):
      bundles.append(self.load_file(path))
    return tuple(sorted(
      bundles,
      key=lambda item: (
        item.release_name,
        item.release_version,
        item.created_at,
        item.release_id,
      ),
    ))

  def export(
    self,
    release_id: str,
    output_path: str | Path,
    *,
    pretty: bool = True,
  ) -> Path:
    """
    Export one stored immutable bundle to a new file.
    """
    bundle = self.require(release_id)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = serialize_architecture_release_bundle(bundle, pretty=pretty)
    if not pretty:
      rendered += "\n"
    _write_new_text(path, rendered)
    return path

  @classmethod
  def load_file(cls, path: str | Path) -> ArchitectureReleaseBundle:
    """
    Load and validate one release bundle file.
    """
    release_path = Path(path)
    try:
      payload = release_path.read_text(encoding="utf-8")
    except OSError as exc:
      raise ArchitectureReleaseStoreError(
        f"Architecture Release Bundle could not be read: {release_path}"
      ) from exc
    try:
      bundle = deserialize_architecture_release_bundle(payload)
      require_valid_architecture_release_bundle(bundle)
    except (
      ArchitectureReleaseBundleError,
      ArchitectureReleaseValidationError,
    ) as exc:
      raise ArchitectureReleaseStoreError(
        f"Architecture Release Bundle is invalid: {release_path}: {exc}"
      ) from exc
    return bundle


def _validate_release_id(value: str) -> str:
  normalized = str(value or "").strip()
  if not _RELEASE_ID_RE.fullmatch(normalized):
    raise ArchitectureReleaseStoreError(
      "Architecture release ID must match rel-<16 lowercase hex characters>."
    )
  return normalized


def _write_new_text(path: Path, content: str) -> None:
  try:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
      handle.write(content)
      handle.flush()
      os.fsync(handle.fileno())
  except FileExistsError as exc:
    raise ArchitectureReleaseStoreError(
      f"Immutable artifact output already exists: {path}"
    ) from exc
  except OSError as exc:
    raise ArchitectureReleaseStoreError(
      f"Immutable artifact could not be written: {path}"
    ) from exc
