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

from metadata.promotion.record import (
  EnvironmentPromotionRecord,
  EnvironmentPromotionRecordError,
  deserialize_environment_promotion_record,
  serialize_environment_promotion_record,
)


ENVIRONMENT_PROMOTION_HISTORY_DIR_ENV = "ELEVATA_PROMOTION_HISTORY_DIR"
DEFAULT_ENVIRONMENT_PROMOTION_HISTORY_DIR = Path(
  ".elevata/promotion/history"
)
_RECORD_ID_RE = re.compile(r"^prom-[0-9a-f]{16}$")
_ENVIRONMENT_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class EnvironmentPromotionRecordStoreError(ValueError):
  """
  Raised when immutable promotion history cannot be stored or loaded.
  """


def resolve_environment_promotion_history_dir(
  default: str | Path = DEFAULT_ENVIRONMENT_PROMOTION_HISTORY_DIR,
) -> Path:
  value = os.getenv(ENVIRONMENT_PROMOTION_HISTORY_DIR_ENV)
  if value and value.strip():
    return Path(value.strip())
  return Path(default)


class EnvironmentPromotionRecordStore:
  """
  Immutable target-environment-scoped successful promotion history.
  """

  def __init__(self, base_path: str | Path | None = None) -> None:
    self.base_path = (
      Path(base_path)
      if base_path is not None
      else resolve_environment_promotion_history_dir()
    )

  def record_file(self, record: EnvironmentPromotionRecord) -> Path:
    environment = _validate_environment_label(
      record.target_environment_label
    )
    record_id = _validate_record_id(record.record_id)
    return self.base_path / environment / f"{record_id}.promotion.json"

  def save(self, record: EnvironmentPromotionRecord) -> Path:
    path = self.record_file(record)
    if path.exists():
      existing = self.load_file(path)
      if existing.record_fingerprint == record.record_fingerprint:
        return path
      raise EnvironmentPromotionRecordStoreError(
        f"Promotion history path already contains different content: {path}"
      )
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_new_text(path, serialize_environment_promotion_record(record))
    return path

  def load(
    self,
    *,
    target_environment_label: str,
    record_id: str,
  ) -> EnvironmentPromotionRecord | None:
    environment = _validate_environment_label(target_environment_label)
    normalized_record_id = _validate_record_id(record_id)
    path = (
      self.base_path
      / environment
      / f"{normalized_record_id}.promotion.json"
    )
    if not path.exists():
      return None
    return self.load_file(path)

  def load_all(
    self,
    *,
    target_environment_label: str | None = None,
  ) -> tuple[EnvironmentPromotionRecord, ...]:
    if not self.base_path.exists():
      return ()
    if target_environment_label is None:
      paths = sorted(self.base_path.glob("*/*.promotion.json"))
    else:
      environment = _validate_environment_label(target_environment_label)
      paths = sorted((self.base_path / environment).glob("*.promotion.json"))
    records = tuple(self.load_file(path) for path in paths)
    return tuple(sorted(
      records,
      key=lambda item: (
        item.target_environment_label,
        item.applied_at,
        item.record_id,
      ),
    ))

  @classmethod
  def load_file(cls, path: str | Path) -> EnvironmentPromotionRecord:
    record_path = Path(path)
    try:
      payload = record_path.read_text(encoding="utf-8")
    except OSError as exc:
      raise EnvironmentPromotionRecordStoreError(
        f"Environment Promotion Record could not be read: {record_path}"
      ) from exc
    try:
      return deserialize_environment_promotion_record(payload)
    except EnvironmentPromotionRecordError as exc:
      raise EnvironmentPromotionRecordStoreError(
        f"Environment Promotion Record is invalid: {record_path}: {exc}"
      ) from exc


def _validate_record_id(value: str) -> str:
  normalized = str(value or "").strip()
  if not _RECORD_ID_RE.fullmatch(normalized):
    raise EnvironmentPromotionRecordStoreError(
      "Promotion record ID must match prom-<16 lowercase hex characters>."
    )
  return normalized


def _validate_environment_label(value: str) -> str:
  normalized = str(value or "").strip()
  if not _ENVIRONMENT_LABEL_RE.fullmatch(normalized):
    raise EnvironmentPromotionRecordStoreError(
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
    raise EnvironmentPromotionRecordStoreError(
      f"Immutable promotion record already exists: {path}"
    ) from exc
  except OSError as exc:
    raise EnvironmentPromotionRecordStoreError(
      f"Immutable promotion record could not be written: {path}"
    ) from exc
