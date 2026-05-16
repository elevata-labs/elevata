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
import hashlib
import json
from pathlib import Path
from typing import Any

from metadata.architecture.report import ArchitectureChangeReport
from metadata.architecture.report_builder import build_architecture_change_report
from metadata.architecture.state import ArchitectureState
from metadata.architecture.store import ArchitectureStateStore
from metadata.materialization.policy import MaterializationPolicy


class ArchitecturePromotionError(ValueError):
  """
  Raised when an architecture promotion report cannot be built.
  """


@dataclass(frozen=True)
class ArchitecturePromotionReport:
  """
  Deterministic report comparing two architecture states.
  """
  source_label: str
  target_label: str
  source_fingerprint: str
  target_fingerprint: str
  change_report: ArchitectureChangeReport

  @property
  def promotion_fingerprint(self) -> str:
    """
    Return the deterministic fingerprint of this promotion report.
    """
    return _stable_json_hash(self.to_dict(include_fingerprint=False))

  @property
  def has_changes(self) -> bool:
    """
    Return True if the target state differs from the source state.
    """
    return self.change_report.has_changes

  @property
  def is_blocked(self) -> bool:
    """
    Return True if any policy decision blocks automatic execution.
    """
    return self.change_report.is_blocked

  def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    data = {
      "source": {
        "label": self.source_label,
        "fingerprint": self.source_fingerprint,
      },
      "target": {
        "label": self.target_label,
        "fingerprint": self.target_fingerprint,
      },
      "summary": {
        "has_changes": self.has_changes,
        "is_blocked": self.is_blocked,
        "change_report_fingerprint": self.change_report.report_fingerprint,
      },
      "change_report": self.change_report.to_dict(),
    }

    if include_fingerprint:
      data["promotion_fingerprint"] = self.promotion_fingerprint

    return data


def build_architecture_promotion_report(
  *,
  source_state: ArchitectureState,
  target_state: ArchitectureState,
  policy: MaterializationPolicy,
  source_label: str = "source",
  target_label: str = "target",
  relevant_dataset_keys: set[str] | None = None,
  schema_short: str | None = None,
) -> ArchitecturePromotionReport:
  """
  Build a deterministic promotion report from two architecture states.
  """
  change_report = build_architecture_change_report(
    previous_state=source_state,
    current_state=target_state,
    policy=policy,
    relevant_dataset_keys=relevant_dataset_keys,
    schema_short=schema_short,
    target_name=None,
  )

  return ArchitecturePromotionReport(
    source_label=source_label,
    target_label=target_label,
    source_fingerprint=source_state.fingerprint,
    target_fingerprint=target_state.fingerprint,
    change_report=change_report,
  )


def build_architecture_promotion_report_from_files(
  *,
  source_path: str | Path,
  target_path: str | Path,
  policy: MaterializationPolicy,
  source_label: str = "source",
  target_label: str = "target",
  relevant_dataset_keys: set[str] | None = None,
  schema_short: str | None = None,
) -> ArchitecturePromotionReport:
  """
  Build a deterministic promotion report from two architecture state files.
  """
  source_state = ArchitectureStateStore.load_file(source_path)
  if source_state is None:
    raise ArchitecturePromotionError(
      f"Architecture state file could not be read: {source_path}"
    )

  target_state = ArchitectureStateStore.load_file(target_path)
  if target_state is None:
    raise ArchitecturePromotionError(
      f"Architecture state file could not be read: {target_path}"
    )

  return build_architecture_promotion_report(
    source_state=source_state,
    target_state=target_state,
    policy=policy,
    source_label=source_label,
    target_label=target_label,
    relevant_dataset_keys=relevant_dataset_keys,
    schema_short=schema_short,
  )


def _stable_json_hash(value: Any) -> str:
  """
  Return a deterministic SHA-256 hash for a JSON-serializable value.
  """
  payload = json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
    default=str,
  )
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()