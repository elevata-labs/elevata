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
from typing import Any, Literal

from metadata.architecture.diff import ColumnChange, DatasetChange
from metadata.architecture.migration_plan import MigrationAction
from metadata.architecture.policy_decisions import PolicyDecision


ArchitectureReportScopeMode = Literal["all", "scoped"]


@dataclass(frozen=True)
class ArchitectureReportScope:
  """
  Dataset scope used to build an architecture change report.
  """
  mode: ArchitectureReportScopeMode
  dataset_keys: tuple[str, ...]
  schema_short: str | None = None
  target_name: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "mode": self.mode,
      "schema_short": self.schema_short,
      "target_name": self.target_name,
      "dataset_keys": list(self.dataset_keys),
    }


@dataclass(frozen=True)
class ArchitectureReportSummary:
  """
  Aggregated counts for an architecture change report.
  """
  dataset_change_count: int
  column_change_count: int
  migration_action_count: int
  policy_decision_count: int
  blocking_policy_decision_count: int

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "dataset_change_count": self.dataset_change_count,
      "column_change_count": self.column_change_count,
      "migration_action_count": self.migration_action_count,
      "policy_decision_count": self.policy_decision_count,
      "blocking_policy_decision_count": self.blocking_policy_decision_count,
    }


@dataclass(frozen=True)
class ArchitectureChangeReport:
  """
  Deterministic report describing architecture change intent and policy decisions.
  """
  scope: ArchitectureReportScope
  previous_fingerprint: str | None
  current_fingerprint: str
  has_changes: bool
  dataset_changes: tuple[DatasetChange, ...]
  column_changes: tuple[ColumnChange, ...]
  migration_actions: tuple[MigrationAction, ...]
  policy_decisions: tuple[PolicyDecision, ...]
  summary: ArchitectureReportSummary


  @property
  def report_fingerprint(self) -> str:
    """
    Return the deterministic fingerprint of this architecture change report.
    """
    return _stable_json_hash(self.to_dict(include_fingerprint=False))

  @property
  def is_blocked(self) -> bool:
    """
    Return True if any policy decision blocks automatic execution.
    """
    return any(decision.is_blocking for decision in self.policy_decisions)

  def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    data = {
      "scope": self.scope.to_dict(),
      "state": {
        "previous_fingerprint": self.previous_fingerprint,
        "current_fingerprint": self.current_fingerprint,
        "has_changes": self.has_changes,
      },
      "summary": self.summary.to_dict(),
      "dataset_changes": [
        _dataset_change_to_dict(change)
        for change in self.dataset_changes
      ],
      "column_changes": [
        _column_change_to_dict(change)
        for change in self.column_changes
      ],
      "migration_actions": [
        _migration_action_to_dict(action)
        for action in self.migration_actions
      ],
      "policy_decisions": [
        decision.to_dict()
        for decision in self.policy_decisions
      ],
      "is_blocked": self.is_blocked,
    }

    if include_fingerprint:
      data["report_fingerprint"] = self.report_fingerprint

    return data


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


def _dataset_change_to_dict(change: DatasetChange) -> dict[str, Any]:
  """
  Return a deterministic dictionary for a dataset change.
  """
  return {
    "dataset_key": change.dataset_key,
    "change_type": change.change_type,
    "dataset_name": change.dataset_name,
    "previous_dataset_name": change.previous_dataset_name,
    "details": dict(change.details or {}),
  }


def _column_change_to_dict(change: ColumnChange) -> dict[str, Any]:
  """
  Return a deterministic dictionary for a column change.
  """
  return {
    "dataset_key": change.dataset_key,
    "change_type": change.change_type,
    "column_name": change.column_name,
    "previous_column_name": change.previous_column_name,
    "details": dict(change.details or {}),
  }


def _migration_action_to_dict(action: MigrationAction) -> dict[str, Any]:
  """
  Return a deterministic dictionary for a migration action.
  """
  return {
    "action_type": action.action_type,
    "strategy": action.strategy,
    "dataset_key": action.dataset_key,
    "previous_dataset_key": action.previous_dataset_key,
    "column_name": action.column_name,
    "previous_column_name": action.previous_column_name,
    "reason": action.reason,
    "summary": action.to_summary_line(),
  }