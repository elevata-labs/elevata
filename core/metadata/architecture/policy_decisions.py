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
from typing import Any, Literal

from metadata.architecture.migration_plan import MigrationAction
from metadata.materialization.policy import MaterializationPolicy


PolicyDecisionStatus = Literal[
  "ALLOW",
  "BLOCKED_BY_POLICY",
  "METADATA_ONLY",
  "REQUIRES_PREFLIGHT",
]


@dataclass(frozen=True)
class PolicyDecision:
  """
  Policy decision for one architecture migration action.
  """
  status: PolicyDecisionStatus
  code: str
  action_type: str
  dataset_key: str
  column_name: str | None = None
  previous_column_name: str | None = None
  previous_dataset_key: str | None = None
  message: str | None = None
  destructive: bool = False

  @property
  def is_blocking(self) -> bool:
    """
    Return True if the decision prevents automatic execution.
    """
    return self.status == "BLOCKED_BY_POLICY"

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "status": self.status,
      "code": self.code,
      "action_type": self.action_type,
      "dataset_key": self.dataset_key,
      "column_name": self.column_name,
      "previous_column_name": self.previous_column_name,
      "previous_dataset_key": self.previous_dataset_key,
      "message": self.message,
      "destructive": self.destructive,
    }


def _is_hist_dataset(dataset_key: str) -> bool:
  """
  Return True if the dataset key points to a history dataset.
  """
  name = (dataset_key or "").rsplit(".", 1)[-1]
  return name.endswith("_hist")


def _decision(
  *,
  action: MigrationAction,
  status: PolicyDecisionStatus,
  code: str,
  message: str,
  destructive: bool = False,
) -> PolicyDecision:
  """
  Build a policy decision from a migration action.
  """
  return PolicyDecision(
    status=status,
    code=code,
    action_type=str(action.action_type),
    dataset_key=action.dataset_key,
    column_name=action.column_name,
    previous_column_name=action.previous_column_name,
    previous_dataset_key=action.previous_dataset_key,
    message=message,
    destructive=destructive,
  )


def evaluate_migration_policy_decisions(
  *,
  actions: tuple[MigrationAction, ...],
  policy: MaterializationPolicy,
) -> tuple[PolicyDecision, ...]:
  """
  Evaluate materialization policy for architecture migration actions.
  """
  decisions: list[PolicyDecision] = []

  for action in actions:
    action_type = str(action.action_type)
    dataset_key = action.dataset_key
    is_hist = _is_hist_dataset(dataset_key)

    if action_type in {"RETIRE_COLUMN", "UNRETIRE_COLUMN"}:
      decisions.append(_decision(
        action=action,
        status="METADATA_ONLY",
        code=f"{action_type}_METADATA_ONLY",
        message="The action changes the architecture contract without physical DDL.",
        destructive=False,
      ))
      continue

    if action_type in {"RENAME_DATASET", "RENAME_COLUMN", "ADD_COLUMN", "CREATE_DATASET"}:
      decisions.append(_decision(
        action=action,
        status="ALLOW",
        code=f"{action_type}_ALLOWED",
        message="The action is allowed by the active materialization policy.",
        destructive=False,
      ))
      continue

    if action_type == "DROP_COLUMN":
      if is_hist and not (policy.allow_auto_drop_columns and policy.allow_auto_drop_hist_columns):
        decisions.append(_decision(
          action=action,
          status="BLOCKED_BY_POLICY",
          code="HIST_COLUMN_DROP_DISABLED",
          message=(
            "Physical drops on history datasets require "
            "ELEVATA_ALLOW_AUTO_DROP_COLUMNS=true and "
            "ELEVATA_ALLOW_AUTO_DROP_HIST_COLUMNS=true."
          ),
          destructive=True,
        ))
        continue

      if not is_hist and not policy.allow_auto_drop_columns:
        decisions.append(_decision(
          action=action,
          status="BLOCKED_BY_POLICY",
          code="COLUMN_DROP_DISABLED",
          message="Physical column drops require ELEVATA_ALLOW_AUTO_DROP_COLUMNS=true.",
          destructive=True,
        ))
        continue

      decisions.append(_decision(
        action=action,
        status="ALLOW",
        code="DROP_COLUMN_ALLOWED",
        message="The physical column drop is allowed by the active materialization policy.",
        destructive=True,
      ))
      continue

    if action_type == "DROP_DATASET":
      decisions.append(_decision(
        action=action,
        status="BLOCKED_BY_POLICY",
        code="DATASET_DROP_DISABLED",
        message="Physical dataset drops are not enabled by the active materialization policy.",
        destructive=True,
      ))
      continue

    if action_type in {"ALTER_COLUMN", "REBUILD_DATASET"}:
      decisions.append(_decision(
        action=action,
        status="REQUIRES_PREFLIGHT",
        code=f"{action_type}_PREFLIGHT_REQUIRED",
        message="The action requires schema preflight validation before execution.",
        destructive=False,
      ))
      continue

    decisions.append(_decision(
      action=action,
      status="REQUIRES_PREFLIGHT",
      code=f"{action_type}_PREFLIGHT_REQUIRED",
      message="The action requires schema preflight validation before execution.",
      destructive=False,
    ))

  return tuple(decisions)