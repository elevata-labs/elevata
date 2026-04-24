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
from typing import Literal


MigrationActionType = Literal[
  "RENAME_DATASET",
  "RENAME_COLUMN",
  "CREATE_DATASET",
  "DROP_DATASET",
  "ADD_COLUMN",
  "DROP_COLUMN",
  "ALTER_COLUMN",
  "REBUILD_DATASET",
]


MigrationStrategy = Literal[
  "RENAME_TABLE",
  "RENAME_COLUMN",
  "CREATE_TABLE",
  "DROP_TABLE",
  "ALTER_TABLE",
  "REBUILD",
  "UNKNOWN",
]


@dataclass(frozen=True)
class MigrationAction:
  """
  A semantic migration action derived from ArchitectureDiff.

  This is a preview-only structure in Patch A. It does not execute anything.
  """
  action_type: MigrationActionType
  strategy: MigrationStrategy
  dataset_key: str
  previous_dataset_key: str | None = None
  column_name: str | None = None
  previous_column_name: str | None = None
  reason: str | None = None

  def to_summary_line(self) -> str:
    """
    Return a compact, human-readable summary line.
    """
    if self.action_type == "RENAME_DATASET":
      return f"~ RENAME_DATASET ({self.strategy}): {self.previous_dataset_key} -> {self.dataset_key}"
    if self.action_type == "RENAME_COLUMN":
      return (
        f"~ RENAME_COLUMN ({self.strategy}): "
        f"{self.dataset_key}.{self.previous_column_name} -> {self.column_name}"
      )
    if self.action_type == "CREATE_DATASET":
      return f"+ CREATE_DATASET ({self.strategy}): {self.dataset_key}"
    if self.action_type == "DROP_DATASET":
      return f"- DROP_DATASET ({self.strategy}): {self.dataset_key}"
    if self.action_type == "ADD_COLUMN":
      return f"+ ADD_COLUMN ({self.strategy}): {self.dataset_key}.{self.column_name}"
    if self.action_type == "DROP_COLUMN":
      return f"- DROP_COLUMN ({self.strategy}): {self.dataset_key}.{self.column_name}"
    if self.action_type == "ALTER_COLUMN":
      return f"~ ALTER_COLUMN ({self.strategy}): {self.dataset_key}.{self.column_name}"
    if self.action_type == "REBUILD_DATASET":
      return f"~ REBUILD_DATASET ({self.strategy}): {self.dataset_key}"
    return f"~ ACTION ({self.strategy}): {self.dataset_key}"


@dataclass(frozen=True)
class MigrationPlan:
  """
  A preview-only plan consisting of semantic migration actions.
  """
  actions: tuple[MigrationAction, ...] = ()

  def is_empty(self) -> bool:
    return len(self.actions) == 0

  def to_summary_lines(self) -> list[str]:
    return [a.to_summary_line() for a in self.actions]
