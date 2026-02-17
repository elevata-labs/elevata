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


StepOp = Literal[
  "ENSURE_SCHEMA",
  "CREATE_TABLE_IF_NOT_EXISTS",
  "ADD_COLUMN",
  "RENAME_DATASET",
  "RENAME_COLUMN",
  "ALTER_COLUMN_TYPE",
  "DROP_TABLE_IF_EXISTS",
  "CREATE_TABLE",
  "INSERT_SELECT",
  "DROP_TABLE",
  "RENAME_TABLE",
  "WARN",
  "BLOCK",
]


@dataclass(frozen=True)
class MaterializationStep:
  op: StepOp
  sql: str | None
  safe: bool
  reason: str


@dataclass
class MaterializationPlan:
  dataset_key: str
  steps: list[MaterializationStep]
  warnings: list[str]
  blocking_errors: list[str]
  requires_backfill: bool = False
  requires_rebuild: bool = False

  def is_blocked(self) -> bool:
    return len(self.blocking_errors) > 0 or any(s.op == "BLOCK" for s in self.steps)
