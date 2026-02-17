"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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
import os


@dataclass(frozen=True)
class MaterializationPolicy:
  # Only sync rawcore+ by default. raw/stage are rebuild-only.
  sync_schema_shorts: set[str]

  # Safety: never auto-drop by default.
  allow_auto_drop_columns: bool

  # type changes are warnings/blocks only (no ALTER).
  allow_type_alter: bool

  # Debug output for planner introspection and decisions.
  debug_plan: bool = False

  # by default, don't allow narrowing datatype changes
  allow_lossy_type_drift: bool = False,


def load_materialization_policy() -> MaterializationPolicy:
  allow_drop = os.getenv("ELEVATA_ALLOW_AUTO_DROP_COLUMNS", "false").lower() in ("1", "true", "yes")
  allow_alter = os.getenv("ELEVATA_ALLOW_TYPE_ALTER", "false").lower() in ("1", "true", "yes")

  return MaterializationPolicy(
    sync_schema_shorts={"rawcore", "bizcore"},
    allow_auto_drop_columns=allow_drop,
    allow_type_alter=allow_alter,
  )
