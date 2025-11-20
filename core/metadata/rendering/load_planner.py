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

from dataclasses import dataclass
from typing import Literal

from metadata.models import TargetDataset

LoadMode = Literal[
  "view_only",
  "full",
  "append",
  "merge",
  "snapshot",
]


@dataclass
class LoadPlan:
  """
  High-level load plan for a single TargetDataset.

  This is an abstract description that can be used by:
  - SQL generation (which templates to pick)
  - orchestration (which steps to run)
  """
  mode: LoadMode
  handle_deletes: bool = False
  historize: bool = False


def build_load_plan(target_dataset: TargetDataset) -> LoadPlan:
  """
  Decide how a dataset should be loaded based on its metadata.

  This does NOT generate any SQL yet, it only describes the load mode.
  """

  mat_type = getattr(target_dataset, "materialization_type", "table")
  strategy = getattr(target_dataset, "incremental_strategy", "full")

  # 1) Views / logical models: no separate load step, just SELECT/VIEW
  if mat_type == "view":
    return LoadPlan(
      mode="view_only",
      handle_deletes=False,
      historize=False,
    )

  # 2) Tables: decide based on incremental strategy
  if strategy == "full":
    return LoadPlan(
      mode="full",
      handle_deletes=False,
      historize=False,
    )

  if strategy == "append":
    return LoadPlan(
      mode="append",
      handle_deletes=False,
      historize=False,
    )

  if strategy == "merge":
    return LoadPlan(
      mode="merge",
      handle_deletes=bool(getattr(target_dataset, "handle_deletes", False)),
      historize=bool(getattr(target_dataset, "historize", False)),
    )

  if strategy == "snapshot":
    return LoadPlan(
      mode="snapshot",
      handle_deletes=False,
      historize=False,
    )

  # Fallback: treat unknown strategy as full refresh
  return LoadPlan(
    mode="full",
    handle_deletes=False,
    historize=False,
  )
