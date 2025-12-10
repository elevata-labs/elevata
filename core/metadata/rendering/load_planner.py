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
  schema = getattr(target_dataset, "target_schema", None)
  schema_short = getattr(schema, "short_name", None)
  dataset_name = getattr(target_dataset, "target_dataset_name", "")
  mat_type = getattr(target_dataset, "materialization_type", "table")
  strategy = getattr(target_dataset, "incremental_strategy", "full")

  # 0) History datasets: no dedicated load strategy yet.
  # They will eventually get their own history load mode.
  if schema_short == "rawcore" and dataset_name.endswith("_hist"):
    return LoadPlan(
      mode="full",
      handle_deletes=False,
      historize=False,
    )

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
    # Append-only loads do not perform delete detection or updates.
    return LoadPlan(
      mode="append",
      handle_deletes=False,
      historize=False,
    )

  if strategy == "merge":
    # Merge is only meaningful for rawcore with keys and incremental_source.
    natural_keys = getattr(target_dataset, "natural_key_fields", None)
    has_keys = bool(natural_keys)
    incremental_source = getattr(target_dataset, "incremental_source", None)
    has_source = incremental_source is not None

    if schema_short == "rawcore" and has_keys and has_source:
      return LoadPlan(
        mode="merge",
        handle_deletes=bool(getattr(target_dataset, "handle_deletes", False)),
        historize=bool(getattr(target_dataset, "historize", False)),
      )

    # Fallback if prerequisites for merge are not met
    return LoadPlan(
      mode="full",
      handle_deletes=False,
      historize=False,
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
