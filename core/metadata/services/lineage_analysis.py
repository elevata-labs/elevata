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

from collections import deque
from metadata.models import TargetDatasetInput, TargetDataset


def _drop_direct_level(levels: dict[int, list[TargetDataset]]) -> dict[int, list[TargetDataset]]:
  """
  Remove level 1 (direct dependencies) and shift remaining levels down by 1.
  This avoids duplication with the 'Upstream inputs' / 'Downstream consumers' tables.
  """
  if not levels:
    return {}
  out: dict[int, list[TargetDataset]] = {}
  for lvl, items in levels.items():
    if lvl <= 1:
      continue
    out[lvl - 1] = items
  return out


def collect_upstream_targets(
  start: TargetDataset,
  depth: int,
) -> dict[int, list[TargetDataset]]:
  # Execution lineage only (no semantic references)
  levels: dict[int, list[TargetDataset]] = {}
  seen: set[int] = {start.id}

  queue = deque([(start, 0)])

  while queue:
    current, level = queue.popleft()
    if level >= depth:
      continue

    links = (
      current.input_links
      .filter(active=True, upstream_target_dataset__isnull=False)
      .select_related("upstream_target_dataset", "upstream_target_dataset__target_schema")
    )

    next_level = []
    for link in links:
      ds = link.upstream_target_dataset
      if ds.id in seen:
        continue
      seen.add(ds.id)
      next_level.append(ds)
      queue.append((ds, level + 1))

    if next_level:
      levels.setdefault(level + 1, []).extend(
        sorted(next_level, key=lambda d: d.target_dataset_name)
      )

  return levels


def collect_downstream_targets(
  start: TargetDataset,
  depth: int,
) -> dict[int, list[TargetDataset]]:
  levels: dict[int, list[TargetDataset]] = {}
  seen: set[int] = {start.id}

  queue = deque([(start, 0)])

  while queue:
    current, level = queue.popleft()
    if level >= depth:
      continue

    links = (
      TargetDatasetInput.objects
      .filter(active=True, upstream_target_dataset=current)
      .select_related("target_dataset", "target_dataset__target_schema")
    )

    next_level = []
    for link in links:
      ds = link.target_dataset
      if ds.id in seen:
        continue
      seen.add(ds.id)
      next_level.append(ds)
      queue.append((ds, level + 1))

    if next_level:
      levels.setdefault(level + 1, []).extend(
        sorted(next_level, key=lambda d: d.target_dataset_name)
      )

  return levels


def collect_upstream_targets_extra(start: TargetDataset, extra_depth: int) -> dict[int, list[TargetDataset]]:
  """
  extra_depth=1 -> show one level *beyond* direct upstream inputs
  """
  levels = collect_upstream_targets(start, extra_depth + 1)
  return _drop_direct_level(levels)


def collect_downstream_targets_extra(start: TargetDataset, extra_depth: int) -> dict[int, list[TargetDataset]]:
  """
  extra_depth=1 -> show one level *beyond* direct downstream consumers
  """
  levels = collect_downstream_targets(start, extra_depth + 1)
  return _drop_direct_level(levels)