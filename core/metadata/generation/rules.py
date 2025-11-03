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

def group_is_eligible_for_generation(group) -> bool:
  """
  A SourceDatasetGroup is eligible if it has at least one SourceDataset with integrate=True.
  """
  return any(
    getattr(ds, "integrate", False) is True
    for ds in group.sourcedataset_set.all()
  )


def dataset_creates_raw_object(dataset) -> bool:
  """
  Decide if this SourceDataset should get a RAW table.

  Rules:
  1. integrate must be True on the dataset itself.
     - integrate=False  → never
     - integrate=None   → treat as False (must be explicit True)

  2. effective_generate_raw_table must be True:
     - if dataset.generate_raw_table is True → True
     - elif dataset.generate_raw_table is False → False
     - elif dataset.generate_raw_table is None → inherit from dataset.source_system.generate_raw_tables
  """

  # 1. integrate must be explicitly True
  if getattr(dataset, "integrate", None) is not True:
    return False

  # 2. resolve generate_raw_table effective flag
  ds_flag = getattr(dataset, "generate_raw_table", None)

  if ds_flag is True:
    eff_generate = True
  elif ds_flag is False:
    eff_generate = False
  else:
    # None → inherit from source_system
    src_sys = getattr(dataset, "source_system", None)
    eff_generate = bool(getattr(src_sys, "generate_raw_tables", False))

  return eff_generate is True

