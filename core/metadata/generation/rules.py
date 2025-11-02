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
  Determines whether a Raw object (table) should be created for a given SourceDataset.
  Rules:
  - integrate must be True
  - generate_raw_table must be True (either on dataset or inherited)
  """
  if not getattr(dataset, "integrate", False):
    return False

  # Check dataset flag
  if getattr(dataset, "generate_raw_table", False):
    return True

  # Optional inheritance: group or system default
  group = getattr(dataset, "group", None)
  system = getattr(dataset, "source_system", None)

  if group and getattr(group, "generate_raw_table", False):
    return True

  if system and getattr(system, "generate_raw_table", False):
    return True

  return False


def default_is_system_managed_for_layer(layer_name: str) -> bool:
  """
  Business rule Iteration 1:
  - Introduction of is_system_managed (from layer rawcore onwards = False by default)
  Interpretation:
    raw      -> True (System fully controls ingestion layer)
    stage    -> True (System transforms from raw)
    rawcore  -> False (starting from rawcore, data is 'owned' / curated)
  To be extended if we later add more layers.
  """
  layer_name = (layer_name or "").lower()
  if layer_name in ("raw", "stage"):
    return True
  if layer_name in ("rawcore"):
    return False
  # default fallback
  return False
