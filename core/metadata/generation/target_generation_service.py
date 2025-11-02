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

from . import naming, rules
from metadata.models import SourceDatasetGroup, SourceDataset

class TargetGenerationService:
  """
  This service will:
  - find eligible SourceDatasetGroups (rules.group_is_eligible_for_generation)
  - find eligible SourceDatasets without group
  - create TargetDatasets (using naming.* to build table names)
  - create TargetColumns (mapped from SourceColumns)
  - attach lineage_origin etc.

  Iteration 1: just scaffold, no heavy logic yet.
  """

  def __init__(self, *, pepper: str | None = None):
    # pepper will be used later for surrogate keys (Iteration 3)
    self.pepper = pepper

  def get_eligible_groups(self):
    """Return all SourceDatasetGroups eligible for generation."""
    return [
      g for g in SourceDatasetGroup.objects.all()
      if rules.group_is_eligible_for_generation(g)
    ]

  def get_eligible_ungrouped_datasets(self):
    """Return SourceDatasets without a group but integrate=True."""
    return SourceDataset.objects.filter(group__isnull=True, integrate=True)

  def preview_all(self):
    """
    Preview what would be generated, including ungrouped datasets.
    """
    result = {
      "groups": [],
      "ungrouped": [],
    }

    # eligible groups
    for group in self.get_eligible_groups():
      result["groups"].append(self.preview_group(group))

    # ungrouped datasets
    for ds in self.get_eligible_ungrouped_datasets():
      result["ungrouped"].append({
        "dataset_id": ds.id,
        "source_name": ds.name,
        "eligible": True,
      })

    return result
