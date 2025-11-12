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

import pytest

from metadata.models import TargetDatasetInput


@pytest.mark.django_db
def test_target_lineage_integrity(raw_stage_rawcore_datasets):
  """
  Validate that each TargetDataset has exactly the expected lineage:
  Raw <- Source, Stage <- Raw, Rawcore <- Stage
  """
  raw_ds, stage_ds, rawcore_ds = raw_stage_rawcore_datasets

  # 1. Raw dataset: one input from source
  raw_inputs = TargetDatasetInput.objects.filter(target_dataset=raw_ds)
  assert raw_inputs.count() == 1
  assert raw_inputs.first().upstream_target_dataset is None
  assert raw_inputs.first().source_dataset is not None

  # 2. Stage dataset: one input from raw
  stage_inputs = TargetDatasetInput.objects.filter(target_dataset=stage_ds)
  assert stage_inputs.count() == 1
  assert stage_inputs.first().upstream_target_dataset == raw_ds

  # 3. Rawcore dataset: one input from stage
  rawcore_inputs = TargetDatasetInput.objects.filter(target_dataset=rawcore_ds)
  assert rawcore_inputs.count() == 1
  assert rawcore_inputs.first().upstream_target_dataset == stage_ds
