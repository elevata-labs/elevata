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

# core/tests/test_target_lineage.py
import pytest

from metadata.models import TargetDatasetInput


@pytest.mark.django_db
def test_raw_stage_rawcore_lineage(raw_stage_rawcore_datasets):
  raw_ds, stage_ds, rawcore_ds = raw_stage_rawcore_datasets

  # Stage should have raw as its only upstream target
  stage_inputs = TargetDatasetInput.objects.filter(target_dataset=stage_ds)
  assert stage_inputs.count() == 1
  assert stage_inputs.first().upstream_target_dataset == raw_ds

  # Rawcore should have stage as its only upstream target
  rawcore_inputs = TargetDatasetInput.objects.filter(target_dataset=rawcore_ds)
  assert rawcore_inputs.count() == 1
  assert rawcore_inputs.first().upstream_target_dataset == stage_ds
