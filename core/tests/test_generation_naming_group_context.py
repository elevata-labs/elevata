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

import pytest

from metadata.models import (
  System,
  SourceDataset,
  TargetSchema,
  SourceDatasetGroup,
  SourceDatasetGroupMembership,
)
from metadata.generation.naming import build_physical_dataset_name


def _ensure_target_schema(*, short_name: str, schema_name: str, physical_prefix: str) -> TargetSchema:
  """
  Tests must be robust against pre-seeded TargetSchema rows (unique short_name).
  If the schema already exists, we update its relevant fields to match the test.
  """
  obj, created = TargetSchema.objects.get_or_create(
    short_name=short_name,
    defaults={
      "schema_name": schema_name,
      "physical_prefix": physical_prefix,
    },
  )
  # If it already existed (fixtures / other tests), align fields for this test.
  changed = False
  if getattr(obj, "schema_name", None) != schema_name:
    obj.schema_name = schema_name
    changed = True
  if getattr(obj, "physical_prefix", None) != physical_prefix:
    obj.physical_prefix = physical_prefix
    changed = True
  if changed:
    obj.save(update_fields=["schema_name", "physical_prefix"])
  return obj


@pytest.mark.django_db
def test_only_raw_uses_source_system_short_name_in_physical_dataset_name():
  sys = System.objects.create(
    short_name="aw1",
    target_short_name="aw",
  )
  ds = SourceDataset.objects.create(
    source_system=sys,
    source_dataset_name="person",
    integrate=True,
    active=True,
  )

  raw = _ensure_target_schema(short_name="raw", schema_name="raw", physical_prefix="raw")
  stage = _ensure_target_schema(short_name="stage", schema_name="stage", physical_prefix="stg")
  rawcore = _ensure_target_schema(short_name="rawcore", schema_name="rawcore", physical_prefix="rc")

  raw_name = build_physical_dataset_name(raw, ds)
  stage_name = build_physical_dataset_name(stage, ds)
  rawcore_name = build_physical_dataset_name(rawcore, ds)

  assert raw_name == "raw_aw1_person"
  assert stage_name == "stg_aw_person"
  assert rawcore_name == "rc_aw_person"


@pytest.mark.django_db
def test_stage_uses_group_target_short_name_when_group_membership_exists():
  sys = System.objects.create(
    short_name="aw1",
    target_short_name="aw",
  )
  ds = SourceDataset.objects.create(
    source_system=sys,
    source_dataset_name="person",
    integrate=True,
    active=True,
  )

  stage = _ensure_target_schema(short_name="stage", schema_name="stage", physical_prefix="stg")

  grp = SourceDatasetGroup.objects.create(
    target_short_name="sap",
    unified_source_dataset_name="customer",
  )
  SourceDatasetGroupMembership.objects.create(
    group=grp,
    source_dataset=ds,
  )

  name = build_physical_dataset_name(stage, ds)
  assert name == "stg_sap_customer"
