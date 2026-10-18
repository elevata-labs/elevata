"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

This file is part of elevata.
"""

import pytest
from django.core.exceptions import ValidationError

from metadata.forms import TargetDatasetForm
from metadata.models import TargetDataset, TargetDatasetInput, TargetSchema


def _bizcore_schema():
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="bizcore",
    defaults={"schema_name": "bizcore"},
  )
  return schema


@pytest.mark.django_db
def test_target_dataset_input_rejects_active_transitive_cycle():
  schema = _bizcore_schema()
  a = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_cycle_a")
  b = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_cycle_b")
  c = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_cycle_c")

  TargetDatasetInput.objects.create(
    target_dataset=b,
    upstream_target_dataset=a,
    role="primary",
    active=True,
  )
  TargetDatasetInput.objects.create(
    target_dataset=c,
    upstream_target_dataset=b,
    role="primary",
    active=True,
  )

  candidate = TargetDatasetInput(
    target_dataset=a,
    upstream_target_dataset=c,
    role="primary",
    active=True,
  )

  with pytest.raises(ValidationError) as exc_info:
    candidate.full_clean()

  message = str(exc_info.value)
  assert "would create a dataset cycle" in message
  assert "bc_cycle_c" in message
  assert "bc_cycle_a" in message


@pytest.mark.django_db
def test_target_dataset_input_allows_inactive_cycle_edge():
  schema = _bizcore_schema()
  a = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_cycle_inactive_a")
  b = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_cycle_inactive_b")

  TargetDatasetInput.objects.create(
    target_dataset=b,
    upstream_target_dataset=a,
    role="primary",
    active=True,
  )

  candidate = TargetDatasetInput(
    target_dataset=a,
    upstream_target_dataset=b,
    role="primary",
    active=False,
  )
  candidate.full_clean()


@pytest.mark.django_db
def test_target_dataset_form_excludes_transitive_downstream_datasets():
  schema = _bizcore_schema()
  a = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_form_cycle_a")
  b = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_form_cycle_b")
  c = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_form_cycle_c")
  unrelated = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="bc_form_cycle_unrelated",
  )

  TargetDatasetInput.objects.create(
    target_dataset=b,
    upstream_target_dataset=a,
    role="primary",
    active=True,
  )
  TargetDatasetInput.objects.create(
    target_dataset=c,
    upstream_target_dataset=b,
    role="primary",
    active=True,
  )

  form = TargetDatasetForm(instance=a)
  available_ids = set(form.fields["upstream_datasets"].queryset.values_list("pk", flat=True))

  assert a.pk not in available_ids
  assert b.pk not in available_ids
  assert c.pk not in available_ids
  assert unrelated.pk in available_ids
