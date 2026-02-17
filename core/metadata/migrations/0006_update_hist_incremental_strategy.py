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

from django.db import migrations


def forwards(apps, schema_editor):
  TargetDataset = apps.get_model("metadata", "TargetDataset")
  TargetSchema = apps.get_model("metadata", "TargetSchema")

  rawcore_ids = list(
    TargetSchema.objects
    .filter(short_name="rawcore")
    .values_list("id", flat=True)
  )

  (
    TargetDataset.objects
    .filter(
      target_schema_id__in=rawcore_ids,
      target_dataset_name__endswith="_hist",
      is_system_managed=True,
    )
    .exclude(incremental_strategy="historize")
    .update(
      incremental_strategy="historize",
      historize=False,
      handle_deletes=False,
    )
  )


def backwards(apps, schema_editor):
  TargetDataset = apps.get_model("metadata", "TargetDataset")
  TargetSchema = apps.get_model("metadata", "TargetSchema")

  rawcore_ids = list(
    TargetSchema.objects
    .filter(short_name="rawcore")
    .values_list("id", flat=True)
  )

  (
    TargetDataset.objects
    .filter(
      target_schema_id__in=rawcore_ids,
      target_dataset_name__endswith="_hist",
      is_system_managed=True,
      incremental_strategy="historize",
    )
    .update(incremental_strategy="full")
  )


class Migration(migrations.Migration):
  dependencies = [
    ("metadata", "0005_alter_targetdataset_incremental_strategy_and_more"),
  ]

  operations = [
    migrations.RunPython(forwards, backwards),
  ]
