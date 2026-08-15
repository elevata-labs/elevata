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

import hashlib
import json
import re

from django.db import migrations


LEGACY_GENERATED_LINEAGE_RE = re.compile(r"^\d+:\d+(?:,\d+)*$")


def _canonical_json(value):
  return json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
  )


def _source_identity(source_dataset):
  return {
    "source_system": str(source_dataset.source_system.short_name or "").strip(),
    "schema_name": str(source_dataset.schema_name or "").strip(),
    "source_dataset_name": str(source_dataset.source_dataset_name or "").strip(),
  }


def _generated_lineage_key(target_schema, source_datasets):
  source_identities = sorted(
    (_source_identity(item) for item in source_datasets),
    key=lambda item: (
      item["source_system"],
      item["schema_name"],
      item["source_dataset_name"],
    ),
  )
  payload = {
    "source_datasets": source_identities,
    "target_schema": str(target_schema.short_name or "").strip(),
  }
  digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
  return f"generated:{target_schema.short_name}:{digest}"


def _dataset_transport_key(target_dataset):
  lineage_key = str(target_dataset.lineage_key or "").strip()
  if lineage_key:
    return lineage_key
  return (
    f"{target_dataset.target_schema.short_name}."
    f"{target_dataset.target_dataset_name}"
  )


def _reference_fk_lineage_key(reference):
  payload = {
    "referencing_dataset": _dataset_transport_key(
      reference.referencing_dataset
    ),
    "reference_prefix": str(reference.reference_prefix or "").strip(),
    "referenced_dataset": _dataset_transport_key(
      reference.referenced_dataset
    ),
  }
  digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
  return f"fk:{digest}"


def migrate_portable_lineage_keys(apps, schema_editor):
  TargetDataset = apps.get_model("metadata", "TargetDataset")
  TargetDatasetReference = apps.get_model("metadata", "TargetDatasetReference")
  TargetColumn = apps.get_model("metadata", "TargetColumn")
  SourceDataset = apps.get_model("metadata", "SourceDataset")
  database_alias = schema_editor.connection.alias

  old_reference_keys = {}
  references = list(
    TargetDatasetReference.objects.using(database_alias).select_related(
      "referencing_dataset__target_schema",
      "referenced_dataset__target_schema",
    )
  )
  for reference in references:
    old_reference_keys[reference.pk] = _reference_fk_lineage_key(reference)

  datasets = list(
    TargetDataset.objects.using(database_alias)
    .filter(is_system_managed=True)
    .select_related("target_schema")
  )
  for target_dataset in datasets:
    old_lineage_key = str(target_dataset.lineage_key or "").strip()
    if not LEGACY_GENERATED_LINEAGE_RE.fullmatch(old_lineage_key):
      continue

    source_id_part = old_lineage_key.split(":", 1)[1]
    source_ids = [int(value) for value in source_id_part.split(",") if value]
    source_datasets = list(
      SourceDataset.objects.using(database_alias)
      .filter(pk__in=source_ids)
      .select_related("source_system")
    )
    if len(source_datasets) != len(set(source_ids)):
      raise RuntimeError(
        "Cannot migrate generated TargetDataset lineage because one or more "
        f"legacy source dataset IDs are missing: {old_lineage_key}."
      )

    new_lineage_key = _generated_lineage_key(
      target_dataset.target_schema,
      source_datasets,
    )
    TargetDataset.objects.using(database_alias).filter(
      pk=target_dataset.pk
    ).update(lineage_key=new_lineage_key)

  refreshed_references = list(
    TargetDatasetReference.objects.using(database_alias).select_related(
      "referencing_dataset__target_schema",
      "referenced_dataset__target_schema",
    )
  )
  for reference in refreshed_references:
    old_fk_lineage_key = old_reference_keys.get(reference.pk)
    new_fk_lineage_key = _reference_fk_lineage_key(reference)
    candidates = TargetColumn.objects.using(database_alias).filter(
      target_dataset_id=reference.referencing_dataset_id,
      system_role="foreign_key",
      lineage_key__in=(
        old_fk_lineage_key,
        f"fk:{reference.pk}",
      ),
    )
    candidate_ids = list(candidates.values_list("pk", flat=True)[:2])
    if len(candidate_ids) > 1:
      raise RuntimeError(
        "Cannot migrate generated FK lineage because multiple candidate "
        f"columns exist for TargetDatasetReference {reference.pk}."
      )
    if candidate_ids:
      TargetColumn.objects.using(database_alias).filter(
        pk=candidate_ids[0]
      ).update(lineage_key=new_fk_lineage_key)


class Migration(migrations.Migration):

  dependencies = [
    ("metadata", "0011_environment_promotion_contract_foundation"),
  ]

  operations = [
    migrations.RunPython(
      migrate_portable_lineage_keys,
      reverse_code=migrations.RunPython.noop,
    ),
  ]
