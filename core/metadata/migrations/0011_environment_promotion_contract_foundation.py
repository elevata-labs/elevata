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
import uuid

import django.core.validators
import django.db.models.deletion
from django.db import migrations, models


def populate_query_node_logical_keys(apps, schema_editor):
  """
  Assign a stable UUID to every existing query node.
  """
  QueryNode = apps.get_model("metadata", "QueryNode")
  database_alias = schema_editor.connection.alias
  for node in (
    QueryNode.objects
    .using(database_alias)
    .filter(logical_key__isnull=True)
    .iterator()
  ):
    node.logical_key = uuid.uuid4()
    node.save(update_fields=["logical_key"])


def validate_source_dataset_group_keys(apps, schema_editor):
  """
  Block the migration when existing group identities are ambiguous.
  """
  SourceDatasetGroup = apps.get_model("metadata", "SourceDatasetGroup")
  database_alias = schema_editor.connection.alias
  duplicates = list(
    SourceDatasetGroup.objects
    .using(database_alias)
    .values("target_short_name", "unified_source_dataset_name")
    .annotate(row_count=models.Count("id"))
    .filter(row_count__gt=1)
    .order_by("target_short_name", "unified_source_dataset_name")
  )
  if duplicates:
    keys = [
      f"{item['target_short_name']}:{item['unified_source_dataset_name']}"
      for item in duplicates
    ]
    raise RuntimeError(
      "Cannot establish the SourceDatasetGroup transport identity because "
      "duplicate logical keys exist: " + ", ".join(keys)
    )


def migrate_reference_fk_lineage_keys(apps, schema_editor):
  """
  Replace local-ID-derived FK lineage keys with canonical reference keys.
  """
  TargetColumn = apps.get_model("metadata", "TargetColumn")
  TargetDatasetReference = apps.get_model(
    "metadata",
    "TargetDatasetReference",
  )

  database_alias = schema_editor.connection.alias
  references = (
    TargetDatasetReference.objects
    .using(database_alias)
    .select_related(
      "referencing_dataset__target_schema",
      "referenced_dataset__target_schema",
    )
  )

  for reference in references.iterator():
    child = reference.referencing_dataset
    parent = reference.referenced_dataset

    child_key = (
      str(child.lineage_key or "").strip()
      or f"{child.target_schema.short_name}.{child.target_dataset_name}"
    )
    parent_key = (
      str(parent.lineage_key or "").strip()
      or f"{parent.target_schema.short_name}.{parent.target_dataset_name}"
    )
    prefix = str(reference.reference_prefix or "").strip()
    transport_key = json.dumps(
      {
        "referencing_dataset": child_key,
        "reference_prefix": prefix,
        "referenced_dataset": parent_key,
      },
      sort_keys=True,
      ensure_ascii=False,
      separators=(",", ":"),
    )
    new_lineage_key = "fk:" + hashlib.sha256(
      transport_key.encode("utf-8")
    ).hexdigest()
    old_lineage_key = f"fk:{reference.pk}"

    TargetColumn.objects.using(database_alias).filter(
      target_dataset=child,
      lineage_key=old_lineage_key,
    ).update(lineage_key=new_lineage_key)


class Migration(migrations.Migration):

  dependencies = [
    ("metadata", "0010_default_member_fallback_enabled"),
  ]

  operations = [
    migrations.RemoveField(
      model_name="targetschema",
      name="database_name",
    ),
    migrations.AlterField(
      model_name="system",
      name="short_name",
      field=models.CharField(
        help_text=(
          "Stable modeled system identifier used for metadata relationships "
          "and runtime profile binding. eg. 'sap', 'nav', 'crm', 'ga4', "
          "'dwhdev', 'dwhprod'."
        ),
        max_length=10,
        unique=True,
        validators=[
          django.core.validators.RegexValidator(
            message=(
              "Must start with a lowercase letter and contain only lowercase "
              "letters and digits. Max length is 10 characters."
            ),
            regex="^[a-z][a-z0-9]{0,9}$",
          )
        ],
      ),
    ),
    migrations.AlterField(
      model_name="targetdataset",
      name="target_schema",
      field=models.ForeignKey(
        help_text=(
          "Which architecture layer / physical schema this dataset belongs "
          "to. Defines schema-level defaults, materialization and governance "
          "expectations."
        ),
        on_delete=django.db.models.deletion.PROTECT,
        related_name="target_datasets",
        to="metadata.targetschema",
      ),
    ),
    migrations.RemoveConstraint(
      model_name="sourcedatasetincrementpolicy",
      name="unique_active_increment_policy_per_env",
    ),
    migrations.AddConstraint(
      model_name="sourcedatasetincrementpolicy",
      constraint=models.UniqueConstraint(
        condition=models.Q(("active", True)),
        fields=("source_dataset", "environment"),
        name="unique_active_increment_policy_per_env",
      ),
    ),
    migrations.RunPython(
      validate_source_dataset_group_keys,
      migrations.RunPython.noop,
    ),
    migrations.AddConstraint(
      model_name="sourcedatasetgroup",
      constraint=models.UniqueConstraint(
        fields=("target_short_name", "unified_source_dataset_name"),
        name="unique_source_dataset_group",
      ),
    ),
    migrations.AddField(
      model_name="querynode",
      name="logical_key",
      field=models.UUIDField(
        editable=False,
        null=True,
      ),
    ),
    migrations.RunPython(
      populate_query_node_logical_keys,
      migrations.RunPython.noop,
    ),
    migrations.AlterField(
      model_name="querynode",
      name="logical_key",
      field=models.UUIDField(
        default=uuid.uuid4,
        editable=False,
        help_text=(
          "Stable transport identity for this query node. "
          "The value is independent of local database IDs."
        ),
        unique=True,
      ),
    ),
    migrations.RunPython(
      migrate_reference_fk_lineage_keys,
      migrations.RunPython.noop,
    ),
  ]
