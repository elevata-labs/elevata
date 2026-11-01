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

from django.db import migrations, models


def normalize_historization_metadata(apps, schema_editor):
  TargetSchema = apps.get_model("metadata", "TargetSchema")
  TargetDataset = apps.get_model("metadata", "TargetDataset")

  TargetSchema.objects.filter(short_name="bizcore").update(default_historize=False)
  TargetDataset.objects.exclude(target_schema__short_name="rawcore").update(
    historize=False,
    handle_deletes=False,
  )


class Migration(migrations.Migration):

  dependencies = [
    ("metadata", "0016_query_semantics_hardening_legacy_cleanup"),
  ]

  operations = [
    migrations.AlterField(
      model_name="targetschema",
      name="default_historize",
      field=models.BooleanField(
        default=False,
        help_text="Default historize value assigned when a dataset is created in this layer.",
      ),
    ),
    migrations.AlterField(
      model_name="targetdataset",
      name="handle_deletes",
      field=models.BooleanField(
        default=False,
        help_text=(
          "Whether deletes in the source should be reflected in this dataset. "
          "Only relevant for Rawcore merge-incremental datasets."
        ),
      ),
    ),
    migrations.AlterField(
      model_name="targetdataset",
      name="historize",
      field=models.BooleanField(
        default=False,
        help_text="Whether this dataset uses the history capability available for its layer.",
      ),
    ),
    migrations.RunPython(normalize_historization_metadata, migrations.RunPython.noop),
  ]
