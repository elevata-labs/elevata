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

# Single schema migration for elevata 3.3.0 Query Semantics Hardening & Legacy Cleanup.

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0015_advanced_transformation_semantics'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='sourcedataset',
            name='static_filter',
        ),
        migrations.RemoveField(
            model_name='sourcedataset',
            name='increment_filter',
        ),
        migrations.RemoveField(
            model_name='targetcolumn',
            name='manual_expression',
        ),
        migrations.RemoveField(
            model_name='targetcolumninput',
            name='manual_expression',
        ),
        migrations.AlterField(
            model_name='querynode',
            name='node_type',
            field=models.CharField(
                choices=[
                    ('select', 'Select'),
                    ('aggregate', 'Aggregate'),
                    ('union', 'Union'),
                    ('window', 'Window'),
                    ('filter', 'Filter'),
                ],
                help_text='Type of query operator represented by this node.',
                max_length=16,
            ),
        ),
        migrations.AlterField(
            model_name='sourcedataset',
            name='portable_static_filter',
            field=models.JSONField(
                blank=True,
                editable=False,
                help_text=(
                    'Filter applied to every extraction from this source dataset. '
                    'It can reference columns of this SourceDataset.'
                ),
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name='sourcedataset',
            name='portable_increment_filter',
            field=models.JSONField(
                blank=True,
                editable=False,
                help_text=(
                    'Filter that defines which rows are included in incremental extraction. '
                    'It can reference columns of this SourceDataset and the Delta cutoff runtime value.'
                ),
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name='targetdataset',
            name='incremental_source',
            field=models.ForeignKey(
                blank=True,
                help_text=(
                    'If set, this target dataset inherits incremental window logic and delete detection scope '
                    'from the referenced SourceDataset. Its increment filter defines which records '
                    'are in scope for delta loading and delete detection.'
                ),
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name='incremental_targets',
                to='metadata.sourcedataset',
            ),
        ),
        migrations.AlterField(
            model_name='queryselectnode',
            name='use_dataset_definition',
            field=models.BooleanField(
                default=True,
                help_text=(
                    'When true, build the SELECT from the owning TargetDataset definition, including joins, '
                    'columns, and expressions.'
                ),
            ),
        ),
        migrations.AlterField(
            model_name='targetcolumn',
            name='portable_expression',
            field=models.JSONField(
                blank=True,
                editable=False,
                help_text=(
                    'Expression used to calculate this target column. Its result becomes the target column value '
                    'and replaces the original input value.'
                ),
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name='targetcolumninput',
            name='portable_expression',
            field=models.JSONField(
                blank=True,
                editable=False,
                help_text=(
                    'Expression used to calculate this target column for this specific input mapping. '
                    'Its result replaces the value from this input and takes precedence over the target column expression.'
                ),
                null=True,
            ),
        ),
    ]
