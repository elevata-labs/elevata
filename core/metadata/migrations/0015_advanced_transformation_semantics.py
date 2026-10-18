# Single schema migration for elevata 3.2.0 Advanced Transformation Semantics.

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0014_alter_partialload_descriptions'),
    ]

    operations = [
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
                    ('cte', 'CTE'),
                ],
                help_text='Type of query operator represented by this node.',
                max_length=16,
            ),
        ),
        migrations.CreateModel(
            name='QueryFilterNode',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated_at', models.DateTimeField(auto_now=True, db_index=True)),
                ('predicate', models.JSONField(help_text='Versioned portable predicate contract evaluated against the input node output.')),
                ('created_by', models.ForeignKey(blank=True, editable=False, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
                ('input_node', models.ForeignKey(help_text='Input query node providing rows to filter (wrapped as a subquery).', on_delete=django.db.models.deletion.PROTECT, related_name='used_as_filter_input', to='metadata.querynode')),
                ('node', models.OneToOneField(help_text='Query node header for this FILTER operator.', limit_choices_to={'node_type': 'filter'}, on_delete=django.db.models.deletion.CASCADE, related_name='filter', to='metadata.querynode')),
                ('updated_by', models.ForeignKey(blank=True, editable=False, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'Query filter node',
                'verbose_name_plural': 'Query filter nodes',
                'db_table': 'query_filter_node',
            },
        ),
        migrations.AlterField(
            model_name='queryaggregatemeasure',
            name='function',
            field=models.CharField(
                choices=[
                    ('SUM', 'SUM'),
                    ('COUNT', 'COUNT'),
                    ('MIN', 'MIN'),
                    ('MAX', 'MAX'),
                    ('AVG', 'AVG'),
                    ('STRING_AGG', 'STRING_AGG'),
                ],
                help_text='Aggregate function.',
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name='querywindowcolumn',
            name='function',
            field=models.CharField(
                choices=[
                    ('ROW_NUMBER', 'ROW_NUMBER'),
                    ('RANK', 'RANK'),
                    ('DENSE_RANK', 'DENSE_RANK'),
                    ('NTILE', 'NTILE'),
                    ('LAG', 'LAG'),
                    ('LEAD', 'LEAD'),
                    ('FIRST_VALUE', 'FIRST_VALUE'),
                    ('LAST_VALUE', 'LAST_VALUE'),
                    ('SUM', 'SUM'),
                    ('AVG', 'AVG'),
                    ('MIN', 'MIN'),
                    ('MAX', 'MAX'),
                    ('COUNT', 'COUNT'),
                ],
                help_text='Window function.',
                max_length=32,
            ),
        ),
        migrations.AddField(
            model_name='querywindowcolumn',
            name='frame_type',
            field=models.CharField(
                blank=True,
                choices=[('', 'No explicit frame'), ('ROWS', 'ROWS'), ('RANGE', 'RANGE')],
                default='',
                help_text="Optional explicit window frame type. Leave empty only when the function does not require an explicit frame.",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name='querywindowcolumn',
            name='frame_start',
            field=models.CharField(
                blank=True,
                choices=[
                    ('', 'Select start'),
                    ('UNBOUNDED_PRECEDING', 'UNBOUNDED PRECEDING'),
                    ('PRECEDING', 'n PRECEDING'),
                    ('CURRENT_ROW', 'CURRENT ROW'),
                    ('FOLLOWING', 'n FOLLOWING'),
                ],
                default='',
                help_text='Start boundary of the explicit window frame.',
                max_length=24,
            ),
        ),
        migrations.AddField(
            model_name='querywindowcolumn',
            name='frame_start_offset',
            field=models.PositiveIntegerField(
                blank=True,
                help_text='Non-negative offset used when the frame start is n PRECEDING or n FOLLOWING.',
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='querywindowcolumn',
            name='frame_end',
            field=models.CharField(
                blank=True,
                choices=[
                    ('', 'Select end'),
                    ('PRECEDING', 'n PRECEDING'),
                    ('CURRENT_ROW', 'CURRENT ROW'),
                    ('FOLLOWING', 'n FOLLOWING'),
                    ('UNBOUNDED_FOLLOWING', 'UNBOUNDED FOLLOWING'),
                ],
                default='',
                help_text='End boundary of the explicit window frame.',
                max_length=24,
            ),
        ),
        migrations.AddField(
            model_name='querywindowcolumn',
            name='frame_end_offset',
            field=models.PositiveIntegerField(
                blank=True,
                help_text='Non-negative offset used when the frame end is n PRECEDING or n FOLLOWING.',
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='targetcolumn',
            name='portable_expression',
            field=models.JSONField(
                blank=True,
                editable=False,
                help_text=(
                    'Platform-neutral structured expression contract used to derive this column. '
                    'Column references are resolved through explicit column-level lineage.'
                ),
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='targetcolumninput',
            name='portable_expression',
            field=models.JSONField(
                blank=True,
                editable=False,
                help_text=(
                    'Platform-neutral structured expression contract for this specific input mapping. '
                    'When set, it overrides the column-level expression for this input.'
                ),
                null=True,
            ),
        ),
        migrations.RemoveField(
            model_name='targetdataset',
            name='static_filter',
        ),
        migrations.AddField(
            model_name='sourcedataset',
            name='portable_increment_filter',
            field=models.JSONField(blank=True, editable=False, help_text='Platform-neutral structured filter for incremental source extraction. The filter can reference columns of this SourceDataset and the Delta cutoff runtime value.', null=True),
        ),
        migrations.AddField(
            model_name='sourcedataset',
            name='portable_static_filter',
            field=models.JSONField(blank=True, editable=False, help_text='Platform-neutral structured filter applied to every extraction from this source dataset. The filter can reference columns of this SourceDataset.', null=True),
        ),
        migrations.AlterField(
            model_name='sourcedataset',
            name='increment_filter',
            field=models.CharField(blank=True, help_text='Deprecated source-specific SQL fallback for incremental source filtering. Migrate this filter to Portable increment filter before upgrading to elevata 3.3.', max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='sourcedataset',
            name='static_filter',
            field=models.CharField(blank=True, help_text='Deprecated source-specific SQL fallback for permanent source filtering. Migrate this filter to Portable static filter before upgrading to elevata 3.3.', max_length=255, null=True),
        ),
    ]
