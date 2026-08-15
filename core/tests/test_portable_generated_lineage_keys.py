"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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

from types import SimpleNamespace

from metadata.portable_keys import build_target_dataset_lineage_key


def _source(*, pk, system_pk, system_short_name, schema_name, name):
  return SimpleNamespace(
    pk=pk,
    source_system=SimpleNamespace(
      pk=system_pk,
      short_name=system_short_name,
    ),
    schema_name=schema_name,
    source_dataset_name=name,
  )


def test_generated_target_lineage_is_independent_of_local_database_ids():
  left_schema = SimpleNamespace(pk=3, short_name="rawcore")
  right_schema = SimpleNamespace(pk=99, short_name="rawcore")
  left_sources = [
    _source(
      pk=6,
      system_pk=1,
      system_short_name="aw",
      schema_name="Sales",
      name="SalesOrderHeader",
    )
  ]
  right_sources = [
    _source(
      pk=800,
      system_pk=77,
      system_short_name="aw",
      schema_name="Sales",
      name="SalesOrderHeader",
    )
  ]

  left = build_target_dataset_lineage_key(left_schema, left_sources)
  right = build_target_dataset_lineage_key(right_schema, right_sources)

  assert left == right
  assert left.startswith("generated:rawcore:")


def test_generated_target_lineage_is_source_order_independent():
  schema = SimpleNamespace(pk=1, short_name="stage")
  first = _source(
    pk=1,
    system_pk=1,
    system_short_name="crm",
    schema_name=None,
    name="customer",
  )
  second = _source(
    pk=2,
    system_pk=2,
    system_short_name="erp",
    schema_name="dbo",
    name="customer",
  )

  assert build_target_dataset_lineage_key(schema, [first, second]) == (
    build_target_dataset_lineage_key(schema, [second, first])
  )
