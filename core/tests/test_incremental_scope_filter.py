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

import re

from metadata.rendering.dialects.duckdb import DuckDBDialect
from metadata.rendering.load_sql import _build_incremental_scope_filter_for_target


def _increment_contract(*, include_deleted_flag: bool = False):
  parts = [
    {
      "type": "comparison",
      "operator": ">=",
      "left": {"type": "column", "name": "ModifiedDate"},
      "right": {"type": "runtime_value", "name": "delta_cutoff"},
    },
  ]
  if include_deleted_flag:
    parts.append({
      "type": "comparison",
      "operator": "=",
      "left": {"type": "column", "name": "is_deleted_flag"},
      "right": {"type": "literal", "value": 0},
    })
  expression = parts[0] if len(parts) == 1 else {
    "type": "boolean",
    "operator": "AND",
    "parts": parts,
  }
  return {"version": 1, "expression": expression}


# ─────────────────────────────────────────────────────────────
# Fakes / Test Doubles
# ─────────────────────────────────────────────────────────────

class FakeDialect(DuckDBDialect):
  def render_identifier(self, name: str) -> str:
    return f'"{name}"'


class FakeSourceDataset:
  def __init__(self, *, id, incremental, portable_increment_filter):
    self.id = id
    self.incremental = incremental
    self.portable_increment_filter = portable_increment_filter


class FakeUpstreamTargetColumn:
  def __init__(self, target_column_name):
    self.target_column_name = target_column_name


class FakeLink:
  def __init__(self, *, upstream_target_column=None):
    self.upstream_target_column = upstream_target_column


class FakeLinkQS:
  def __init__(self, links):
    self._links = links

  def filter(self, **kwargs):
    # Ignore filter conditions; behave like a permissive Django QS
    return self

  def select_related(self, *args, **kwargs):
    return self

  def order_by(self, *args, **kwargs):
    return self

  def first(self):
    return self._links[0] if self._links else None

  def all(self):
    return list(self._links)


class FakeTargetColumn:
  def __init__(self, *, pk, target_column_name, links):
    self.pk = pk
    self.target_column_name = target_column_name
    self.input_links = FakeLinkQS(links)


class FakeTargetColumnsQS:
  def __init__(self, cols):
    self._cols = cols

  def all(self):
    return list(self._cols)


class FakeTargetDataset:
  def __init__(self, *, incremental_source, target_columns):
    self.incremental_source = incremental_source
    self.target_columns = FakeTargetColumnsQS(target_columns)


# ─────────────────────────────────────────────────────────────
# Test
# ─────────────────────────────────────────────────────────────

def test_scope_filter_maps_source_cols_to_qualified_target_cols_and_keeps_delta_cutoff():
  dialect = FakeDialect()

  # Source dataset with incremental filter authored in Source/Stage terms
  src = FakeSourceDataset(
    id=1,
    incremental=True,
    portable_increment_filter=_increment_contract(include_deleted_flag=True),
  )

  # Stage → Rawcore lineage
  # ModifiedDate (stage) → upd_date (rawcore)
  upstream_modified = FakeUpstreamTargetColumn("ModifiedDate")
  upstream_deleted = FakeUpstreamTargetColumn("is_deleted_flag")

  rc_modified = FakeTargetColumn(
    pk=10,
    target_column_name="upd_date",
    links=[FakeLink(upstream_target_column=upstream_modified)],
  )

  rc_deleted = FakeTargetColumn(
    pk=11,
    target_column_name="is_deleted_flag",
    links=[FakeLink(upstream_target_column=upstream_deleted)],
  )

  td = FakeTargetDataset(
    incremental_source=src,
    target_columns=[rc_modified, rc_deleted],
  )

  out = _build_incremental_scope_filter_for_target(
    td,
    dialect=dialect,
    target_alias="t",
  )

  # ── Assertions ─────────────────────────────────────────────

  assert out is not None

  # Rawcore rename must be applied and qualified
  assert '"t"."upd_date"' in out
  assert '"t"."is_deleted_flag"' in out

  # Placeholder must remain untouched at render-time
  assert "{{DELTA_CUTOFF}}" in out

  # Original source column name must not leak into SQL
  assert re.search(r"\bModifiedDate\b", out) is None

def test_scope_filter_returns_none_if_no_stage_to_rawcore_mapping_exists():
  dialect = FakeDialect()

  # Incremental filter exists, but no usable lineage mapping
  src = FakeSourceDataset(
    id=1,
    incremental=True,
    portable_increment_filter=_increment_contract(),
  )

  # Rawcore column WITHOUT upstream_target_column
  # → mapping cannot be built
  rc_col = FakeTargetColumn(
    pk=10,
    target_column_name="upd_date",
    links=[
      FakeLink(upstream_target_column=None)  # deliberately broken lineage
    ],
  )

  td = FakeTargetDataset(
    incremental_source=src,
    target_columns=[rc_col],
  )

  out = _build_incremental_scope_filter_for_target(
    td,
    dialect=dialect,
    target_alias="t",
  )

  # No mapping possible → function must refuse to build a scope filter
  assert out is None
