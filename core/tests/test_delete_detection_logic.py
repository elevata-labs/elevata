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

from types import SimpleNamespace

import pytest

from metadata.rendering import load_sql as load_sql_mod
from metadata.rendering.load_sql import render_delete_missing_rows_sql
from tests._dialect_test_mixin import DialectTestMixin


class DummyDialect(DialectTestMixin):
  pass

def _make_td(
  schema_short: str = "rawcore",
  schema_name: str = "dw_rawcore",
  dataset_name: str = "rc_customer",
  natural_keys=None,
  incremental_source=True,
):
  """Minimal TargetDataset stub.

  We only model the attributes used by render_delete_missing_rows_sql().
  """
  if natural_keys is None:
    natural_keys = ["business_id"]

  td = SimpleNamespace()
  td.id = 42
  td.target_dataset_name = dataset_name
  td.target_schema = SimpleNamespace(
    short_name=schema_short,
    schema_name=schema_name,
  )
  td.natural_key_fields = list(natural_keys)
  td.incremental_source = object() if incremental_source else None
  # input_links, target_columns etc. will be mocked with the most tests
  return td

def test_delete_detection_requires_supporting_dialect():
  class NoDeleteDialect:
    supports_delete_detection = False

  td = _make_td()

  with pytest.raises(NotImplementedError):
    render_delete_missing_rows_sql(td, NoDeleteDialect())

def test_delete_detection_returns_none_if_not_merge_or_handle_deletes(monkeypatch):
  td = _make_td()
  dialect = DummyDialect()

  # Plan: mode != "merge" or handle_deletes = False -> None expected
  monkeypatch.setattr(
    load_sql_mod,
    "build_load_plan",
    lambda _td: SimpleNamespace(mode="full", handle_deletes=False),
  )

  result = render_delete_missing_rows_sql(td, dialect)
  assert result is None

def test_delete_detection_only_for_rawcore_targets(monkeypatch):
  td = _make_td(schema_short="stage", schema_name="dw_stage")
  dialect = DummyDialect()

  monkeypatch.setattr(
    load_sql_mod,
    "build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )

  sql = render_delete_missing_rows_sql(td, dialect)

  assert "only implemented for rawcore" in sql
  assert "No delete detection SQL generated" in sql

def test_delete_detection_missing_scope_filter(monkeypatch):
  td = _make_td()
  dialect = DummyDialect()

  monkeypatch.setattr(
    load_sql_mod,
    "build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )
  # simulate: no usable incremental_filter
  monkeypatch.setattr(
    load_sql_mod,
    "_build_incremental_scope_filter_for_target",
    lambda td, **kwargs: None
  )

  sql = render_delete_missing_rows_sql(td, dialect)

  assert "no usable incremental_filter could be derived" in sql
  assert "No delete detection SQL generated" in sql

def test_delete_detection_missing_incremental_source(monkeypatch):
  td = _make_td(incremental_source=False)
  dialect = DummyDialect()

  monkeypatch.setattr(
    load_sql_mod,
    "build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )
  monkeypatch.setattr(
    load_sql_mod,
    "_build_incremental_scope_filter_for_target",
    lambda _td, **_kwargs: "(t.load_ts > {{DELTA_CUTOFF}})",
  )

  sql = render_delete_missing_rows_sql(td, dialect)

  assert "incremental_source is not set" in sql
  assert "No delete detection SQL generated" in sql

def test_delete_detection_missing_natural_keys(monkeypatch):
  td = _make_td(natural_keys=[])
  dialect = DummyDialect()

  monkeypatch.setattr(
    load_sql_mod,
    "build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )
  monkeypatch.setattr(
    load_sql_mod,
    "_build_incremental_scope_filter_for_target",
    lambda _td, **_kwargs: "(t.load_ts > {{DELTA_CUTOFF}})",
  )

  sql = render_delete_missing_rows_sql(td, dialect)

  assert "natural_key_fields are not defined" in sql
  assert "No delete detection SQL generated" in sql

def test_delete_detection_missing_stage_upstream(monkeypatch):
  td = _make_td()
  dialect = DummyDialect()

  monkeypatch.setattr(
    load_sql_mod,
    "build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )
  monkeypatch.setattr(
    load_sql_mod,
    "_build_incremental_scope_filter_for_target",
    lambda _td, **_kwargs: "(t.load_ts > {{DELTA_CUTOFF}})",
  )

  # no Stage-Upstream found
  monkeypatch.setattr(
    load_sql_mod,
    "_find_stage_upstream_for_rawcore",
    lambda _td: None,
  )

  sql = render_delete_missing_rows_sql(td, dialect)

  assert "no upstream stage dataset could be resolved" in sql
  assert "No delete detection SQL generated" in sql

def test_delete_detection_happy_path_calls_dialect_with_expected_args(monkeypatch):
  td = _make_td(
    schema_short="rawcore",
    schema_name="dw_rawcore",
    dataset_name="rc_customer",
    natural_keys=["business_id", "system_id"],
  )
  dialect = DummyDialect()

  # Merge + handle_deletes = True
  monkeypatch.setattr(
    load_sql_mod,
    "build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )
  # usable scope filter
  monkeypatch.setattr(
    load_sql_mod,
    "_build_incremental_scope_filter_for_target",
    lambda _td, **_kwargs:  '(t."load_ts" > {{DELTA_CUTOFF}})',
  )

  # Fake Stage-TargetDataset
  stage_td = SimpleNamespace(
    target_schema=SimpleNamespace(
      short_name="stage",
      schema_name="dw_stage",
    ),
    target_dataset_name="stg_customer",
  )
  monkeypatch.setattr(
    load_sql_mod,
    "_find_stage_upstream_for_rawcore",
    lambda _td: stage_td,
  )

  # expr_map: expressions per target column name
  monkeypatch.setattr(
    load_sql_mod,
    "_get_rendered_column_exprs_for_target",
    lambda _td, _dialect: {
      "business_id": 's."business_id"',
      "system_id": 's."system_id"',
    },
  )

  sql = render_delete_missing_rows_sql(td, dialect)

  assert sql == "-- dummy delete detection sql"
  assert len(dialect.calls) == 1

  call = dialect.calls[0]

  assert call["target_schema"] == "dw_rawcore"
  assert call["target_table"] == "rc_customer"
  assert call["stage_schema"] == "dw_stage"
  assert call["stage_table"] == "stg_customer"
  assert call["scope_filter"] == '(t."load_ts" > {{DELTA_CUTOFF}})'

  # join_predicates should follow natural_key_fields order
  assert call["join_predicates"] == [
    't.business_id = s."business_id"',
    't.system_id = s."system_id"',
  ]


def test_delete_detection_falls_back_to_simple_source_ref_if_expr_missing(monkeypatch):
  td = _make_td(
    schema_short="rawcore",
    schema_name="dw_rawcore",
    dataset_name="rc_customer",
    natural_keys=["missing_key"],
  )
  dialect = DummyDialect()

  monkeypatch.setattr(
    load_sql_mod,
    "build_load_plan",
    lambda _td: SimpleNamespace(mode="merge", handle_deletes=True),
  )
  monkeypatch.setattr(
    load_sql_mod,
    "_build_incremental_scope_filter_for_target",
    lambda _td, **_kwargs: "(t.load_ts > {{DELTA_CUTOFF}})",
  )

  stage_td = SimpleNamespace(
    target_schema=SimpleNamespace(short_name="stage", schema_name="dw_stage"),
    target_dataset_name="stg_customer",
  )
  monkeypatch.setattr(
    load_sql_mod,
    "_find_stage_upstream_for_rawcore",
    lambda _td: stage_td,
  )

  # expr_map does not deliver an entry for "missing_key" here
  monkeypatch.setattr(
    load_sql_mod,
    "_get_rendered_column_exprs_for_target",
    lambda _td, _dialect: {},
  )

  sql = render_delete_missing_rows_sql(td, dialect)

  assert sql == "-- dummy delete detection sql"
  assert len(dialect.calls) == 1

  call = dialect.calls[0]
  assert call["join_predicates"] == [
    't.missing_key = s.missing_key',
    ]
