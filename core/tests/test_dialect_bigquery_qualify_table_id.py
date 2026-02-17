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

import pytest

class DummyClient:
  def __init__(self, project=None):
    self.project = project


def _engine(client):
  from metadata.rendering.dialects.bigquery import BigQueryExecutionEngine
  return BigQueryExecutionEngine(client)


def test_qualify_table_id_keeps_fully_qualified():
  eng = _engine(DummyClient(project=None))
  assert eng._qualify_table_id("p1.ds.tbl") == "p1.ds.tbl"


def test_qualify_table_id_qualifies_with_client_project():
  eng = _engine(DummyClient(project="myproj"))
  assert eng._qualify_table_id("ds.tbl") == "myproj.ds.tbl"


def test_qualify_table_id_qualifies_with_engine_project_id():
  eng = _engine(DummyClient(project=None))
  eng.project_id = "engineproj"
  assert eng._qualify_table_id("ds.tbl") == "engineproj.ds.tbl"


def test_qualify_table_id_raises_if_project_missing():
  eng = _engine(DummyClient(project=None))
  with pytest.raises(RuntimeError, match="requires a default project"):
    eng._qualify_table_id("ds.tbl")


def test_qualify_table_id_rejects_invalid_format():
  eng = _engine(DummyClient(project="p"))
  with pytest.raises(ValueError):
    eng._qualify_table_id("too.many.parts.here")


def test_bigquery_introspect_table_missing_returns_table_exists_false(monkeypatch):
  """
  Regression guard:
  If BigQuery API lookup (client.get_table) fails, introspection must report table_exists=False.
  """
  from metadata.rendering.dialects.bigquery import BigQueryDialect

  class DummyClient:
    def __init__(self):
      self.project = "p"

    def get_table(self, _table_ref):
      raise Exception("not found")

  class DummyExecEngine:
    client = DummyClient()
    project_id = "p"

  d = BigQueryDialect()
  res = d.introspect_table(
    schema_name="rawcore",
    table_name="does_not_exist",
    introspection_engine=object(),
    exec_engine=DummyExecEngine(),
    debug_plan=False,
  )

  assert res["table_exists"] is False
  assert res["actual_cols_by_norm_name"] == {}


def test_bigquery_introspect_table_existing_returns_columns(monkeypatch):
  """
  Smoke test:
  If client.get_table succeeds and returns a schema, we should get table_exists=True and mapped cols.
  """
  from metadata.rendering.dialects.bigquery import BigQueryDialect

  class DummyField:
    def __init__(self, name, field_type):
      self.name = name
      self.field_type = field_type

  class DummyTable:
    def __init__(self):
      self.schema = [
        DummyField("id", "INT64"),
        DummyField("name", "STRING"),
      ]

  class DummyClient:
    def __init__(self):
      self.project = "p"

    def get_table(self, _table_ref):
      return DummyTable()

  class DummyExecEngine:
    client = DummyClient()
    project_id = "p"

  d = BigQueryDialect()
  res = d.introspect_table(
    schema_name="rawcore",
    table_name="some_table",
    introspection_engine=object(),
    exec_engine=DummyExecEngine(),
    debug_plan=False,
  )

  assert res["table_exists"] is True
  cols = res["actual_cols_by_norm_name"]
  assert "id" in cols
  assert cols["id"]["type"] == "INT64"
  assert "name" in cols
  assert cols["name"]["type"] == "STRING"
