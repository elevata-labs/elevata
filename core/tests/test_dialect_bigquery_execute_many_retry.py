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

from __future__ import annotations

import types

import pytest


def _get_NotFound():
  # Avoid a hard dependency on google packages for unit tests.
  try:
    from google.api_core.exceptions import NotFound  # type: ignore
    return NotFound
  except Exception:
    class NotFound(Exception):
      pass
    return NotFound


class DummyBigQueryClient:
  def __init__(self, *, project: str, fail_times: int = 0, fail_kind: str = "notfound"):
    self.project = project
    self._fail_times = int(fail_times)
    self._fail_kind = str(fail_kind)
    self.insert_calls: list[tuple[str, list[dict[str, object]]]] = []
    self.get_table_calls: list[str] = []

  def insert_rows_json(self, table_id: str, rows: list[dict[str, object]]):
    self.insert_calls.append((table_id, rows))
    if self._fail_times > 0:
      self._fail_times -= 1
      if self._fail_kind == "notfound":
        raise _get_NotFound()("Table not found")
      if self._fail_kind == "string":
        raise RuntimeError("Not Found: table not found")
      raise RuntimeError("Some other error")
    return []  # BigQuery returns [] on success

  def get_table(self, table_id: str):
    # Simulate that the table becomes visible after the first failure.
    self.get_table_calls.append(table_id)
    return {"table_id": table_id}


@pytest.fixture
def bq_engine(monkeypatch):
  # Import the engine class from your module.
  # Adjust this import if the class/module name differs in your repo.
  from metadata.rendering.dialects.bigquery import BigQueryExecutionEngine  # type: ignore
  mod = __import__("metadata.rendering.dialects.bigquery", fromlist=["time"])

  # Disable real sleeping for deterministic/fast unit tests.
  sleep_calls = []

  def _fake_sleep(seconds: float):
    sleep_calls.append(float(seconds))

  monkeypatch.setattr(mod.time, "sleep", _fake_sleep, raising=True)

  return BigQueryExecutionEngine, sleep_calls


def test_execute_many_qualifies_table_id(bq_engine):
  BigQueryExecutionEngine, sleep_calls = bq_engine

  client = DummyBigQueryClient(project="elevata-481913", fail_times=0)
  eng = BigQueryExecutionEngine(client)

  insert_sql = "INSERT INTO raw.raw_aw1_productmodel (a, b) VALUES (?, ?);"
  params = [(1, "x")]

  n = eng.execute_many(insert_sql, params)

  assert n == 1
  assert len(client.insert_calls) == 1
  table_id, rows = client.insert_calls[0]
  assert table_id == "elevata-481913.raw.raw_aw1_productmodel"
  assert rows == [{"a": 1, "b": "x"}]
  assert sleep_calls == []


def test_execute_many_retries_on_notfound_then_succeeds(bq_engine):
  BigQueryExecutionEngine, sleep_calls = bq_engine

  client = DummyBigQueryClient(project="elevata-481913", fail_times=1, fail_kind="notfound")
  eng = BigQueryExecutionEngine(client)

  insert_sql = "INSERT INTO raw.raw_aw1_productmodel (a, b) VALUES (?, ?);"
  params = [(1, "x")]

  n = eng.execute_many(insert_sql, params)

  assert n == 1
  # First insert fails (NotFound), second succeeds.
  assert len(client.insert_calls) == 2
  # Backoff should have slept at least once.
  assert len(sleep_calls) >= 1


def test_execute_many_retries_on_generic_not_found_message(bq_engine):
  BigQueryExecutionEngine, sleep_calls = bq_engine

  # Some wrappers may not raise google.api_core.exceptions.NotFound but still include "not found" in the message.
  client = DummyBigQueryClient(project="elevata-481913", fail_times=1, fail_kind="string")
  eng = BigQueryExecutionEngine(client)

  insert_sql = "INSERT INTO raw.raw_aw1_productmodel (a, b) VALUES (?, ?);"
  params = [(1, "x")]

  n = eng.execute_many(insert_sql, params)

  assert n == 1
  assert len(client.insert_calls) == 2
  assert len(sleep_calls) >= 1
