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

from metadata.rendering.dialects.duckdb import DuckDBDialect
from metadata.rendering.load_sql import (
  render_default_member_sql_for_target,
  render_inferred_members_sql_for_reference,
)


class FakeRelated(list):
  def filter(self, **kwargs):
    out = FakeRelated()
    for item in self:
      keep = True
      for key, expected in kwargs.items():
        if getattr(item, key, None) != expected:
          keep = False
          break
      if keep:
        out.append(item)
    return out

  def order_by(self, *_args):
    return FakeRelated(sorted(
      self,
      key=lambda c: (getattr(c, "ordinal_position", 0) or 0, getattr(c, "id", 0) or 0),
    ))

  def all(self):
    return self

  def select_related(self, *_args):
    return self

  def prefetch_related(self, *_args):
    return self


def _schema(short="rawcore"):
  return SimpleNamespace(short_name=short, schema_name=short)


def _col(name, role="", datatype="STRING", ordinal=1, active=True, nullable=True, max_length=None):
  return SimpleNamespace(
    id=ordinal,
    target_column_name=name,
    system_role=role,
    datatype=datatype,
    ordinal_position=ordinal,
    active=active,
    nullable=nullable,
    max_length=max_length,
  )


def _rawcore_dataset(name, columns):
  return SimpleNamespace(
    target_schema=_schema("rawcore"),
    target_dataset_name=name,
    target_columns=FakeRelated(columns),
    is_hist=False,
    outgoing_references=FakeRelated(),
  )


def test_default_member_sql_is_idempotent_and_sets_marker_flags():
  dialect = DuckDBDialect()
  td = _rawcore_dataset("rc_customer", [
    _col("rc_customer_key", "surrogate_key", ordinal=1),
    _col("customer_id", "business_key", datatype="INTEGER", ordinal=2),
    _col("row_hash", "row_hash", ordinal=3),
    _col("inferred_member", "inferred_member", datatype="BOOLEAN", ordinal=4),
    _col("default_member", "default_member", datatype="BOOLEAN", ordinal=5),
    _col("load_run_id", "load_run_id", ordinal=6),
    _col("loaded_at", "loaded_at", datatype="TIMESTAMP", ordinal=7),
    _col("account_no", "", datatype="STRING", ordinal=8, nullable=False, max_length=50),
  ])

  sql = render_default_member_sql_for_target(td, dialect)

  assert "INSERT INTO rawcore.rc_customer" in sql
  assert "customer_id" in sql
  assert "-1" in sql
  assert "FALSE" in sql
  assert "TRUE" in sql
  assert "WHERE NOT EXISTS" in sql
  assert "p.default_member = TRUE" in sql
  assert "{{ load_run_id }}" in sql
  assert "{{ load_timestamp }}" in sql
  assert "account_no" in sql
  assert "(Default)" in sql


def test_inferred_member_sql_uses_child_fk_and_parent_bk_mapping():
  dialect = DuckDBDialect()

  parent_bk = _col("customer_id", "business_key", datatype="INTEGER", ordinal=2)
  parent = _rawcore_dataset("rc_customer", [
    _col("rc_customer_key", "surrogate_key", ordinal=1),
    parent_bk,
    _col("row_hash", "row_hash", ordinal=3),
    _col("inferred_member", "inferred_member", datatype="BOOLEAN", ordinal=4),
    _col("default_member", "default_member", datatype="BOOLEAN", ordinal=5),
    _col("load_run_id", "load_run_id", ordinal=6),
    _col("loaded_at", "loaded_at", datatype="TIMESTAMP", ordinal=7),
    _col("customer_name", "", datatype="STRING", ordinal=8, nullable=False, max_length=50),
  ])

  child_col = _col("customer_id", "business_key", datatype="INTEGER", ordinal=1)
  child = _rawcore_dataset("rc_order", [
    _col("rc_order_key", "surrogate_key", ordinal=1),
    child_col,
    _col("rc_customer_key", "foreign_key", ordinal=2),
  ])

  component = SimpleNamespace(
    from_column=child_col,
    to_column=parent_bk,
    ordinal_position=1,
    id=1,
  )

  reference = SimpleNamespace(
    referencing_dataset=child,
    referenced_dataset=parent,
    inferred_members_enabled=True,
    key_components=FakeRelated([component]),
    get_child_fk_name=lambda: "rc_customer_key",
  )

  sql = render_inferred_members_sql_for_reference(reference, dialect)

  assert "INSERT INTO rawcore.rc_customer" in sql
  assert "SELECT DISTINCT" in sql
  assert "FROM rawcore.rc_order AS c" in sql
  assert "c.rc_customer_key" in sql
  assert "c.customer_id" in sql
  assert "c.rc_customer_key IS NOT NULL" in sql
  assert "c.customer_id IS NOT NULL" in sql
  assert "NOT EXISTS" in sql
  assert "p.rc_customer_key = c.rc_customer_key" in sql
  assert "TRUE" in sql
  assert "FALSE" in sql
  assert "customer_name" in sql
  assert "(Inferred)" in sql


def test_inferred_member_sql_is_disabled_by_default():
  dialect = DuckDBDialect()
  parent = _rawcore_dataset("rc_customer", [
    _col("rc_customer_key", "surrogate_key", ordinal=1),
    _col("customer_id", "business_key", datatype="INTEGER", ordinal=2),
    _col("inferred_member", "inferred_member", datatype="BOOLEAN", ordinal=3),
  ])
  child = _rawcore_dataset("rc_order", [])
  reference = SimpleNamespace(
    referencing_dataset=child,
    referenced_dataset=parent,
    inferred_members_enabled=False,
    key_components=FakeRelated(),
    get_child_fk_name=lambda: "rc_customer_key",
  )

  assert render_inferred_members_sql_for_reference(reference, dialect) is None
