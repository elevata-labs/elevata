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

from __future__ import annotations

from types import SimpleNamespace

from metadata.services.quality_review import build_quality_review


class FakeDialect:
  DIALECT_NAME = "fake"

  def __init__(self):
    self.calls = []

  def render_quality_not_null_examples_statement(self, **kwargs):
    self.calls.append(("not_null", kwargs))
    return "not null sql"

  def render_quality_duplicate_key_examples_statement(self, **kwargs):
    self.calls.append(("duplicate_key", kwargs))
    return "duplicate key sql"

  def render_quality_blank_string_examples_statement(self, **kwargs):
    self.calls.append(("blank_string", kwargs))
    return "blank string sql"

  def render_quality_row_presence_statement(self, **kwargs):
    self.calls.append(("row_presence", kwargs))
    return "row presence sql"


class FakeEngine:
  def __init__(self, rows_by_sql):
    self.rows_by_sql = rows_by_sql
    self.executed_sql = []

  def fetch_all(self, sql: str):
    self.executed_sql.append(sql)
    return list(self.rows_by_sql.get(sql, []))


def _column(
  name: str,
  *,
  nullable: bool = True,
  system_role: str = "",
  ordinal_position: int = 1,
  datatype: str = "",
):
  return SimpleNamespace(
    target_column_name=name,
    nullable=nullable,
    system_role=system_role,
    ordinal_position=ordinal_position,
    datatype=datatype,
    active=True,
    id=ordinal_position,
  )


def _dataset(columns):
  return SimpleNamespace(
    id=17,
    target_schema=SimpleNamespace(short_name="rawcore", schema_name="rawcore"),
    target_dataset_name="customer",
    target_columns=columns,
  )


def test_quality_review_keeps_dataset_level_empty_advisory_without_column_checks():
  dialect = FakeDialect()
  engine = FakeEngine({"row presence sql": [(3,)]})

  review = build_quality_review(
    _dataset([_column("description")]),
    dialect=dialect,
    engine=engine,
  )

  assert review.status == "passed"
  assert review.check_count == 1
  assert review.results[0].definition.check_type == "empty_dataset"
  assert review.results[0].notes == (
    "The target dataset contains at least one row.",
  )


def test_quality_review_detects_not_null_and_duplicate_key_examples():
  dialect = FakeDialect()
  engine = FakeEngine({
    "row presence sql": [(10,)],
    "not null sql": [("C-001", None)],
    "duplicate key sql": [("C-002", 2)],
  })

  review = build_quality_review(
    _dataset([
      _column(
        "customer_key",
        nullable=False,
        system_role="surrogate_key",
        ordinal_position=1,
      ),
      _column(
        "customer_id",
        nullable=False,
        system_role="business_key",
        ordinal_position=2,
      ),
    ]),
    dialect=dialect,
    engine=engine,
    example_limit=5,
  )

  assert review.status == "failed"
  assert review.failed_count == 4
  assert review.passed_count == 1
  assert review.checked_count == 5
  assert [call[0] for call in dialect.calls] == [
    "row_presence",
    "not_null",
    "not_null",
    "duplicate_key",
    "duplicate_key",
  ]

  not_null_result = review.results[0]
  assert not_null_result.definition.check_type == "not_null"
  assert not_null_result.examples[0].values == {
    "customer_id": "C-001",
    "customer_key": None,
  }

  duplicate_result = review.results[2]
  assert duplicate_result.definition.check_type == "duplicate_key"
  assert duplicate_result.examples[0].values == {
    "customer_key": "C-002",
    "duplicate_count": 2,
  }


def test_quality_review_keeps_sql_rendering_in_dialect():
  dialect = FakeDialect()
  engine = FakeEngine({"row presence sql": [(1,)], "not null sql": []})

  review = build_quality_review(
    _dataset([
      _column("customer_id", nullable=False, ordinal_position=1),
    ]),
    dialect=dialect,
    engine=engine,
    include_sql=True,
  )

  assert review.status == "passed"
  assert engine.executed_sql == ["row presence sql", "not null sql"]
  assert review.results[0].sql == "row presence sql"
  assert review.results[1].sql == "not null sql"
  assert dialect.calls[0][0] == "row_presence"
  assert dialect.calls[0][1]["probe_column"] == "customer_id"
  assert dialect.calls[1][0] == "not_null"
  assert dialect.calls[1][1]["schema_name"] == "rawcore"
  assert dialect.calls[1][1]["table_name"] == "customer"
  assert dialect.calls[1][1]["column_name"] == "customer_id"


def test_quality_review_warns_for_empty_dataset():
  dialect = FakeDialect()
  engine = FakeEngine({"row presence sql": []})

  review = build_quality_review(
    _dataset([_column("description")]),
    dialect=dialect,
    engine=engine,
  )

  assert review.status == "warning"
  assert review.warning_count == 1
  assert review.results[0].definition.check_type == "empty_dataset"
  assert review.results[0].notes == (
    "The target dataset currently contains no rows.",
  )


def test_quality_review_groups_passed_checks_for_compact_display():
  dialect = FakeDialect()
  engine = FakeEngine({
    "row presence sql": [(10,)],
    "not null sql": [],
    "duplicate key sql": [],
  })

  review = build_quality_review(
    _dataset([
      _column(
        "product_name",
        nullable=False,
        ordinal_position=3,
      ),
      _column(
        "product_id",
        nullable=False,
        system_role="business_key",
        ordinal_position=2,
      ),
      _column(
        "rc_aw_product_key",
        nullable=False,
        system_role="surrogate_key",
        ordinal_position=1,
      ),
    ]),
    dialect=dialect,
    engine=engine,
  )

  assert review.status == "passed"
  assert [(group.subject, group.checks) for group in review.passed_check_groups] == [
    ("Dataset", ("Empty dataset advisory",)),
    ("product_id", ("NOT NULL", "Duplicate business key")),
    ("product_name", ("NOT NULL",)),
    ("rc_aw_product_key", ("NOT NULL", "Duplicate surrogate key")),
  ]


def test_quality_review_warns_for_blank_string_business_key_values():
  dialect = FakeDialect()
  engine = FakeEngine({
    "row presence sql": [(10,)],
    "not null sql": [],
    "duplicate key sql": [],
    "blank string sql": [("",)],
  })

  review = build_quality_review(
    _dataset([
      _column(
        "customer_id",
        nullable=False,
        system_role="business_key",
        datatype="string",
      ),
    ]),
    dialect=dialect,
    engine=engine,
  )

  assert review.status == "warning"
  assert review.warning_count == 1
  assert [call[0] for call in dialect.calls] == [
    "row_presence",
    "not_null",
    "duplicate_key",
    "blank_string",
  ]
  blank_result = review.results[0]
  assert blank_result.definition.check_type == "blank_string"
  assert blank_result.examples[0].values == {"customer_id": ""}


def test_quality_review_passed_groups_include_blank_business_key_check():
  dialect = FakeDialect()
  engine = FakeEngine({
    "row presence sql": [(10,)],
    "not null sql": [],
    "duplicate key sql": [],
    "blank string sql": [],
  })

  review = build_quality_review(
    _dataset([
      _column(
        "customer_id",
        nullable=False,
        system_role="business_key",
        datatype="string",
      ),
    ]),
    dialect=dialect,
    engine=engine,
  )

  assert review.status == "passed"
  assert [(group.subject, group.checks) for group in review.passed_check_groups] == [
    ("Dataset", ("Empty dataset advisory",)),
    ("customer_id", ("NOT NULL", "Duplicate business key", "Blank business key")),
  ]
