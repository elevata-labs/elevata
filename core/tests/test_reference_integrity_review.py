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

from dataclasses import dataclass
from typing import Any

from metadata.services.reference_integrity_review import (
  build_reference_integrity_review,
  render_missing_parent_examples_sql,
)


class _Manager(list):
  """Tiny Django-ish queryset stand-in for service tests."""

  def all(self):
    return self

  def select_related(self, *args, **kwargs):
    return self

  def prefetch_related(self, *args, **kwargs):
    return self

  def order_by(self, *args, **kwargs):
    return list(self)


class _FakeDialect:
  """Dialect test double that owns SQL rendering for the service tests."""

  def __init__(self):
    self.calls: list[dict[str, Any]] = []

  def render_reference_integrity_missing_examples_statement(self, **kwargs) -> str:
    self.calls.append(kwargs)
    return "DIALECT_RENDERED_REFERENCE_INTEGRITY_SQL"


class _FakeEngine:
  def __init__(self, rows: list[tuple[Any, ...]]):
    self.rows = rows
    self.sql: list[str] = []

  def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
    self.sql.append(sql)
    return self.rows


@dataclass
class _Schema:
  short_name: str
  schema_name: str


@dataclass
class _Dataset:
  id: int
  target_schema: _Schema
  target_dataset_name: str
  active: bool = True
  outgoing_references: _Manager | None = None

  def __post_init__(self):
    if self.outgoing_references is None:
      self.outgoing_references = _Manager()

  def __str__(self) -> str:
    return self.target_dataset_name


@dataclass
class _Column:
  target_column_name: str
  active: bool = True


@dataclass
class _Component:
  from_column: _Column | None
  to_column: _Column | None
  ordinal_position: int = 1
  id: int = 1


@dataclass
class _Reference:
  id: int
  referencing_dataset: _Dataset
  referenced_dataset: _Dataset
  key_components: _Manager
  reference_prefix: str | None = None
  relationship_type: str = "n_to_1"


def _reference(*components: _Component) -> tuple[_Dataset, _Reference]:
  parent = _Dataset(
    id=1,
    target_schema=_Schema("rawcore", "rawcore"),
    target_dataset_name="rc_customer",
  )
  child = _Dataset(
    id=2,
    target_schema=_Schema("bizcore", "bizcore"),
    target_dataset_name="bc_order",
  )
  ref = _Reference(
    id=10,
    referencing_dataset=child,
    referenced_dataset=parent,
    key_components=_Manager(components),
  )
  child.outgoing_references = _Manager([ref])
  return child, ref


def test_render_missing_parent_examples_sql_delegates_to_dialect() -> None:
  """The service should pass semantic ingredients and let the dialect render SQL."""
  _child, ref = _reference(
    _Component(_Column("customer_id"), _Column("customer_id"), 1),
    _Component(_Column("sales_org"), _Column("sales_org"), 2),
  )
  dialect = _FakeDialect()

  sql = render_missing_parent_examples_sql(
    ref,
    dialect=dialect,
    example_limit=20,
  )

  assert sql == "DIALECT_RENDERED_REFERENCE_INTEGRITY_SQL"
  assert len(dialect.calls) == 1
  assert dialect.calls[0] == {
    "child_schema": "bizcore",
    "child_table": "bc_order",
    "parent_schema": "rawcore",
    "parent_table": "rc_customer",
    "key_pairs": [
      ("customer_id", "customer_id"),
      ("sales_org", "sales_org"),
    ],
    "example_limit": 20,
  }


def test_build_reference_integrity_review_returns_attention_for_missing_examples() -> None:
  """Any returned row is a proven reference integrity violation."""
  child, _ref = _reference(
    _Component(_Column("customer_id"), _Column("customer_id"), 1),
  )
  dialect = _FakeDialect()
  engine = _FakeEngine(rows=[("4711",), ("4712",)])

  review = build_reference_integrity_review(
    child,
    dialect=dialect,
    engine=engine,
    example_limit=20,
  )

  assert review.status == "attention"
  assert review.reference_count == 1
  assert review.attention_reference_count == 1
  assert review.checked_reference_count == 1
  assert engine.sql == ["DIALECT_RENDERED_REFERENCE_INTEGRITY_SQL"]

  result = review.results[0]
  assert result.status == "attention"
  assert result.missing_example_count == 2
  assert result.missing_examples[0].values == {"customer_id": "4711"}
  assert result.missing_examples[1].values == {"customer_id": "4712"}


def test_build_reference_integrity_review_returns_complete_when_no_missing_examples() -> None:
  """No returned anti-join rows means no missing parent examples were found."""
  child, _ref = _reference(
    _Component(_Column("customer_id"), _Column("customer_id"), 1),
  )
  dialect = _FakeDialect()
  engine = _FakeEngine(rows=[])

  review = build_reference_integrity_review(
    child,
    dialect=dialect,
    engine=engine,
    example_limit=20,
  )

  assert review.status == "complete"
  assert review.complete_reference_count == 1
  assert review.attention_reference_count == 0
  assert review.not_checked_reference_count == 0
  assert review.results[0].missing_examples == ()
  assert len(dialect.calls) == 1


def test_build_reference_integrity_review_returns_not_applicable_without_references() -> None:
  """Datasets without outgoing references should not trigger runtime checks."""
  child = _Dataset(
    id=2,
    target_schema=_Schema("bizcore", "bizcore"),
    target_dataset_name="bc_order",
    outgoing_references=_Manager(),
  )
  dialect = _FakeDialect()
  engine = _FakeEngine(rows=[("should_not_be_read",)])

  review = build_reference_integrity_review(
    child,
    dialect=dialect,
    engine=engine,
    example_limit=20,
  )

  assert review.status == "not_applicable"
  assert review.reference_count == 0
  assert engine.sql == []
  assert dialect.calls == []


def test_build_reference_integrity_review_marks_incomplete_reference_not_checked() -> None:
  """Incomplete component metadata should not execute SQL."""
  child, _ref = _reference(
    _Component(_Column("customer_id"), None, 1),
  )
  dialect = _FakeDialect()
  engine = _FakeEngine(rows=[])

  review = build_reference_integrity_review(
    child,
    dialect=dialect,
    engine=engine,
    example_limit=20,
  )

  assert review.status == "not_checked"
  assert review.not_checked_reference_count == 1
  assert review.results[0].checked is False
  assert review.results[0].notes
  assert engine.sql == []
  assert dialect.calls == []
