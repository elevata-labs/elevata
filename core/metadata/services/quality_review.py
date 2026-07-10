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

from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Literal

from metadata.config.targets import get_target_system
from metadata.models import TargetColumn, TargetDataset
from metadata.rendering.dialects import get_active_dialect
from metadata.rendering.dialects.base import BaseExecutionEngine, SqlDialect

QualityReviewStatus = Literal[
  "not_applicable",
  "passed",
  "warning",
  "failed",
  "unavailable",
]

QualityCheckType = Literal[
  "not_null",
  "duplicate_key",
  "empty_dataset",
  "blank_string",
]


@dataclass(frozen=True)
class QualityCheckDefinition:
  """Transient metadata-defined quality check definition."""

  check_id: str
  check_type: QualityCheckType
  label: str
  severity: str
  columns: tuple[str, ...]
  notes: tuple[str, ...] = ()

  def to_dict(self) -> dict[str, Any]:
    """Return a deterministic dictionary representation for UI/API use."""
    return asdict(self)


@dataclass(frozen=True)
class QualityCheckExample:
  """One bounded example returned by a read-only quality check."""

  values: dict[str, Any]


@dataclass(frozen=True)
class QualityCheckDisplayGroup:
  """Compact display group for passed quality checks."""

  subject: str
  checks: tuple[str, ...]


@dataclass(frozen=True)
class QualityCheckResult:
  """Runtime result for one metadata-defined quality check."""

  definition: QualityCheckDefinition
  status: QualityReviewStatus
  checked: bool
  examples: tuple[QualityCheckExample, ...]
  example_count: int
  example_limit: int
  notes: tuple[str, ...]
  sql: str = ""

  def to_dict(self) -> dict[str, Any]:
    """Return a deterministic dictionary representation for UI/API use."""
    return asdict(self)


@dataclass(frozen=True)
class QualityReview:
  """Transient read-only Architecture Quality Review for one TargetDataset."""

  dataset_id: int | None
  dataset_key: str
  status: QualityReviewStatus
  checked_count: int
  passed_count: int
  warning_count: int
  failed_count: int
  unavailable_count: int
  check_count: int
  example_limit: int
  results: tuple[QualityCheckResult, ...]
  passed_check_groups: tuple[QualityCheckDisplayGroup, ...]
  notes: tuple[str, ...]

  def to_dict(self) -> dict[str, Any]:
    """Return a deterministic dictionary representation for UI/API use."""
    return asdict(self)


def build_quality_review(
  target_dataset: TargetDataset,
  *,
  target_system_name: str | None = None,
  dialect_name: str | None = None,
  dialect: SqlDialect | None = None,
  engine: BaseExecutionEngine | None = None,
  columns: Iterable[TargetColumn] | None = None,
  example_limit: int = 20,
  include_sql: bool = False,
) -> QualityReview:
  """
  Run metadata-defined, read-only quality checks for one TargetDataset.

  The review is transient and diagnostic. It does not persist results, mutate
  metadata, mutate target data, execute loads or create Architecture Control
  gates. Services determine check semantics; dialects render SQL.
  """
  safe_example_limit = max(1, int(example_limit or 20))
  dataset_key = _dataset_key(target_dataset)
  target_columns = _load_target_columns(target_dataset, columns)
  definitions = tuple(_build_quality_check_definitions(target_columns))

  if not definitions:
    return QualityReview(
      dataset_id=getattr(target_dataset, "id", None),
      dataset_key=dataset_key,
      status="not_applicable",
      checked_count=0,
      passed_count=0,
      warning_count=0,
      failed_count=0,
      unavailable_count=0,
      check_count=0,
      example_limit=safe_example_limit,
      results=(),
      passed_check_groups=(),
      notes=("No metadata-defined quality checks are available for this dataset.",),
    )

  runtime_dialect = dialect or get_active_dialect(dialect_name)
  runtime_engine = engine
  if runtime_engine is None:
    target_system = get_target_system(target_system_name)
    runtime_engine = runtime_dialect.get_execution_engine(target_system)

  results = _sort_quality_results(tuple(
    _run_quality_check(
      target_dataset,
      definition,
      columns=target_columns,
      dialect=runtime_dialect,
      engine=runtime_engine,
      example_limit=safe_example_limit,
      include_sql=include_sql,
    )
    for definition in definitions
  ))

  failed_count = sum(1 for r in results if r.status == "failed")
  warning_count = sum(1 for r in results if r.status == "warning")
  unavailable_count = sum(1 for r in results if r.status == "unavailable")
  passed_count = sum(1 for r in results if r.status == "passed")
  checked_count = sum(1 for r in results if r.checked)

  if failed_count:
    status: QualityReviewStatus = "failed"
  elif warning_count:
    status = "warning"
  elif unavailable_count:
    status = "unavailable"
  elif passed_count == len(results):
    status = "passed"
  else:
    status = "unavailable"

  return QualityReview(
    dataset_id=getattr(target_dataset, "id", None),
    dataset_key=dataset_key,
    status=status,
    checked_count=checked_count,
    passed_count=passed_count,
    warning_count=warning_count,
    failed_count=failed_count,
    unavailable_count=unavailable_count,
    check_count=len(results),
    example_limit=safe_example_limit,
    results=results,
    passed_check_groups=_build_passed_check_groups(results),
    notes=(),
  )


def _build_quality_check_definitions(
  columns: list[TargetColumn],
) -> list[QualityCheckDefinition]:
  """Build transient quality check definitions from TargetColumn metadata."""
  definitions: list[QualityCheckDefinition] = [
    QualityCheckDefinition(
      check_id="empty_dataset",
      check_type="empty_dataset",
      label="Empty dataset advisory",
      severity="warning",
      columns=(),
      notes=(
        "Checks whether the loaded target dataset currently contains rows.",
      ),
    )
  ]

  for column in columns:
    column_name = _column_name(column)
    if not column_name:
      continue
    if _is_not_null_column(column):
      definitions.append(QualityCheckDefinition(
        check_id=f"not_null:{column_name}",
        check_type="not_null",
        label=f"NOT NULL: {column_name}",
        severity="error",
        columns=(column_name,),
      ))

  surrogate_key_columns = [
    _column_name(column)
    for column in columns
    if _system_role(column) == "surrogate_key" and _column_name(column)
  ]
  for column_name in surrogate_key_columns:
    definitions.append(QualityCheckDefinition(
      check_id=f"duplicate_key:surrogate_key:{column_name}",
      check_type="duplicate_key",
      label=f"Duplicate surrogate key: {column_name}",
      severity="error",
      columns=(column_name,),
    ))

  business_key_columns = [
    _column_name(column)
    for column in columns
    if _system_role(column) == "business_key" and _column_name(column)
  ]
  if business_key_columns:
    definitions.append(QualityCheckDefinition(
      check_id="duplicate_key:business_key:" + "+".join(business_key_columns),
      check_type="duplicate_key",
      label="Duplicate business key: " + ", ".join(business_key_columns),
      severity="error",
      columns=tuple(business_key_columns),
    ))

  for column in columns:
    column_name = _column_name(column)
    if (
      column_name
      and _system_role(column) == "business_key"
      and _is_text_like_column(column)
    ):
      definitions.append(QualityCheckDefinition(
        check_id=f"blank_string:business_key:{column_name}",
        check_type="blank_string",
        label=f"Blank business key value: {column_name}",
        severity="warning",
        columns=(column_name,),
        notes=(
          "Checks whether a text-based business key contains blank values.",
        ),
      ))

  return definitions


def _run_quality_check(
  target_dataset: TargetDataset,
  definition: QualityCheckDefinition,
  *,
  columns: list[TargetColumn],
  dialect: SqlDialect,
  engine: BaseExecutionEngine,
  example_limit: int,
  include_sql: bool,
) -> QualityCheckResult:
  """Run one quality check and return a deterministic result."""
  schema_name = _physical_schema_name(target_dataset)
  table_name = _physical_dataset_name(target_dataset)
  if not schema_name or not table_name:
    return _quality_result(
      definition,
      status="unavailable",
      checked=False,
      examples=(),
      example_limit=example_limit,
      notes=("Target dataset has no physical schema or table name.",),
    )

  try:
    if definition.check_type == "empty_dataset":
      probe_column = _row_presence_probe_column(columns)
      try:
        sql = dialect.render_quality_row_presence_statement(
          schema_name=schema_name,
          table_name=table_name,
          probe_column=probe_column,
        )
      except TypeError:
        # Backward-compatible fallback for third-party dialects that have not
        # yet accepted probe_column. Built-in dialects support the argument.
        sql = dialect.render_quality_row_presence_statement(
          schema_name=schema_name,
          table_name=table_name,
        )
      rows = engine.fetch_all(sql)
      if not rows:
        return _quality_result(
          definition,
          status="warning",
          checked=True,
          examples=(),
          example_limit=example_limit,
          notes=("The target dataset currently contains no rows.",),
          sql=sql if include_sql else "",
        )
      return _quality_result(
        definition,
        status="passed",
        checked=True,
        examples=(),
        example_limit=example_limit,
        notes=("The target dataset contains at least one row.",),
        sql=sql if include_sql else "",
      )
    if definition.check_type == "not_null":
      column_name = definition.columns[0]
      sql = dialect.render_quality_not_null_examples_statement(
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name,
        context_columns=_context_column_names(columns, checked_column=column_name),
        example_limit=example_limit,
      )
      rows = engine.fetch_all(sql)
      result_columns = _context_column_names(columns, checked_column=column_name)
      if column_name not in result_columns:
        result_columns.append(column_name)
    elif definition.check_type == "duplicate_key":
      sql = dialect.render_quality_duplicate_key_examples_statement(
        schema_name=schema_name,
        table_name=table_name,
        key_columns=list(definition.columns),
        example_limit=example_limit,
      )
      rows = engine.fetch_all(sql)
      result_columns = list(definition.columns) + ["duplicate_count"]
    elif definition.check_type == "blank_string":
      column_name = definition.columns[0]
      sql = dialect.render_quality_blank_string_examples_statement(
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name,
        context_columns=_context_column_names(columns, checked_column=column_name),
        example_limit=example_limit,
      )
      rows = engine.fetch_all(sql)
      result_columns = _context_column_names(columns, checked_column=column_name)
      if column_name not in result_columns:
        result_columns.append(column_name)
    else:
      return _quality_result(
        definition,
        status="unavailable",
        checked=False,
        examples=(),
        example_limit=example_limit,
        notes=(f"Unsupported quality check type: {definition.check_type}",),
      )
  except Exception as exc:
    return _quality_result(
      definition,
      status="unavailable",
      checked=False,
      examples=(),
      example_limit=example_limit,
      notes=(f"Quality check could not be executed: {exc}",),
    )

  examples = tuple(
    _row_to_example(row, result_columns)
    for row in rows[:example_limit]
  )
  status: QualityReviewStatus = "passed"
  if examples:
    status = "warning" if definition.severity == "warning" else "failed"

  return _quality_result(
    definition,
    status=status,
    checked=True,
    examples=examples,
    example_limit=example_limit,
    notes=(),
    sql=sql if include_sql else "",
  )


def _sort_quality_results(
  results: tuple[QualityCheckResult, ...],
) -> tuple[QualityCheckResult, ...]:
  """Return results with findings first and passed checks last."""
  status_priority = {
    "failed": 0,
    "warning": 1,
    "unavailable": 2,
    "not_applicable": 3,
    "passed": 4,
  }
  indexed = list(enumerate(results))
  indexed.sort(key=lambda item: (
    status_priority.get(item[1].status, 9),
    item[0],
  ))
  return tuple(result for _, result in indexed)


def _build_passed_check_groups(
  results: tuple[QualityCheckResult, ...],
) -> tuple[QualityCheckDisplayGroup, ...]:
  """Build compact, alphabetically ordered display groups for passed checks."""
  grouped: dict[tuple[str, ...], list[str]] = {}
  for result in results:
    if result.status != "passed":
      continue

    subject_key = result.definition.columns or ("Dataset",)
    label = _display_check_label(result.definition)
    labels = grouped.setdefault(subject_key, [])
    if label not in labels:
      labels.append(label)

  items: list[tuple[tuple[int, str], QualityCheckDisplayGroup]] = []
  for subject_key, labels in grouped.items():
    subject = _display_subject(subject_key)
    sorted_labels = tuple(sorted(
      labels,
      key=lambda label: (_display_check_priority(label), label.lower()),
    ))
    items.append((
      _display_subject_sort_key(subject_key),
      QualityCheckDisplayGroup(
        subject=subject,
        checks=sorted_labels,
      ),
    ))

  items.sort(key=lambda item: item[0])
  return tuple(group for _, group in items)


def _display_subject(subject_key: tuple[str, ...]) -> str:
  """Return a compact review-detail subject for a check group."""
  if not subject_key or subject_key == ("Dataset",):
    return "Dataset"
  return ", ".join(str(value) for value in subject_key)


def _display_subject_sort_key(subject_key: tuple[str, ...]) -> tuple[int, str]:
  """Sort dataset-level advisory first, then column groups alphabetically."""
  if not subject_key or subject_key == ("Dataset",):
    return (0, "")
  return (1, "|".join(str(value).lower() for value in subject_key))


def _display_check_label(definition: QualityCheckDefinition) -> str:
  """Return the compact, user-facing label for one passed check."""
  if definition.check_type == "not_null":
    return "NOT NULL"

  if definition.check_type == "duplicate_key":
    if "surrogate_key" in definition.check_id:
      return "Duplicate surrogate key"
    if "business_key" in definition.check_id:
      return "Duplicate business key"
    return "Duplicate key"

  if definition.check_type == "blank_string":
    return "Blank business key"

  if definition.check_type == "empty_dataset":
    return "Empty dataset advisory"

  return str(definition.check_type)


def _display_check_priority(label: str) -> int:
  """Return a stable display order for grouped passed checks."""
  priorities = {
    "Empty dataset advisory": 0,
    "NOT NULL": 10,
    "Duplicate surrogate key": 20,
    "Duplicate business key": 30,
    "Duplicate key": 40,
    "Blank business key": 50,
  }
  return priorities.get(label, 99)


def _quality_result(
  definition: QualityCheckDefinition,
  *,
  status: QualityReviewStatus,
  checked: bool,
  examples: tuple[QualityCheckExample, ...],
  example_limit: int,
  notes: tuple[str, ...],
  sql: str = "",
) -> QualityCheckResult:
  """Build a deterministic check-level result."""
  return QualityCheckResult(
    definition=definition,
    status=status,
    checked=checked,
    examples=examples,
    example_count=len(examples),
    example_limit=example_limit,
    notes=tuple(notes),
    sql=sql,
  )


def _load_target_columns(
  target_dataset: TargetDataset,
  columns: Iterable[TargetColumn] | None,
) -> list[TargetColumn]:
  """Load active TargetColumns in deterministic metadata order."""
  if columns is not None:
    items = list(columns)
  else:
    manager = getattr(target_dataset, "target_columns", None)
    if manager is None:
      return []
    qs = manager.all() if hasattr(manager, "all") else manager
    if hasattr(qs, "order_by"):
      qs = qs.order_by("ordinal_position", "target_column_name", "id")
    items = list(qs)

  active_items = [
    item
    for item in items
    if getattr(item, "active", True)
  ]
  return sorted(
    active_items,
    key=lambda c: (
      int(getattr(c, "ordinal_position", 0) or 0),
      _column_name(c),
      int(getattr(c, "id", 0) or 0),
    ),
  )


def _context_column_names(
  columns: list[TargetColumn],
  *,
  checked_column: str,
  max_columns: int = 5,
) -> list[str]:
  """Return compact key-like context columns for bounded examples."""
  selected: list[str] = []
  for column in columns:
    role = _system_role(column)
    if role not in {"surrogate_key", "business_key"}:
      continue
    name = _column_name(column)
    if name and name != checked_column and name not in selected:
      selected.append(name)
    if len(selected) >= max_columns:
      break
  return selected


def _row_presence_probe_column(columns: list[TargetColumn]) -> str | None:
  """Return a real target column for bounded row-presence checks."""
  for column in columns:
    name = _column_name(column)
    if name:
      return name
  return None


def _row_to_example(row: Any, columns: list[str]) -> QualityCheckExample:
  """Convert one engine row into a deterministic example."""
  values = tuple(row) if not isinstance(row, tuple) else row
  return QualityCheckExample(values={
    column: _serialize_value(values[index] if index < len(values) else None)
    for index, column in enumerate(columns)
  })


def _is_not_null_column(column: TargetColumn) -> bool:
  """Return whether a TargetColumn defines a NOT NULL contract."""
  return not bool(getattr(column, "nullable", True))


def _is_text_like_column(column: TargetColumn) -> bool:
  """Return whether a TargetColumn uses a text-like logical datatype."""
  dtype = str(getattr(column, "datatype", "") or "").strip().lower()
  return dtype in {"string", "varchar", "nvarchar", "char", "text"}


def _system_role(column: TargetColumn) -> str:
  """Return normalized TargetColumn system role."""
  return str(getattr(column, "system_role", None) or "").strip().lower()


def _column_name(column: TargetColumn) -> str:
  """Return a stable TargetColumn name."""
  return str(getattr(column, "target_column_name", None) or "").strip()


def _physical_schema_name(target_dataset: TargetDataset | None) -> str:
  """Return the physical schema name used for SQL rendering."""
  schema = getattr(target_dataset, "target_schema", None)
  return (getattr(schema, "schema_name", None) or "").strip()


def _physical_dataset_name(target_dataset: TargetDataset | None) -> str:
  """Return the physical dataset name used for SQL rendering."""
  return (getattr(target_dataset, "target_dataset_name", None) or "").strip()


def _dataset_key(target_dataset: TargetDataset | None) -> str:
  """Return the stable human-readable dataset key."""
  if target_dataset is None:
    return "<?>"
  schema = getattr(target_dataset, "target_schema", None)
  schema_short = (getattr(schema, "short_name", None) or "<?>").strip()
  dataset_name = (getattr(target_dataset, "target_dataset_name", None) or "<?>").strip()
  return f"{schema_short}.{dataset_name}"


def _serialize_value(value: Any) -> Any:
  """Return a JSON-friendly value for transient review output."""
  if value is None:
    return None
  if isinstance(value, (str, int, float, bool)):
    return value
  if isinstance(value, datetime):
    return value.isoformat()
  if isinstance(value, date):
    return value.isoformat()
  if isinstance(value, Decimal):
    return str(value)
  return str(value)
