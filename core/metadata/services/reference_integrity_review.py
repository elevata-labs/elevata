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
from metadata.models import TargetDataset, TargetDatasetReference
from metadata.rendering.dialects import get_active_dialect
from metadata.rendering.dialects.base import BaseExecutionEngine, SqlDialect

ReferenceIntegrityStatus = Literal[
  "not_applicable",
  "complete",
  "attention",
  "not_checked",
]


@dataclass(frozen=True)
class ReferenceIntegrityComponent:
  """Column mapping used to check one reference integrity predicate."""

  child_column: str
  parent_column: str
  ordinal_position: int


@dataclass(frozen=True)
class ReferenceIntegrityMissingParentExample:
  """One proven child key combination that currently has no parent match."""

  values: dict[str, Any]


@dataclass(frozen=True)
class ReferenceIntegrityReferenceResult:
  """Review result for one TargetDatasetReference."""

  reference_id: int | None
  reference_key: str
  child_dataset: str
  parent_dataset: str
  relationship_type: str
  status: ReferenceIntegrityStatus
  checked: bool
  components: tuple[ReferenceIntegrityComponent, ...]
  missing_examples: tuple[ReferenceIntegrityMissingParentExample, ...]
  missing_example_count: int
  example_limit: int
  notes: tuple[str, ...]
  sql: str = ""

  def to_dict(self) -> dict[str, Any]:
    """Return a deterministic dictionary representation for UI/API use."""
    return asdict(self)


@dataclass(frozen=True)
class ReferenceIntegrityReview:
  """Transient read-only Reference Integrity Review for one TargetDataset."""

  dataset_id: int | None
  dataset_key: str
  status: ReferenceIntegrityStatus
  checked_reference_count: int
  complete_reference_count: int
  attention_reference_count: int
  not_checked_reference_count: int
  reference_count: int
  example_limit: int
  results: tuple[ReferenceIntegrityReferenceResult, ...]
  notes: tuple[str, ...]

  def to_dict(self) -> dict[str, Any]:
    """Return a deterministic dictionary representation for UI/API use."""
    return asdict(self)


def build_reference_integrity_review(
  target_dataset: TargetDataset,
  *,
  target_system_name: str | None = None,
  dialect_name: str | None = None,
  dialect: SqlDialect | None = None,
  engine: BaseExecutionEngine | None = None,
  references: Iterable[TargetDatasetReference] | None = None,
  example_limit: int = 20,
  max_references: int = 20,
  include_sql: bool = False,
) -> ReferenceIntegrityReview:
  """
  Check outgoing TargetDatasetReferences against loaded target data.

  The review is read-only and transient. It does not persist results and does
  not mutate metadata or target data. Returned examples are not statistical
  samples: every returned row is a proven missing parent key combination.
  """
  safe_example_limit = max(1, int(example_limit or 20))
  safe_max_references = max(1, int(max_references or 20))
  dataset_key = _dataset_key(target_dataset)

  all_reference_items = _load_outgoing_references(target_dataset, references)
  truncated = len(all_reference_items) > safe_max_references
  reference_items = all_reference_items[:safe_max_references]

  if not reference_items:
    return ReferenceIntegrityReview(
      dataset_id=getattr(target_dataset, "id", None),
      dataset_key=dataset_key,
      status="not_applicable",
      checked_reference_count=0,
      complete_reference_count=0,
      attention_reference_count=0,
      not_checked_reference_count=0,
      reference_count=0,
      example_limit=safe_example_limit,
      results=(),
      notes=("No outgoing references are modeled for this dataset.",),
    )

  runtime_dialect = dialect or get_active_dialect(dialect_name)

  results = tuple(
    _check_reference(
      reference,
      dialect=runtime_dialect,
      engine=engine,
      target_system_name=target_system_name,
      example_limit=safe_example_limit,
      include_sql=include_sql,
    )
    for reference in reference_items
  )

  attention_count = sum(1 for r in results if r.status == "attention")
  complete_count = sum(1 for r in results if r.status == "complete")
  not_checked_count = sum(1 for r in results if r.status == "not_checked")
  checked_count = sum(1 for r in results if r.checked)

  if attention_count:
    status: ReferenceIntegrityStatus = "attention"
  elif not_checked_count:
    status = "not_checked"
  elif complete_count == len(results):
    status = "complete"
  else:
    status = "not_checked"

  return ReferenceIntegrityReview(
    dataset_id=getattr(target_dataset, "id", None),
    dataset_key=dataset_key,
    status=status,
    checked_reference_count=checked_count,
    complete_reference_count=complete_count,
    attention_reference_count=attention_count,
    not_checked_reference_count=not_checked_count,
    reference_count=len(results),
    example_limit=safe_example_limit,
    results=results,
    notes=(
      f"Reference review limited to {safe_max_references} references.",
    ) if truncated else (),
  )


def render_missing_parent_examples_sql(
  reference: TargetDatasetReference,
  *,
  dialect: SqlDialect,
  example_limit: int = 20,
) -> str:
  """
  Ask the active dialect to render missing-parent example SQL for a reference.

  The service provides only semantic ingredients: child table, parent table and
  ordered key pairs. SQL shape, joins and limit syntax are owned by the dialect.
  """
  components = _reference_components(reference)
  if not components:
    raise ValueError("Reference has no complete key components.")

  child_dataset = reference.referencing_dataset
  parent_dataset = reference.referenced_dataset

  return dialect.render_reference_integrity_missing_examples_statement(
    child_schema=_physical_schema_name(child_dataset),
    child_table=_physical_dataset_name(child_dataset),
    parent_schema=_physical_schema_name(parent_dataset),
    parent_table=_physical_dataset_name(parent_dataset),
    key_pairs=[
      (component.child_column, component.parent_column)
      for component in components
    ],
    example_limit=example_limit,
  )


def _check_reference(
  reference: TargetDatasetReference,
  *,
  dialect: SqlDialect,
  engine: BaseExecutionEngine | None,
  target_system_name: str | None,
  example_limit: int,
  include_sql: bool,
) -> ReferenceIntegrityReferenceResult:
  """Check one reference and return a compact review result."""
  notes = list(_reference_metadata_notes(reference))
  components = _reference_components(reference)

  if notes or not components:
    return _reference_result(
      reference,
      status="not_checked",
      checked=False,
      components=components,
      missing_examples=(),
      example_limit=example_limit,
      notes=tuple(notes or ["Reference has no complete key components."]),
    )

  try:
    runtime_engine = engine
    if runtime_engine is None:
      target_system = get_target_system(target_system_name)
      runtime_engine = dialect.get_execution_engine(target_system)

    sql = render_missing_parent_examples_sql(
      reference,
      dialect=dialect,
      example_limit=example_limit,
    )
    rows = runtime_engine.fetch_all(sql)
  except Exception as exc:
    return _reference_result(
      reference,
      status="not_checked",
      checked=False,
      components=components,
      missing_examples=(),
      example_limit=example_limit,
      notes=(f"Reference could not be checked: {exc}",),
    )

  examples = tuple(
    _row_to_missing_example(row, components)
    for row in rows[:example_limit]
  )
  status: ReferenceIntegrityStatus = "attention" if examples else "complete"

  return _reference_result(
    reference,
    status=status,
    checked=True,
    components=components,
    missing_examples=examples,
    example_limit=example_limit,
    notes=(),
    sql=sql if include_sql else "",
  )


def _reference_result(
  reference: TargetDatasetReference,
  *,
  status: ReferenceIntegrityStatus,
  checked: bool,
  components: tuple[ReferenceIntegrityComponent, ...],
  missing_examples: tuple[ReferenceIntegrityMissingParentExample, ...],
  example_limit: int,
  notes: tuple[str, ...],
  sql: str = "",
) -> ReferenceIntegrityReferenceResult:
  """Build a deterministic reference-level result."""
  child_dataset = getattr(reference, "referencing_dataset", None)
  parent_dataset = getattr(reference, "referenced_dataset", None)
  child_key = _dataset_key(child_dataset)
  parent_key = _dataset_key(parent_dataset)
  prefix = (getattr(reference, "reference_prefix", None) or "").strip()
  suffix = f"#{prefix}" if prefix else ""

  return ReferenceIntegrityReferenceResult(
    reference_id=getattr(reference, "id", None),
    reference_key=f"{child_key}->{parent_key}{suffix}",
    child_dataset=child_key,
    parent_dataset=parent_key,
    relationship_type=(getattr(reference, "relationship_type", None) or "").strip(),
    status=status,
    checked=checked,
    components=components,
    missing_examples=missing_examples,
    missing_example_count=len(missing_examples),
    example_limit=example_limit,
    notes=tuple(notes),
    sql=sql,
  )


def _load_outgoing_references(
  target_dataset: TargetDataset,
  references: Iterable[TargetDatasetReference] | None,
) -> list[TargetDatasetReference]:
  """Load outgoing references in a stable order."""
  if references is not None:
    return sorted(
      list(references),
      key=lambda r: (
        _dataset_key(getattr(r, "referenced_dataset", None)),
        getattr(r, "reference_prefix", "") or "",
        getattr(r, "id", 0) or 0,
      ),
    )

  manager = getattr(target_dataset, "outgoing_references", None)
  if manager is None:
    return []

  qs = manager.all() if hasattr(manager, "all") else manager
  if hasattr(qs, "select_related"):
    qs = qs.select_related(
      "referencing_dataset__target_schema",
      "referenced_dataset__target_schema",
    )
  if hasattr(qs, "prefetch_related"):
    qs = qs.prefetch_related(
      "key_components__from_column",
      "key_components__to_column",
    )
  if hasattr(qs, "order_by"):
    qs = qs.order_by(
      "referenced_dataset__target_schema__short_name",
      "referenced_dataset__target_dataset_name",
      "reference_prefix",
      "id",
    )
  return list(qs)


def _reference_metadata_notes(reference: TargetDatasetReference) -> tuple[str, ...]:
  """Return reasons why a reference cannot be checked safely."""
  notes: list[str] = []
  child_dataset = getattr(reference, "referencing_dataset", None)
  parent_dataset = getattr(reference, "referenced_dataset", None)

  if child_dataset is None:
    notes.append("Reference has no child dataset.")
  if parent_dataset is None:
    notes.append("Reference has no parent dataset.")

  for label, dataset in (("Child", child_dataset), ("Parent", parent_dataset)):
    if dataset is None:
      continue
    if not getattr(dataset, "active", True):
      notes.append(f"{label} dataset is inactive.")
    if not _physical_schema_name(dataset):
      notes.append(f"{label} dataset has no physical schema name.")
    if not _physical_dataset_name(dataset):
      notes.append(f"{label} dataset has no physical dataset name.")

  if not _reference_components(reference):
    notes.append("Reference has no complete key components.")

  return tuple(notes)


def _reference_components(
  reference: TargetDatasetReference,
) -> tuple[ReferenceIntegrityComponent, ...]:
  """Return complete and active reference components in deterministic order."""
  raw_components = _load_key_components(reference)
  components: list[ReferenceIntegrityComponent] = []

  for raw_component in raw_components:
    from_column = getattr(raw_component, "from_column", None)
    to_column = getattr(raw_component, "to_column", None)
    if from_column is None or to_column is None:
      continue
    if not getattr(from_column, "active", True):
      continue
    if not getattr(to_column, "active", True):
      continue

    child_column = (getattr(from_column, "target_column_name", None) or "").strip()
    parent_column = (getattr(to_column, "target_column_name", None) or "").strip()
    if not child_column or not parent_column:
      continue

    components.append(ReferenceIntegrityComponent(
      child_column=child_column,
      parent_column=parent_column,
      ordinal_position=int(getattr(raw_component, "ordinal_position", 0) or 0),
    ))

  return tuple(sorted(
    components,
    key=lambda c: (c.ordinal_position, c.child_column, c.parent_column),
  ))


def _load_key_components(reference: TargetDatasetReference) -> list[Any]:
  """Load key components in stable order from a real or test double manager."""
  manager = getattr(reference, "key_components", None)
  if manager is None:
    return []

  qs = manager.all() if hasattr(manager, "all") else manager
  if hasattr(qs, "select_related"):
    qs = qs.select_related("from_column", "to_column")
  if hasattr(qs, "order_by"):
    qs = qs.order_by("ordinal_position", "id")
  return list(qs)


def _row_to_missing_example(
  row: Any,
  components: tuple[ReferenceIntegrityComponent, ...],
) -> ReferenceIntegrityMissingParentExample:
  """Convert one engine row into a deterministic missing-parent example."""
  values = tuple(row) if not isinstance(row, tuple) else row
  return ReferenceIntegrityMissingParentExample(values={
    component.child_column: _serialize_value(values[index] if index < len(values) else None)
    for index, component in enumerate(components)
  })


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
