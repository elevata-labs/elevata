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

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from metadata.models import TargetColumn, TargetColumnInput


DEFAULT_GUIDANCE_SCHEMA_SHORT_NAMES = (
  "rawcore",
  "bizcore",
)

EXCLUDED_TARGET_SYSTEM_ROLES = frozenset({
  "surrogate_key",
  "foreign_key",
  "entity_key",
  "row_hash",
  "load_run_id",
  "loaded_at",
  "version_started_at",
  "version_ended_at",
  "version_state",
  "payload",
})

DOMINANT_MIN_USAGE = 3
DOMINANT_USAGE_RATIO = 3
DEFAULT_EXAMPLE_LIMIT = 3


@dataclass(frozen=True)
class NamingGuidanceExample:
  """
  One transparent metadata example behind a naming guidance candidate.
  """
  source_column_id: int
  source_column_name: str
  target_column_id: int
  target_column_name: str
  target_dataset_id: int
  target_dataset_key: str


@dataclass(frozen=True)
class NamingGuidanceCandidate:
  """
  Aggregated target-column name usage for one technical source-column name.
  """
  target_column_name: str
  usage_count: int
  examples: tuple[NamingGuidanceExample, ...]

  @property
  def usage_label(self) -> str:
    """
    Return a compact human-readable usage count label.
    """
    if self.usage_count == 1:
      return "used 1 time"
    return f"used {self.usage_count} times"


@dataclass(frozen=True)
class NamingGuidance:
  """
  Deterministic naming guidance for one technical source-column name.
  """
  source_column_name: str
  status: str
  recommended_target_column_name: str
  candidates: tuple[NamingGuidanceCandidate, ...]

  @property
  def has_usage(self) -> bool:
    """
    Return whether any previous target-column naming usage exists.
    """
    return bool(self.candidates)

  @property
  def has_recommendation(self) -> bool:
    """
    Return whether the guidance contains a concrete recommended target name.
    """
    return bool(self.recommended_target_column_name)

  @property
  def usage_count(self) -> int:
    """
    Return the total number of observed source-to-target naming usages.
    """
    return sum(candidate.usage_count for candidate in self.candidates)


def normalize_source_column_name(value: str | None) -> str:
  """
  Normalize a technical source-column name for deterministic matching.
  """
  return str(value or "").strip().casefold()


def build_naming_guidance_for_source_column(source_column, **kwargs) -> NamingGuidance:
  """
  Build naming guidance for a SourceColumn instance.
  """
  return build_naming_guidance_for_source_column_name(
    getattr(source_column, "source_column_name", ""),
    **kwargs,
  )


def build_naming_guidance_for_source_column_name(
  source_column_name: str,
  *,
  included_schema_short_names: Iterable[str] = DEFAULT_GUIDANCE_SCHEMA_SHORT_NAMES,
  exclude_target_column_id: int | None = None,
  example_limit: int = DEFAULT_EXAMPLE_LIMIT,
) -> NamingGuidance:
  """
  Build read-only naming guidance from existing SourceColumn-to-TargetColumn usage.
  """
  normalized_source_name = normalize_source_column_name(source_column_name)
  if not normalized_source_name:
    return _empty_guidance(source_column_name)

  usages = _collect_guidance_examples(
    normalized_source_name,
    included_schema_short_names=tuple(included_schema_short_names),
    exclude_target_column_id=exclude_target_column_id,
  )
  candidates = _build_candidates(usages, example_limit=example_limit)
  recommended_candidate = _recommended_candidate(candidates)

  status = "new"
  recommended_target_column_name = ""
  if recommended_candidate is not None:
    status = "suggested"
    recommended_target_column_name = recommended_candidate.target_column_name
  elif candidates:
    status = "conflict"

  return NamingGuidance(
    source_column_name=source_column_name,
    status=status,
    recommended_target_column_name=recommended_target_column_name,
    candidates=candidates,
  )


def build_naming_guidance_for_target_column(
  target_column: TargetColumn,
  *,
  included_schema_short_names: Iterable[str] = DEFAULT_GUIDANCE_SCHEMA_SHORT_NAMES,
  example_limit: int = DEFAULT_EXAMPLE_LIMIT,
) -> tuple[NamingGuidance, ...]:
  """
  Build naming guidance for all immediate input-column names feeding a TargetColumn.
  """
  if not getattr(target_column, "pk", None):
    return ()

  input_columns = _resolve_input_columns_for_target_column(target_column.pk)
  source_names = sorted({name for _, name in input_columns}, key=normalize_source_column_name)

  return tuple(
    build_naming_guidance_for_source_column_name(
      source_name,
      included_schema_short_names=included_schema_short_names,
      exclude_target_column_id=target_column.pk,
      example_limit=example_limit,
    )
    for source_name in source_names
  )


def _empty_guidance(source_column_name: str) -> NamingGuidance:
  """
  Return the neutral guidance object for an unknown source-column name.
  """
  return NamingGuidance(
    source_column_name=source_column_name,
    status="new",
    recommended_target_column_name="",
    candidates=(),
  )


def _collect_guidance_examples(
  normalized_source_name: str,
  *,
  included_schema_short_names: tuple[str, ...],
  exclude_target_column_id: int | None,
) -> tuple[NamingGuidanceExample, ...]:
  """
  Collect usage examples for one normalized immediate input-column name.
  """
  examples: list[NamingGuidanceExample] = []

  for target_column in _guidance_target_columns(included_schema_short_names):
    if exclude_target_column_id and target_column.pk == exclude_target_column_id:
      continue

    input_columns = _resolve_input_columns_for_target_column(target_column.pk)
    matching_input_columns = _matching_input_columns_for_guidance(
      input_columns,
      normalized_source_name=normalized_source_name,
    )
    if not matching_input_columns:
      continue

    source_column_id, source_column_name = matching_input_columns[0]
    examples.append(
      NamingGuidanceExample(
        source_column_id=source_column_id,
        source_column_name=source_column_name,
        target_column_id=target_column.pk,
        target_column_name=target_column.target_column_name,
        target_dataset_id=target_column.target_dataset_id,
        target_dataset_key=_target_dataset_key(target_column),
      )
    )

  examples.sort(
    key=lambda item: (
      item.target_column_name,
      item.target_dataset_key,
      item.source_column_name,
      item.source_column_id,
      item.target_column_id,
    )
  )
  return tuple(examples)


def _guidance_target_columns(included_schema_short_names: tuple[str, ...]):
  """
  Return target columns that should contribute to naming guidance.
  """
  return (
    TargetColumn.objects
    .filter(
      active=True,
      target_dataset__active=True,
      target_dataset__target_schema__short_name__in=included_schema_short_names,
    )
    .exclude(target_dataset__incremental_strategy="historize")
    .exclude(system_role__in=EXCLUDED_TARGET_SYSTEM_ROLES)
    .select_related("target_dataset", "target_dataset__target_schema")
    .order_by(
      "target_column_name",
      "target_dataset__target_schema__short_name",
      "target_dataset__target_dataset_name",
      "id",
    )
  )


def _resolve_input_columns_for_target_column(
  target_column_id: int,
) -> frozenset[tuple[int, str]]:
  """
  Resolve immediate input-column names feeding a target column.

  Direct source inputs use SourceColumn.source_column_name.
  Upstream target inputs use the upstream TargetColumn.target_column_name.
  The lookup intentionally stops after one edge so guidance reflects the
  current modelling step rather than the original system column.
  """
  input_columns: set[tuple[int, str]] = set()
  input_links = (
    TargetColumnInput.objects
    .filter(active=True, target_column_id=target_column_id)
    .select_related("source_column", "upstream_target_column")
    .order_by("ordinal_position", "id")
  )

  for input_link in input_links:
    if input_link.source_column_id and input_link.source_column is not None:
      source_column_name = (input_link.source_column.source_column_name or "").strip()
      if source_column_name:
        input_columns.add((input_link.source_column_id, source_column_name))
      continue

    if (
      input_link.upstream_target_column_id
      and input_link.upstream_target_column is not None
    ):
      upstream_column_name = (
        input_link.upstream_target_column.target_column_name or ""
      ).strip()
      if upstream_column_name:
        input_columns.add((input_link.upstream_target_column_id, upstream_column_name))

  return frozenset(input_columns)


def _matching_input_columns_for_guidance(
  input_columns: frozenset[tuple[int, str]],
  *,
  normalized_source_name: str,
) -> tuple[tuple[int, str], ...]:
  """
  Return matching input columns only for clear single-input-name lineage.

  Naming guidance is intentionally based on deterministic 1:1-style evidence.
  If a target column resolves to multiple different input column names,
  we treat it as ambiguous and exclude it from guidance examples.
  """
  normalized_source_names = {
    normalize_source_column_name(source_column_name)
    for _, source_column_name in input_columns
    if normalize_source_column_name(source_column_name)
  }

  if normalized_source_name not in normalized_source_names:
    return ()

  if len(normalized_source_names) != 1:
    return ()

  matching_source_columns = [
    (source_column_id, source_column_name)
    for source_column_id, source_column_name in sorted(input_columns)
    if normalize_source_column_name(source_column_name) == normalized_source_name
  ]
  return tuple(matching_source_columns)


def _build_candidates(
  examples: tuple[NamingGuidanceExample, ...],
  *,
  example_limit: int,
) -> tuple[NamingGuidanceCandidate, ...]:
  """
  Aggregate naming usage examples into sorted guidance candidates.
  """
  examples_by_target_name: dict[str, list[NamingGuidanceExample]] = defaultdict(list)
  for example in examples:
    examples_by_target_name[example.target_column_name].append(example)

  candidates = [
    NamingGuidanceCandidate(
      target_column_name=target_column_name,
      usage_count=len(candidate_examples),
      examples=tuple(candidate_examples[:example_limit]),
    )
    for target_column_name, candidate_examples in examples_by_target_name.items()
  ]
  candidates.sort(key=lambda item: (-item.usage_count, item.target_column_name))
  return tuple(candidates)


def _recommended_candidate(
  candidates: tuple[NamingGuidanceCandidate, ...],
) -> NamingGuidanceCandidate | None:
  """
  Return the deterministic recommended candidate, if the usage pattern is clear.
  """
  if not candidates:
    return None

  if len(candidates) == 1:
    return candidates[0]

  top_candidate = candidates[0]
  second_candidate = candidates[1]
  if (
    top_candidate.usage_count >= DOMINANT_MIN_USAGE
    and top_candidate.usage_count >= second_candidate.usage_count * DOMINANT_USAGE_RATIO
  ):
    return top_candidate

  return None


def _target_dataset_key(target_column: TargetColumn) -> str:
  """
  Return a compact schema.dataset key for a TargetColumn.
  """
  target_dataset = target_column.target_dataset
  target_schema = getattr(target_dataset, "target_schema", None)
  schema_short_name = getattr(target_schema, "short_name", "") or ""
  dataset_name = getattr(target_dataset, "target_dataset_name", "") or ""
  return f"{schema_short_name}.{dataset_name}".strip(".")
