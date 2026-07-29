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

from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
import hashlib
import json
from typing import Any

from .execution_impact import (
  ExecutionImpactDecision,
  ExecutionImpactEvidenceReference,
  ExecutionImpactItem,
  ExecutionImpactPlan,
  ExecutionImpactPropagation,
  ExecutionImpactReasonCode,
  strongest_execution_impact_decision,
)


_DATA_PROPAGATION_DEPENDENCY_REASONS = frozenset({
  "lineage_input",
  "source_raw_ready",
})

_READINESS_ONLY_DEPENDENCY_REASONS = frozenset({
  "reference_parent_ready",
})

_EXECUTION_DECISIONS = frozenset({
  "INCREMENTAL_EXECUTE",
  "FULL_REBUILD",
})

_DEPENDENCY_MODES = frozenset({
  "with_dependencies",
  "target_only",
})


@dataclass(frozen=True)
class _ExecutionImpactDependencyEdge:
  """
  Normalized immediate execution dependency used for impact propagation.
  """
  downstream_dataset_key: str
  upstream_dataset_key: str
  reason: str
  reference_id: int | None = None

  @property
  def evidence_key(self) -> str:
    """
    Return a stable dependency evidence key.
    """
    reference_token = (
      str(self.reference_id)
      if self.reference_id is not None
      else "none"
    )
    return (
      f"{self.downstream_dataset_key}<-{self.upstream_dataset_key}"
      f":{self.reason}:{reference_token}"
    )

  @property
  def fingerprint(self) -> str:
    """
    Return a deterministic fingerprint for this dependency edge.
    """
    return _stable_json_hash(self.to_dict())

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "downstream_dataset_key": self.downstream_dataset_key,
      "upstream_dataset_key": self.upstream_dataset_key,
      "reason": self.reason,
      "reference_id": self.reference_id,
    }


@dataclass(frozen=True)
class _TargetExecutionSemantics:
  """
  Current TargetDataset execution semantics relevant to propagation.
  """
  materialization_type: str
  incremental_strategy: str
  is_virtual: bool
  supports_incremental_execution: bool


def propagate_execution_impact_plan(
  *,
  local_plan: ExecutionImpactPlan,
  target_datasets: Iterable[Any],
  dependency_resolver: Callable[[Any], Iterable[Any]] | None = None,
  dependency_mode: str = "with_dependencies",
) -> ExecutionImpactPlan:
  """
  Propagate dataset-local impact decisions across execution dependencies.

  Propagation is read-only and conservative:
  - lineage_input and source_raw_ready propagate data-impact decisions
  - reference_parent_ready remains a readiness and ordering dependency
  - missing or unsupported dependency evidence blocks unsafe decisions
  - no execution behavior is changed by this function
  """
  resolver = dependency_resolver or _default_dependency_resolver
  resolved_dependency_mode = _normalize_dependency_mode(dependency_mode)
  target_by_key = _target_datasets_by_key(target_datasets)
  target_semantics_by_key = {
    dataset_key: _target_execution_semantics(target_dataset)
    for dataset_key, target_dataset in target_by_key.items()
  }
  item_by_key = {
    item.dataset_key: item
    for item in local_plan.items
  }

  edges, resolution_errors = _resolve_dependency_edges(
    item_by_key=item_by_key,
    target_by_key=target_by_key,
    dependency_resolver=resolver,
  )
  incoming_by_downstream = _incoming_edges_by_downstream(edges)
  downstream_by_upstream = _downstream_keys_by_upstream(edges)
  order = _topological_dataset_order(
    dataset_keys=tuple(item_by_key),
    incoming_by_downstream=incoming_by_downstream,
  )

  propagated_by_key: dict[str, ExecutionImpactItem] = {}
  for dataset_key in order:
    propagated_by_key[dataset_key] = _propagate_item(
      item=item_by_key[dataset_key],
      incoming_edges=incoming_by_downstream.get(dataset_key, ()),
      downstream_dataset_keys=downstream_by_upstream.get(dataset_key, ()),
      propagated_by_key=propagated_by_key,
      target_semantics=target_semantics_by_key.get(dataset_key),
      resolution_error=resolution_errors.get(dataset_key),
      dependency_mode=resolved_dependency_mode,
    )

  return ExecutionImpactPlan(
    scope_key=local_plan.scope_key,
    architecture_fingerprint=local_plan.architecture_fingerprint,
    baseline_fingerprint=local_plan.baseline_fingerprint,
    report_fingerprint=local_plan.report_fingerprint,
    items=tuple(propagated_by_key.values()),
    evidence=_merge_evidence(
      local_plan.evidence,
      (
        _dependency_mode_evidence(
          scope_key=local_plan.scope_key,
          dependency_mode=resolved_dependency_mode,
          dataset_keys=tuple(item_by_key),
        ),
        _dependency_graph_evidence(
          scope_key=local_plan.scope_key,
          edges=edges,
          resolution_errors=resolution_errors,
        ),
      ),
    ),
  )


def _default_dependency_resolver(target_dataset: Any) -> Iterable[Any]:
  """
  Resolve dependencies through the canonical execution dependency service.
  """
  from metadata.execution.load_graph import resolve_execution_dependencies

  return resolve_execution_dependencies(target_dataset)


def _target_datasets_by_key(
  target_datasets: Iterable[Any],
) -> dict[str, Any]:
  """
  Index TargetDataset-shaped objects by deterministic dataset key.
  """
  indexed: dict[str, Any] = {}
  for target_dataset in target_datasets:
    dataset_key = _target_dataset_key(target_dataset)
    if dataset_key in indexed:
      raise ValueError(
        "Duplicate TargetDataset supplied for execution impact propagation: "
        f"{dataset_key}"
      )
    indexed[dataset_key] = target_dataset
  return indexed


def _target_dataset_key(target_dataset: Any) -> str:
  """
  Return the stable dataset key for a TargetDataset-shaped object.
  """
  schema_short = str(
    getattr(getattr(target_dataset, "target_schema", None), "short_name", "")
    or ""
  ).strip()
  target_name = str(
    getattr(target_dataset, "target_dataset_name", "") or ""
  ).strip()
  if not schema_short or not target_name:
    raise ValueError(
      "Execution impact propagation requires TargetDataset schema and name."
    )
  return f"{schema_short}.{target_name}"


def _target_execution_semantics(
  target_dataset: Any,
) -> _TargetExecutionSemantics:
  """
  Resolve current materialization and incremental execution semantics.
  """
  effective_materialization = getattr(
    target_dataset,
    "effective_materialization_type",
    None,
  )
  if callable(effective_materialization):
    effective_materialization = effective_materialization()

  materialization_type = str(
    effective_materialization
    or getattr(target_dataset, "materialization_type", None)
    or getattr(
      getattr(target_dataset, "target_schema", None),
      "default_materialization_type",
      None,
    )
    or "table"
  ).strip().lower()
  incremental_strategy = str(
    getattr(target_dataset, "incremental_strategy", None)
    or "full"
  ).strip().lower()

  return _TargetExecutionSemantics(
    materialization_type=materialization_type,
    incremental_strategy=incremental_strategy,
    is_virtual=materialization_type == "view",
    supports_incremental_execution=(
      incremental_strategy in {"append", "merge", "snapshot", "historize"}
    ),
  )


def _resolve_dependency_edges(
  *,
  item_by_key: dict[str, ExecutionImpactItem],
  target_by_key: dict[str, Any],
  dependency_resolver: Callable[[Any], Iterable[Any]],
) -> tuple[
  tuple[_ExecutionImpactDependencyEdge, ...],
  dict[str, str],
]:
  """
  Resolve and normalize immediate dependencies for every assessed dataset.
  """
  edges: dict[tuple[str, str, str, int | None], _ExecutionImpactDependencyEdge] = {}
  errors: dict[str, set[str]] = {}

  def add_error(dataset_key: str, message: str) -> None:
    errors.setdefault(dataset_key, set()).add(message)

  for dataset_key in sorted(item_by_key):
    target_dataset = target_by_key.get(dataset_key)
    if target_dataset is None:
      add_error(
        dataset_key,
        "The assessed dataset is unavailable for execution dependency "
        "resolution.",
      )
      continue

    try:
      dependencies = tuple(dependency_resolver(target_dataset) or ())
    except Exception as exc:
      add_error(
        dataset_key,
        "Execution dependency resolution failed: "
        f"{exc}",
      )
      continue

    for dependency in dependencies:
      upstream = getattr(dependency, "upstream", None)
      if upstream is None:
        add_error(
          dataset_key,
          "Execution dependency resolution returned an entry without an "
          "upstream TargetDataset.",
        )
        continue

      try:
        upstream_key = _target_dataset_key(upstream)
      except ValueError as exc:
        add_error(dataset_key, str(exc))
        continue

      reason = str(getattr(dependency, "reason", "") or "").strip()
      if not reason:
        add_error(
          dataset_key,
          "Execution dependency resolution returned an entry without a reason.",
        )
        continue

      reference_id = getattr(dependency, "reference_id", None)
      try:
        normalized_reference_id = (
          int(reference_id)
          if reference_id is not None
          else None
        )
      except (TypeError, ValueError):
        add_error(
          dataset_key,
          "Execution dependency reference id must be an integer when set.",
        )
        continue

      edge = _ExecutionImpactDependencyEdge(
        downstream_dataset_key=dataset_key,
        upstream_dataset_key=upstream_key,
        reason=reason,
        reference_id=normalized_reference_id,
      )
      edges[(
        edge.downstream_dataset_key,
        edge.upstream_dataset_key,
        edge.reason,
        edge.reference_id,
      )] = edge

  return (
    tuple(sorted(edges.values(), key=_dependency_edge_sort_key)),
    {
      dataset_key: " ".join(sorted(messages))
      for dataset_key, messages in errors.items()
    },
  )


def _incoming_edges_by_downstream(
  edges: tuple[_ExecutionImpactDependencyEdge, ...],
) -> dict[str, tuple[_ExecutionImpactDependencyEdge, ...]]:
  """
  Group normalized dependency edges by downstream dataset.
  """
  grouped: dict[str, list[_ExecutionImpactDependencyEdge]] = {}
  for edge in edges:
    grouped.setdefault(edge.downstream_dataset_key, []).append(edge)
  return {
    dataset_key: tuple(sorted(items, key=_dependency_edge_sort_key))
    for dataset_key, items in grouped.items()
  }


def _downstream_keys_by_upstream(
  edges: tuple[_ExecutionImpactDependencyEdge, ...],
) -> dict[str, tuple[str, ...]]:
  """
  Build deterministic immediate downstream scopes for assessed datasets.
  """
  grouped: dict[str, set[str]] = {}
  for edge in edges:
    grouped.setdefault(edge.upstream_dataset_key, set()).add(
      edge.downstream_dataset_key
    )
  return {
    dataset_key: tuple(sorted(keys))
    for dataset_key, keys in grouped.items()
  }


def _topological_dataset_order(
  *,
  dataset_keys: tuple[str, ...],
  incoming_by_downstream: dict[
    str,
    tuple[_ExecutionImpactDependencyEdge, ...],
  ],
) -> tuple[str, ...]:
  """
  Return assessed datasets in deterministic upstream-first order.
  """
  assessed = set(dataset_keys)
  state: dict[str, int] = {}
  ordered: list[str] = []
  stack: list[str] = []

  def visit(dataset_key: str) -> None:
    status = state.get(dataset_key, 0)
    if status == 2:
      return
    if status == 1:
      cycle_start = stack.index(dataset_key)
      cycle = stack[cycle_start:] + [dataset_key]
      raise ValueError(
        "Cycle detected in execution impact dependency graph: "
        + " -> ".join(cycle)
      )

    state[dataset_key] = 1
    stack.append(dataset_key)
    upstream_keys = sorted({
      edge.upstream_dataset_key
      for edge in incoming_by_downstream.get(dataset_key, ())
      if edge.upstream_dataset_key in assessed
    })
    for upstream_key in upstream_keys:
      visit(upstream_key)
    stack.pop()
    state[dataset_key] = 2
    ordered.append(dataset_key)

  for dataset_key in sorted(dataset_keys):
    visit(dataset_key)

  return tuple(ordered)


def _propagate_item(
  *,
  item: ExecutionImpactItem,
  incoming_edges: tuple[_ExecutionImpactDependencyEdge, ...],
  downstream_dataset_keys: tuple[str, ...],
  propagated_by_key: dict[str, ExecutionImpactItem],
  target_semantics: _TargetExecutionSemantics | None,
  resolution_error: str | None,
  dependency_mode: str,
) -> ExecutionImpactItem:
  """
  Apply immediate upstream decisions to one dataset impact item.
  """
  decision = item.decision
  reason_codes: set[ExecutionImpactReasonCode] = set(item.reason_codes)
  evidence = list(item.evidence)
  propagations = list(item.propagations)
  upstream_dataset_keys = {
    edge.upstream_dataset_key
    for edge in incoming_edges
  }
  readiness_edges: list[_ExecutionImpactDependencyEdge] = []

  if resolution_error:
    decision = strongest_execution_impact_decision(decision, "BLOCKED")
    reason_codes.add("MANUAL_REVIEW_REQUIRED")
    evidence.append(ExecutionImpactEvidenceReference(
      evidence_type="execution_dependency",
      evidence_key=f"dataset:{item.dataset_key}:resolution",
      status="unavailable",
      required=True,
      message=resolution_error,
    ))

  for edge in incoming_edges:
    upstream_item = propagated_by_key.get(edge.upstream_dataset_key)

    if edge.reason in _READINESS_ONLY_DEPENDENCY_REASONS:
      readiness_edges.append(edge)
      continue

    if (
      upstream_item is None
      and dependency_mode == "target_only"
      and edge.reason in _DATA_PROPAGATION_DEPENDENCY_REASONS
    ):
      evidence.append(_available_dependency_evidence(
        edge,
        required=False,
        message=(
          "Upstream readiness is assumed because target-only execution "
          "explicitly excludes execution dependencies."
        ),
      ))
      continue

    if upstream_item is None:
      decision = strongest_execution_impact_decision(decision, "BLOCKED")
      reason_codes.add("MANUAL_REVIEW_REQUIRED")
      evidence.append(_unavailable_dependency_evidence(
        edge,
        message=(
          "The upstream dataset is outside the assessed Execution Impact Plan."
        ),
      ))
      continue

    evidence.append(_available_dependency_evidence(edge))

    if edge.reason not in _DATA_PROPAGATION_DEPENDENCY_REASONS:
      decision = strongest_execution_impact_decision(decision, "BLOCKED")
      reason_codes.add("MANUAL_REVIEW_REQUIRED")
      propagations.append(ExecutionImpactPropagation(
        upstream_dataset_key=edge.upstream_dataset_key,
        upstream_decision=upstream_item.decision,
        dependency_reason=edge.reason,
        reason_code="MANUAL_REVIEW_REQUIRED",
      ))
      continue

    propagated = _data_dependency_propagation(
      upstream_decision=upstream_item.decision,
      downstream_decision=decision,
      target_semantics=target_semantics,
    )
    if propagated is None:
      continue

    propagated_decision, propagated_reasons, propagation_reason = propagated
    if propagated_decision is not None:
      decision = strongest_execution_impact_decision(
        decision,
        propagated_decision,
      )
    reason_codes.update(propagated_reasons)
    propagations.append(ExecutionImpactPropagation(
      upstream_dataset_key=edge.upstream_dataset_key,
      upstream_decision=upstream_item.decision,
      dependency_reason=edge.reason,
      reason_code=propagation_reason,
    ))

  for edge in readiness_edges:
    upstream_item = propagated_by_key.get(edge.upstream_dataset_key)

    if upstream_item is None and dependency_mode == "target_only":
      evidence.append(_available_dependency_evidence(
        edge,
        required=False,
        message=(
          "Reference-parent readiness is assumed because target-only execution "
          "explicitly excludes execution dependencies."
        ),
      ))
      continue

    if upstream_item is None:
      if decision in _EXECUTION_DECISIONS:
        decision = strongest_execution_impact_decision(decision, "BLOCKED")
        reason_codes.add("MANUAL_REVIEW_REQUIRED")
        evidence.append(_unavailable_dependency_evidence(
          edge,
          message=(
            "Reference-parent readiness cannot be evaluated because the "
            "upstream dataset is outside the assessed plan."
          ),
        ))
      else:
        evidence.append(_available_dependency_evidence(
          edge,
          required=False,
          message=(
            "Reference-parent readiness is not required because this dataset "
            "does not require data execution."
          ),
        ))
      continue

    evidence.append(_available_dependency_evidence(edge))

    if (
      upstream_item.decision == "BLOCKED"
      and decision in _EXECUTION_DECISIONS
    ):
      decision = "BLOCKED"
      reason_codes.add("UPSTREAM_BLOCKED")
      propagations.append(ExecutionImpactPropagation(
        upstream_dataset_key=edge.upstream_dataset_key,
        upstream_decision=upstream_item.decision,
        dependency_reason=edge.reason,
        reason_code="UPSTREAM_BLOCKED",
      ))

  if decision != "REUSE" or propagations:
    reason_codes.discard("NO_RELEVANT_CHANGE")

  if decision != "INCREMENTAL_EXECUTE":
    reason_codes.discard("INCREMENTAL_LOAD_STRATEGY")

  if decision != "FULL_REBUILD":
    reason_codes.discard("FULL_REFRESH_STRATEGY")

  if decision != "REUSE":
    reason_codes.discard("VIRTUAL_MATERIALIZATION_REUSED")

  return replace(
    item,
    decision=decision,
    reason_codes=tuple(sorted(reason_codes)),
    evidence=_merge_evidence(item.evidence, tuple(evidence)),
    upstream_dataset_keys=tuple(sorted(upstream_dataset_keys)),
    downstream_dataset_keys=downstream_dataset_keys,
    propagations=_merge_propagations(
      item.propagations,
      tuple(propagations),
    ),
  )


def _data_dependency_propagation(
  *,
  upstream_decision: ExecutionImpactDecision,
  downstream_decision: ExecutionImpactDecision,
  target_semantics: _TargetExecutionSemantics | None,
) -> tuple[
  ExecutionImpactDecision | None,
  tuple[ExecutionImpactReasonCode, ...],
  ExecutionImpactReasonCode,
] | None:
  """
  Map a data-bearing upstream decision to safe downstream impact.
  """
  if upstream_decision == "BLOCKED":
    return "BLOCKED", ("UPSTREAM_BLOCKED",), "UPSTREAM_BLOCKED"

  if upstream_decision not in {
    "INCREMENTAL_EXECUTE",
    "FULL_REBUILD",
  }:
    return None

  if (
    target_semantics is not None
    and target_semantics.is_virtual
    and downstream_decision in {"REUSE", "REVALIDATE"}
  ):
    # A virtual materialization can observe incremental upstream changes
    # without being recreated. A full upstream rebuild is different: the
    # load runner may need to drop managed dependent views before replacing
    # the physical relation. The view must therefore participate in execution
    # so its own dataset step recreates it deterministically.
    if upstream_decision == "FULL_REBUILD":
      return (
        "FULL_REBUILD",
        ("UPSTREAM_REBUILD_REQUIRED",),
        "UPSTREAM_REBUILD_REQUIRED",
      )

    return (
      None,
      (
        "UPSTREAM_EXECUTION_REQUIRED",
        "VIRTUAL_MATERIALIZATION_REUSED",
      ),
      "UPSTREAM_EXECUTION_REQUIRED",
    )

  if (
    target_semantics is not None
    and target_semantics.supports_incremental_execution
  ):
    return (
      "INCREMENTAL_EXECUTE",
      (
        "INCREMENTAL_LOAD_STRATEGY",
        "UPSTREAM_EXECUTION_REQUIRED",
      ),
      "UPSTREAM_EXECUTION_REQUIRED",
    )

  if upstream_decision == "FULL_REBUILD":
    return (
      "FULL_REBUILD",
      ("UPSTREAM_REBUILD_REQUIRED",),
      "UPSTREAM_REBUILD_REQUIRED",
    )

  return (
    "FULL_REBUILD",
    ("UPSTREAM_EXECUTION_REQUIRED",),
    "UPSTREAM_EXECUTION_REQUIRED",
  )


def _available_dependency_evidence(
  edge: _ExecutionImpactDependencyEdge,
  *,
  required: bool = True,
  message: str | None = None,
) -> ExecutionImpactEvidenceReference:
  """
  Build available evidence for one resolved dependency edge.
  """
  return ExecutionImpactEvidenceReference(
    evidence_type="execution_dependency",
    evidence_key=edge.evidence_key,
    status="available",
    required=required,
    fingerprint=edge.fingerprint,
    artifact_reference=(
      str(edge.reference_id)
      if edge.reference_id is not None
      else None
    ),
    message=message or (
      "Immediate execution dependency used for impact propagation."
    ),
  )


def _unavailable_dependency_evidence(
  edge: _ExecutionImpactDependencyEdge,
  *,
  message: str,
) -> ExecutionImpactEvidenceReference:
  """
  Build unavailable evidence when an upstream decision cannot be resolved.
  """
  return ExecutionImpactEvidenceReference(
    evidence_type="execution_dependency",
    evidence_key=edge.evidence_key,
    status="unavailable",
    required=True,
    artifact_reference=(
      str(edge.reference_id)
      if edge.reference_id is not None
      else None
    ),
    message=message,
  )


def _dependency_graph_evidence(
  *,
  scope_key: str,
  edges: tuple[_ExecutionImpactDependencyEdge, ...],
  resolution_errors: dict[str, str],
) -> ExecutionImpactEvidenceReference:
  """
  Build scope-level evidence for the normalized dependency graph result.
  """
  payload = {
    "scope_key": scope_key,
    "edges": [edge.to_dict() for edge in edges],
    "resolution_errors": {
      key: resolution_errors[key]
      for key in sorted(resolution_errors)
    },
  }
  return ExecutionImpactEvidenceReference(
    evidence_type="execution_dependency",
    evidence_key=f"scope:{scope_key}",
    status="available",
    required=True,
    fingerprint=_stable_json_hash(payload),
    message=(
      f"{len(edges)} execution dependency edge(s) resolved for impact "
      f"propagation; {len(resolution_errors)} dataset resolution error(s)."
    ),
  )


def _dependency_mode_evidence(
  *,
  scope_key: str,
  dependency_mode: str,
  dataset_keys: tuple[str, ...],
) -> ExecutionImpactEvidenceReference:
  """
  Bind the dependency mode and exact assessed dataset scope to the plan.
  """
  payload = {
    "scope_key": scope_key,
    "dependency_mode": dependency_mode,
    "dataset_keys": sorted(dataset_keys),
  }
  message = (
    "Execution dependencies are included in the assessed execution scope."
    if dependency_mode == "with_dependencies"
    else (
      "Target-only execution assesses only the selected TargetDataset and "
      "assumes that excluded upstream datasets are ready for consumption."
    )
  )
  return ExecutionImpactEvidenceReference(
    evidence_type="execution_dependency",
    evidence_key=f"dependency_mode:{scope_key}",
    status="available",
    required=True,
    fingerprint=_stable_json_hash(payload),
    artifact_reference=dependency_mode,
    message=message,
  )


def _normalize_dependency_mode(value: str) -> str:
  """
  Validate the dependency mode used for impact propagation.
  """
  normalized = str(value or "").strip()
  if normalized not in _DEPENDENCY_MODES:
    raise ValueError(
      f"Unsupported execution impact dependency mode: {normalized}"
    )
  return normalized


def _merge_evidence(
  *groups: tuple[ExecutionImpactEvidenceReference, ...],
) -> tuple[ExecutionImpactEvidenceReference, ...]:
  """
  Merge evidence references deterministically without duplicates.
  """
  merged: dict[
    tuple[str, str, str, bool, str | None],
    ExecutionImpactEvidenceReference,
  ] = {}
  for group in groups:
    for item in group:
      key = (
        item.evidence_type,
        item.evidence_key,
        item.status,
        item.required,
        item.fingerprint,
      )
      merged[key] = item
  return tuple(sorted(
    merged.values(),
    key=lambda item: (
      item.evidence_type,
      item.evidence_key,
      item.status,
      item.fingerprint or "",
    ),
  ))


def _merge_propagations(
  *groups: tuple[ExecutionImpactPropagation, ...],
) -> tuple[ExecutionImpactPropagation, ...]:
  """
  Merge propagation explanations deterministically without duplicates.
  """
  merged: dict[
    tuple[str, str, str, str],
    ExecutionImpactPropagation,
  ] = {}
  for group in groups:
    for item in group:
      key = (
        item.upstream_dataset_key,
        item.upstream_decision,
        item.dependency_reason,
        item.reason_code,
      )
      merged[key] = item
  return tuple(sorted(
    merged.values(),
    key=lambda item: (
      item.upstream_dataset_key,
      item.dependency_reason,
      item.upstream_decision,
      item.reason_code,
    ),
  ))


def _dependency_edge_sort_key(
  edge: _ExecutionImpactDependencyEdge,
) -> tuple[str, str, str, int]:
  """
  Return the deterministic dependency edge sort key.
  """
  return (
    edge.downstream_dataset_key,
    edge.upstream_dataset_key,
    edge.reason,
    -1 if edge.reference_id is None else edge.reference_id,
  )


def _stable_json_hash(value: Any) -> str:
  """
  Return a deterministic SHA-256 hash for a JSON-serializable value.
  """
  payload = json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
    default=str,
  )
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()
