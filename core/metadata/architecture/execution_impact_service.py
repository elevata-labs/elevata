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

from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from typing import Any

from .execution_impact import ExecutionImpactPlan
from .execution_impact_decisions import build_local_execution_impact_plan
from .execution_impact_evidence import (
  ExecutionImpactEvidenceResolution,
  resolve_execution_impact_evidence,
)
from .execution_impact_propagation import propagate_execution_impact_plan
from .state import ArchitectureState


class ExecutionImpactPlanError(ValueError):
  """
  Raised when an Execution Impact Plan cannot be assembled safely.
  """


_EXECUTION_DEPENDENCY_MODES = frozenset({
  "with_dependencies",
  "target_only",
})


def build_execution_impact_plan(
  *,
  scope_key: str,
  current_state: ArchitectureState,
  baseline_resolution: Any,
  report: Any,
  review_status: Any,
  target_datasets: Iterable[Any],
  execution_record_store: Any | None = None,
  dependency_resolver: Callable[[Any], Iterable[Any]] | None = None,
  execution_dataset_keys: Iterable[str] | None = None,
  dependency_mode: str = "with_dependencies",
) -> ExecutionImpactPlan:
  """
  Build the final read-only Execution Impact Plan for one architecture scope.

  The caller supplies already resolved Architecture Control inputs so the
  service does not rebuild architecture state, comparison baselines, reports or
  review status independently. This keeps all impact stages bound to the same
  authoritative runtime context.
  """
  targets = tuple(target_datasets)
  assessed_dataset_keys = _normalize_execution_dataset_keys(
    execution_dataset_keys
  )
  resolved_dependency_mode = _normalize_dependency_mode(dependency_mode)

  _validate_authoritative_inputs(
    scope_key=scope_key,
    current_state=current_state,
    baseline_resolution=baseline_resolution,
    report=report,
    review_status=review_status,
  )

  evidence_resolution = _resolve_evidence(
    scope_key=scope_key,
    current_state=current_state,
    baseline_resolution=baseline_resolution,
    report=report,
    review_status=review_status,
    execution_record_store=execution_record_store,
    dataset_keys=assessed_dataset_keys,
  )
  evidence_resolution = _bind_target_execution_semantics(
    evidence_resolution=evidence_resolution,
    target_datasets=targets,
  )
  local_plan = _build_local_plan(evidence_resolution)
  final_plan = _propagate_plan(
    local_plan=local_plan,
    target_datasets=targets,
    dependency_resolver=dependency_resolver,
    dependency_mode=resolved_dependency_mode,
  )

  _validate_assembled_plan(
    evidence_resolution=evidence_resolution,
    local_plan=local_plan,
    final_plan=final_plan,
    expected_dataset_keys=assessed_dataset_keys,
  )
  return final_plan


def _validate_authoritative_inputs(
  *,
  scope_key: str,
  current_state: ArchitectureState,
  baseline_resolution: Any,
  report: Any,
  review_status: Any,
) -> None:
  """
  Verify that all supplied control artifacts describe the same evaluation.
  """
  normalized_scope_key = str(scope_key or "").strip()
  if not normalized_scope_key:
    raise ExecutionImpactPlanError(
      "Execution Impact Plan scope key must not be empty."
    )

  if not isinstance(current_state, ArchitectureState):
    raise ExecutionImpactPlanError(
      "Execution Impact Plan requires an ArchitectureState as current state."
    )

  report_payload = _report_payload(report)
  report_fingerprint = str(
    getattr(report, "report_fingerprint", None)
    or report_payload.get("report_fingerprint")
    or ""
  ).strip()
  if not report_fingerprint:
    raise ExecutionImpactPlanError(
      "Execution Impact Plan requires an Architecture Change Report fingerprint."
    )

  state_payload = report_payload.get("state") or {}
  if not isinstance(state_payload, Mapping):
    raise ExecutionImpactPlanError(
      "Architecture Change Report state payload must be a mapping."
    )

  reported_current_fingerprint = str(
    state_payload.get("current_fingerprint") or ""
  ).strip()
  if (
    reported_current_fingerprint
    and reported_current_fingerprint != current_state.fingerprint
  ):
    raise ExecutionImpactPlanError(
      "Architecture Change Report current fingerprint does not match the "
      "supplied Architecture State."
    )

  previous_state = getattr(baseline_resolution, "previous_state", None)
  baseline_fingerprint = (
    previous_state.fingerprint
    if isinstance(previous_state, ArchitectureState)
    else None
  )
  reported_previous_fingerprint = str(
    state_payload.get("previous_fingerprint") or ""
  ).strip() or None
  if reported_previous_fingerprint != baseline_fingerprint:
    raise ExecutionImpactPlanError(
      "Architecture Change Report previous fingerprint does not match the "
      "supplied baseline resolution."
    )

  review_report_fingerprint = str(
    getattr(review_status, "report_fingerprint", None) or ""
  ).strip()
  if (
    review_report_fingerprint
    and review_report_fingerprint != report_fingerprint
  ):
    raise ExecutionImpactPlanError(
      "Architecture review status does not belong to the supplied "
      "Architecture Change Report."
    )

  review_scope_key = str(
    getattr(review_status, "dataset_key", None) or ""
  ).strip()
  if review_scope_key and review_scope_key != normalized_scope_key:
    raise ExecutionImpactPlanError(
      "Architecture review status scope does not match the Execution Impact "
      "Plan scope."
    )


def _resolve_evidence(
  *,
  scope_key: str,
  current_state: ArchitectureState,
  baseline_resolution: Any,
  report: Any,
  review_status: Any,
  execution_record_store: Any | None,
  dataset_keys: tuple[str, ...] | None,
) -> ExecutionImpactEvidenceResolution:
  """
  Resolve evidence while preserving a stable service-level error boundary.
  """
  try:
    return resolve_execution_impact_evidence(
      scope_key=scope_key,
      current_state=current_state,
      baseline_resolution=baseline_resolution,
      report=report,
      review_status=review_status,
      execution_record_store=execution_record_store,
      dataset_keys=dataset_keys,
    )
  except (TypeError, ValueError) as exc:
    raise ExecutionImpactPlanError(
      f"Execution impact evidence resolution failed: {exc}"
    ) from exc


def _bind_target_execution_semantics(
  *,
  evidence_resolution: ExecutionImpactEvidenceResolution,
  target_datasets: tuple[Any, ...],
) -> ExecutionImpactEvidenceResolution:
  """
  Bind impact evidence to effective runtime execution semantics.

  Architecture State may preserve an empty dataset-level materialization
  override because an empty value means "inherit the schema default". Impact
  decisions must use the effective TargetDataset value instead of interpreting
  that inherited state as unknown.
  """
  targets_by_key: dict[str, Any] = {}
  for target_dataset in target_datasets:
    dataset_key = _target_dataset_key(target_dataset)
    if dataset_key in targets_by_key:
      raise ExecutionImpactPlanError(
        "Execution Impact Plan received duplicate TargetDataset runtime "
        f"metadata: {dataset_key}"
      )
    targets_by_key[dataset_key] = target_dataset

  evidence_keys = {
    item.dataset_key
    for item in evidence_resolution.datasets
  }
  missing_keys = tuple(sorted(evidence_keys - set(targets_by_key)))
  if missing_keys:
    raise ExecutionImpactPlanError(
      "Execution Impact Plan is missing TargetDataset runtime metadata for: "
      + ", ".join(missing_keys)
    )

  datasets = tuple(
    replace(
      item,
      materialization_type=_effective_materialization_type(
        targets_by_key[item.dataset_key]
      ),
      incremental_strategy=_target_incremental_strategy(
        targets_by_key[item.dataset_key]
      ),
    )
    for item in evidence_resolution.datasets
  )
  return replace(evidence_resolution, datasets=datasets)


def _target_dataset_key(target_dataset: Any) -> str:
  """
  Return the stable key for TargetDataset runtime metadata.
  """
  schema_short = str(
    getattr(
      getattr(target_dataset, "target_schema", None),
      "short_name",
      "",
    )
    or ""
  ).strip()
  dataset_name = str(
    getattr(target_dataset, "target_dataset_name", "") or ""
  ).strip()
  if not schema_short or not dataset_name:
    raise ExecutionImpactPlanError(
      "TargetDataset runtime metadata requires a target schema and dataset name."
    )
  return f"{schema_short}.{dataset_name}"


def _effective_materialization_type(target_dataset: Any) -> str:
  """
  Return the effective materialization including schema inheritance.
  """
  value = getattr(target_dataset, "effective_materialization_type", None)
  if callable(value):
    value = value()
  if not value:
    value = getattr(target_dataset, "materialization_type", None)
  if not value:
    value = getattr(
      getattr(target_dataset, "target_schema", None),
      "default_materialization_type",
      None,
    )

  normalized = str(value or "").strip().lower()
  if not normalized:
    raise ExecutionImpactPlanError(
      "TargetDataset runtime metadata has no effective materialization type: "
      f"{_target_dataset_key(target_dataset)}"
    )
  return normalized


def _target_incremental_strategy(target_dataset: Any) -> str:
  """
  Return the normalized TargetDataset load strategy used by the load planner.
  """
  normalized = str(
    getattr(target_dataset, "incremental_strategy", None) or ""
  ).strip().lower()
  if not normalized:
    raise ExecutionImpactPlanError(
      "TargetDataset runtime metadata has no incremental strategy: "
      f"{_target_dataset_key(target_dataset)}"
    )
  return normalized


def _build_local_plan(
  evidence_resolution: ExecutionImpactEvidenceResolution,
) -> ExecutionImpactPlan:
  """
  Build dataset-local decisions behind a stable service error boundary.
  """
  try:
    return build_local_execution_impact_plan(
      evidence_resolution=evidence_resolution,
    )
  except (TypeError, ValueError) as exc:
    raise ExecutionImpactPlanError(
      f"Local execution impact decision assembly failed: {exc}"
    ) from exc


def _propagate_plan(
  *,
  local_plan: ExecutionImpactPlan,
  target_datasets: tuple[Any, ...],
  dependency_resolver: Callable[[Any], Iterable[Any]] | None,
  dependency_mode: str,
) -> ExecutionImpactPlan:
  """
  Apply dependency propagation behind a stable service error boundary.
  """
  try:
    return propagate_execution_impact_plan(
      local_plan=local_plan,
      target_datasets=target_datasets,
      dependency_resolver=dependency_resolver,
      dependency_mode=dependency_mode,
    )
  except (TypeError, ValueError) as exc:
    raise ExecutionImpactPlanError(
      f"Execution impact dependency propagation failed: {exc}"
    ) from exc


def _validate_assembled_plan(
  *,
  evidence_resolution: ExecutionImpactEvidenceResolution,
  local_plan: ExecutionImpactPlan,
  final_plan: ExecutionImpactPlan,
  expected_dataset_keys: tuple[str, ...] | None,
) -> None:
  """
  Verify that assembly stages did not alter authoritative scope contracts.
  """
  expected_header = (
    evidence_resolution.scope_key,
    evidence_resolution.architecture_fingerprint,
    evidence_resolution.baseline_fingerprint,
    evidence_resolution.report_fingerprint,
  )

  for stage_name, plan in (
    ("local decision", local_plan),
    ("dependency propagation", final_plan),
  ):
    actual_header = (
      plan.scope_key,
      plan.architecture_fingerprint,
      plan.baseline_fingerprint,
      plan.report_fingerprint,
    )
    if actual_header != expected_header:
      raise ExecutionImpactPlanError(
        f"Execution Impact Plan {stage_name} stage changed an authoritative "
        "scope fingerprint."
      )

  evidence_by_key = {
    item.dataset_key: item
    for item in evidence_resolution.datasets
  }
  final_by_key = {
    item.dataset_key: item
    for item in final_plan.items
  }
  if set(final_by_key) != set(evidence_by_key):
    raise ExecutionImpactPlanError(
      "Execution Impact Plan dataset scope changed during service assembly."
    )

  if (
    expected_dataset_keys is not None
    and set(final_by_key) != set(expected_dataset_keys)
  ):
    raise ExecutionImpactPlanError(
      "Execution Impact Plan dataset scope does not match the requested "
      "execution scope."
    )

  for dataset_key, final_item in final_by_key.items():
    expected_fingerprint = evidence_by_key[dataset_key].architecture_fingerprint
    if final_item.architecture_fingerprint != expected_fingerprint:
      raise ExecutionImpactPlanError(
        "Execution Impact Plan dataset architecture fingerprint changed "
        f"during service assembly: {dataset_key}"
      )


def _normalize_execution_dataset_keys(
  dataset_keys: Iterable[str] | None,
) -> tuple[str, ...] | None:
  """
  Normalize the optional authoritative execution dataset scope.
  """
  if dataset_keys is None:
    return None

  if isinstance(dataset_keys, (str, bytes)):
    raise ExecutionImpactPlanError(
      "Execution Impact Plan dataset keys must be an iterable of keys."
    )

  normalized = tuple(sorted({
    str(dataset_key).strip()
    for dataset_key in dataset_keys
    if str(dataset_key).strip()
  }))
  if not normalized:
    raise ExecutionImpactPlanError(
      "Execution Impact Plan execution scope must contain at least one dataset."
    )
  return normalized


def _normalize_dependency_mode(value: str) -> str:
  """
  Validate the dependency mode bound to the Execution Impact Plan.
  """
  normalized = str(value or "").strip()
  if normalized not in _EXECUTION_DEPENDENCY_MODES:
    raise ExecutionImpactPlanError(
      f"Unsupported Execution Impact Plan dependency mode: {normalized}"
    )
  return normalized


def _report_payload(report: Any) -> Mapping[str, Any]:
  """
  Return the Architecture Change Report payload for input validation.
  """
  to_dict = getattr(report, "to_dict", None)
  if not callable(to_dict):
    raise ExecutionImpactPlanError(
      "Architecture Change Report must provide to_dict()."
    )

  payload = to_dict()
  if not isinstance(payload, Mapping):
    raise ExecutionImpactPlanError(
      "Architecture Change Report to_dict() must return a mapping."
    )
  return payload
