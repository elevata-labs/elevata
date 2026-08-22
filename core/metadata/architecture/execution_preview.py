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
import hashlib
import json
from typing import Any, Literal

from metadata.architecture.control import (
  ArchitectureControlContext,
  ArchitectureControlScope,
  build_architecture_control_context,
)
from metadata.execution.executor import build_execution_plan
from metadata.execution.load_scope import LoadScopeError, resolve_partial_load_scope
from metadata.execution.load_graph import (
  resolve_execution_order,
  resolve_execution_order_all,
)
from metadata.models import TargetDataset


ArchitectureExecutionGateStatus = Literal[
  "ready",
  "ready_no_changes",
  "ready_initial_deployment",
  "pending_approval",
  "blocked_by_policy",
  "baseline_missing",
  "impact_plan_unavailable",
  "impact_plan_blocked",
  "impact_revalidation_required",
]


class ArchitectureExecutionPreviewError(ValueError):
  """
  Raised when an Architecture Control execution preview cannot be built.
  """


@dataclass(frozen=True)
class ArchitectureExecutionScopeResolution:
  """
  Exact TargetDatasets selected by one concrete controlled execution mode.
  """
  roots: tuple[TargetDataset, ...]
  execution_order: tuple[TargetDataset, ...]
  dependency_mode: str

  @property
  def root_dataset_keys(self) -> tuple[str, ...]:
    """
    Return selected root keys in deterministic root order.
    """
    return tuple(_dataset_key(item) for item in self.roots)

  @property
  def execution_dataset_keys(self) -> tuple[str, ...]:
    """
    Return exact execution keys in execution order.
    """
    return tuple(_dataset_key(item) for item in self.execution_order)


@dataclass(frozen=True)
class ArchitectureExecutionPreviewStep:
  """
  One dataset step in an Architecture Control execution preview.
  """
  dataset_id: int
  dataset_key: str
  upstream_keys: tuple[str, ...]


@dataclass(frozen=True)
class ArchitectureExecutionGate:
  """
  Execution readiness result for an Architecture Control scope.
  """
  status: ArchitectureExecutionGateStatus
  can_execute: bool
  label: str
  message: str
  badge_class: str
  icon: str


@dataclass(frozen=True)
class ArchitectureExecutionImpactBinding:
  """
  Immutable Execution Impact Plan evidence bound to an execution preview.
  """
  plan_fingerprint: str
  assessed_count: int
  decision_counts: tuple[tuple[str, int], ...]

  def __post_init__(self) -> None:
    plan_fingerprint = str(self.plan_fingerprint or "").strip()
    if not plan_fingerprint:
      raise ValueError(
        "Execution Impact Plan binding fingerprint must not be empty."
      )

    assessed_count = int(self.assessed_count)
    if assessed_count < 0:
      raise ValueError(
        "Execution Impact Plan assessed count must not be negative."
      )

    normalized_counts: list[tuple[str, int]] = []
    seen_keys: set[str] = set()

    for raw_key, raw_count in tuple(self.decision_counts or ()):
      decision_key = str(raw_key or "").strip()
      decision_count = int(raw_count)

      if not decision_key:
        raise ValueError(
          "Execution Impact Plan decision keys must not be empty."
        )
      if decision_key in seen_keys:
        raise ValueError(
          f"Duplicate Execution Impact Plan decision key: {decision_key}"
        )
      if decision_count < 0:
        raise ValueError(
          "Execution Impact Plan decision counts must not be negative."
        )

      seen_keys.add(decision_key)
      normalized_counts.append((decision_key, decision_count))

    if sum(count for _key, count in normalized_counts) != assessed_count:
      raise ValueError(
        "Execution Impact Plan decision counts do not match assessed count."
      )

    object.__setattr__(self, "plan_fingerprint", plan_fingerprint)
    object.__setattr__(self, "assessed_count", assessed_count)
    object.__setattr__(
      self,
      "decision_counts",
      tuple(normalized_counts),
    )

  def to_dict(self) -> dict[str, Any]:
    """
    Return the canonical binding payload.
    """
    return {
      "plan_fingerprint": self.plan_fingerprint,
      "assessed_count": self.assessed_count,
      "decision_counts": dict(self.decision_counts),
    }


@dataclass(frozen=True)
class ArchitectureExecutionPreview:
  """
  Execution preview for an Architecture Control scope.
  """
  scope_key: str
  scope_label: str
  dependency_mode: str
  report_fingerprint: str
  approval_id: str | None
  review_status: str
  preview_fingerprint: str
  root_dataset_keys: tuple[str, ...]
  execution_dataset_keys: tuple[str, ...]
  steps: tuple[ArchitectureExecutionPreviewStep, ...]
  gate: ArchitectureExecutionGate
  impact_plan_binding: ArchitectureExecutionImpactBinding | None = None

  @property
  def root_count(self) -> int:
    """
    Return the number of selected root datasets.
    """
    return len(self.root_dataset_keys)

  @property
  def step_count(self) -> int:
    """
    Return the number of execution steps.
    """
    return len(self.steps)


def build_architecture_execution_preview(
  scope: ArchitectureControlScope,
  *,
  control_context: ArchitectureControlContext | None = None,
  no_deps: bool = False,
  execution_scope_resolution: ArchitectureExecutionScopeResolution | None = None,
) -> ArchitectureExecutionPreview:
  """
  Build an Architecture Control execution preview for the selected scope.
  """
  context = control_context or build_architecture_control_context(scope)
  resolved_execution_scope = (
    execution_scope_resolution
    or resolve_architecture_execution_scope(
      scope,
      no_deps=no_deps,
    )
  )
  roots = list(resolved_execution_scope.roots)
  execution_order = list(resolved_execution_scope.execution_order)

  plan = build_execution_plan(
    batch_run_id="architecture-control-preview",
    execution_order=execution_order,
  )

  steps = tuple(
    ArchitectureExecutionPreviewStep(
      dataset_id=step.dataset_id,
      dataset_key=step.dataset_key,
      upstream_keys=tuple(step.upstream_keys),
    )
    for step in plan.steps
  )

  execution_dataset_keys = tuple(step.dataset_key for step in steps)
  root_dataset_keys = resolved_execution_scope.root_dataset_keys
  if execution_dataset_keys != resolved_execution_scope.execution_dataset_keys:
    raise ArchitectureExecutionPreviewError(
      "Execution plan steps do not match the resolved execution scope."
    )

  gate = _build_execution_gate(context)
  dependency_mode = resolved_execution_scope.dependency_mode
  impact_plan_binding = build_architecture_execution_impact_binding(
    context,
    execution_dataset_keys=execution_dataset_keys,
    dependency_mode=dependency_mode,
  )

  preview_fingerprint = _stable_json_hash({
    "scope_key": context.scope.key,
    "dependency_mode": dependency_mode,
    "report_fingerprint": context.report.report_fingerprint,
    "approval_id": context.review_status.approval_id,
    "review_status": context.review_status.status,
    "root_dataset_keys": list(root_dataset_keys),
    "execution_dataset_keys": list(execution_dataset_keys),
    "gate_status": gate.status,
    "execution_impact_plan": (
      impact_plan_binding.to_dict()
      if impact_plan_binding is not None else None
    ),
  })

  return ArchitectureExecutionPreview(
    scope_key=context.scope.key,
    scope_label=context.scope.label,
    dependency_mode=dependency_mode,
    report_fingerprint=context.report.report_fingerprint,
    approval_id=context.review_status.approval_id,
    review_status=context.review_status.status,
    preview_fingerprint=preview_fingerprint,
    root_dataset_keys=root_dataset_keys,
    execution_dataset_keys=execution_dataset_keys,
    steps=steps,
    gate=gate,
    impact_plan_binding=impact_plan_binding,
  )


def resolve_architecture_execution_scope(
  scope: ArchitectureControlScope,
  *,
  no_deps: bool = False,
) -> ArchitectureExecutionScopeResolution:
  """
  Resolve the exact root and execution TargetDatasets for one execution mode.
  """
  if scope.mode == "partial_load":
    if no_deps:
      raise ArchitectureExecutionPreviewError(
        "Partial Load execution always includes required dependencies."
      )

    try:
      resolved = resolve_partial_load_scope(scope.partial_load_name or "")
    except LoadScopeError as exc:
      raise ArchitectureExecutionPreviewError(str(exc)) from exc

    return ArchitectureExecutionScopeResolution(
      roots=resolved.roots,
      execution_order=resolved.execution_order,
      dependency_mode="with_dependencies",
    )

  roots = tuple(_resolve_execution_roots(scope))
  execution_order = tuple(_resolve_execution_order(
    scope,
    list(roots),
    no_deps=no_deps,
  ))
  return ArchitectureExecutionScopeResolution(
    roots=roots,
    execution_order=execution_order,
    dependency_mode=_dependency_mode(
      scope=scope,
      no_deps=no_deps,
    ),
  )


def build_architecture_execution_impact_binding(
  context: ArchitectureControlContext,
  *,
  execution_dataset_keys: tuple[str, ...] | None = None,
  dependency_mode: str | None = None,
) -> ArchitectureExecutionImpactBinding | None:
  """
  Bind the current Execution Impact Plan to an execution artifact.
  """
  plan = getattr(context, "execution_impact_plan", None)
  if plan is None:
    return None

  expected_dataset_keys = (
    tuple(execution_dataset_keys)
    if execution_dataset_keys is not None
    else ()
  )
  if len(expected_dataset_keys) != len(set(expected_dataset_keys)):
    raise ArchitectureExecutionPreviewError(
      "Execution preview contains duplicate execution dataset keys."
    )

  plan_dataset_keys = tuple(
    item.dataset_key
    for item in getattr(plan, "items", ()) or ()
  )
  if expected_dataset_keys and (
    set(plan_dataset_keys) != set(expected_dataset_keys)
    or len(plan_dataset_keys) != len(expected_dataset_keys)
  ):
    raise ArchitectureExecutionPreviewError(
      "Execution Impact Plan dataset scope does not match the Execution Preview."
    )

  context_dataset_keys = tuple(
    getattr(context, "execution_dataset_keys", ()) or ()
  )
  if expected_dataset_keys and context_dataset_keys and (
    set(context_dataset_keys) != set(expected_dataset_keys)
    or len(context_dataset_keys) != len(expected_dataset_keys)
  ):
    raise ArchitectureExecutionPreviewError(
      "Architecture Control context dataset scope does not match the "
      "Execution Preview."
    )

  context_dependency_mode = str(
    getattr(context, "execution_dependency_mode", "") or ""
  )
  if (
    dependency_mode
    and context_dependency_mode
    and context_dependency_mode != dependency_mode
  ):
    raise ArchitectureExecutionPreviewError(
      "Execution Impact Plan dependency mode does not match the "
      "Execution Preview."
    )

  scope_key = str(getattr(plan, "scope_key", "") or "")
  if scope_key != context.scope.key:
    raise ArchitectureExecutionPreviewError(
      "Execution Impact Plan scope does not match the execution preview scope."
    )

  report_fingerprint = str(
    getattr(plan, "report_fingerprint", "") or ""
  )
  if report_fingerprint != context.report.report_fingerprint:
    raise ArchitectureExecutionPreviewError(
      "Execution Impact Plan report fingerprint does not match the "
      "execution preview report."
    )

  decision_counts = getattr(plan, "decision_counts", None)
  if not isinstance(decision_counts, dict):
    raise ArchitectureExecutionPreviewError(
      "Execution Impact Plan decision counts are unavailable."
    )

  try:
    binding = ArchitectureExecutionImpactBinding(
      plan_fingerprint=str(
        getattr(plan, "plan_fingerprint", "") or ""
      ),
      assessed_count=int(
        getattr(plan, "assessed_count", 0) or 0
      ),
      decision_counts=tuple(
        (str(key), int(value))
        for key, value in decision_counts.items()
      ),
    )
  except (TypeError, ValueError) as exc:
    raise ArchitectureExecutionPreviewError(
      f"Invalid Execution Impact Plan binding: {exc}"
    ) from exc

  if expected_dataset_keys and binding.assessed_count != len(
    expected_dataset_keys
  ):
    raise ArchitectureExecutionPreviewError(
      "Execution Impact Plan assessed count does not match the Execution Preview."
    )

  return binding


def _resolve_execution_roots(
  scope: ArchitectureControlScope,
) -> list[TargetDataset]:
  """
  Resolve the root TargetDatasets selected by an Architecture Control scope.
  """
  qs = TargetDataset.objects.select_related("target_schema").filter(active=True)

  if scope.mode == "all":
    roots = list(
      qs.order_by("target_schema__short_name", "target_dataset_name", "id")
    )
  elif scope.mode == "schema":
    if not scope.schema_short:
      raise ArchitectureExecutionPreviewError(
        "Schema scope requires a schema short name."
      )

    roots = list(
      qs
      .filter(target_schema__short_name=scope.schema_short)
      .order_by("target_schema__short_name", "target_dataset_name", "id")
    )
  elif scope.mode == "target_dataset":
    if not scope.schema_short or not scope.target_name:
      raise ArchitectureExecutionPreviewError(
        "Target dataset scope requires a schema and dataset name."
      )

    roots = list(
      qs
      .filter(
        target_schema__short_name=scope.schema_short,
        target_dataset_name=scope.target_name,
      )
      .order_by("target_schema__short_name", "target_dataset_name", "id")
    )
  else:
    raise ArchitectureExecutionPreviewError(
      f"Unsupported Architecture Control scope: {scope.mode}"
    )

  if not roots:
    raise ArchitectureExecutionPreviewError(
      "No active TargetDatasets match the selected Architecture Control scope."
    )

  return roots


def _resolve_execution_order(
  scope: ArchitectureControlScope,
  roots: list[TargetDataset],
  *,
  no_deps: bool = False,
) -> list[TargetDataset]:
  """
  Resolve the execution order for the selected Architecture Control scope.
  """
  if scope.mode == "target_dataset" and no_deps:
    return roots

  if scope.mode in {"all", "schema"}:
    execution_order = resolve_execution_order_all(roots)
  else:
    execution_order = resolve_execution_order(roots[0])

  if not execution_order:
    raise ArchitectureExecutionPreviewError(
      "The selected Architecture Control scope produced no execution steps."
    )

  return execution_order


def _dependency_mode(
  *,
  scope: ArchitectureControlScope,
  no_deps: bool,
) -> str:
  """
  Return the dependency handling mode for the execution preview.
  """
  if scope.mode == "target_dataset" and no_deps:
    return "target_only"

  return "with_dependencies"


def _build_execution_gate(
  context: ArchitectureControlContext,
) -> ArchitectureExecutionGate:
  """
  Build the execution readiness gate for an Architecture Control context.
  """
  baseline = getattr(context, "baseline_resolution", None)
  if baseline is not None and not getattr(baseline, "can_execute", False):
    return ArchitectureExecutionGate(
      status="baseline_missing",
      can_execute=False,
      label="Baseline unresolved",
      message=getattr(
        baseline,
        "message",
        "No safe architecture baseline could be resolved for this runtime context.",
      ),
      badge_class="text-bg-warning",
      icon="bi-database-exclamation",
    )

  if context.report.is_blocked:
    return ArchitectureExecutionGate(
      status="blocked_by_policy",
      can_execute=False,
      label="Blocked by policy",
      message=(
        "Execution is blocked because the Architecture Change Report contains "
        "blocking policy decisions."
      ),
      badge_class="text-bg-danger",
      icon="bi-shield-x",
    )

  impact_plan = getattr(context, "execution_impact_plan", None)
  impact_plan_error = str(
    getattr(context, "execution_impact_plan_error", "") or ""
  ).strip()

  if impact_plan is None and impact_plan_error:
    return ArchitectureExecutionGate(
      status="impact_plan_unavailable",
      can_execute=False,
      label="Execution impact unavailable",
      message=(
        "Controlled execution cannot be bound to an Execution Impact Plan. "
        f"{impact_plan_error}"
      ),
      badge_class="text-bg-warning",
      icon="bi-signpost-split",
    )

  if impact_plan is not None:
    decision_counts = (
      getattr(impact_plan, "decision_counts", None)
      or {}
    )
    blocked_count = int(decision_counts.get("BLOCKED", 0) or 0)
    revalidation_count = int(
      decision_counts.get("REVALIDATE", 0) or 0
    )

    if blocked_count:
      return ArchitectureExecutionGate(
        status="impact_plan_blocked",
        can_execute=False,
        label="Execution impact blocked",
        message=(
          "Controlled execution cannot start because the Execution Impact "
          f"Plan contains {blocked_count} blocked dataset decision(s)."
        ),
        badge_class="text-bg-danger",
        icon="bi-signpost-split-fill",
      )

    if revalidation_count:
      return ArchitectureExecutionGate(
        status="impact_revalidation_required",
        can_execute=False,
        label="Revalidation required",
        message=(
          "Controlled execution cannot start because the Execution Impact "
          f"Plan requires revalidation for {revalidation_count} dataset(s)."
        ),
        badge_class="text-bg-info",
        icon="bi-arrow-repeat",
      )

  if _is_verified_initial_deployment(context):
    return ArchitectureExecutionGate(
      status="ready_initial_deployment",
      can_execute=True,
      label="Ready for initial deployment",
      message=(
        "The complete managed target scope was physically verified as empty. "
        "Controlled execution can create the initial architecture without an "
        "Approval Artifact. The first successful finalizer will establish the "
        "recorded Architecture State."
      ),
      badge_class="text-bg-success",
      icon="bi-database-add",
    )

  if not context.report.has_changes:
    return ArchitectureExecutionGate(
      status="ready_no_changes",
      can_execute=True,
      label="Ready for controlled load",
      message=(
        "The selected scope has no architecture changes. Controlled execution can run "
        "without an approval artifact because no architecture change approval is required. "
        "Architecture Control still enforces the Architecture Guard during execution."
      ),
      badge_class="text-bg-success",
      icon="bi-play-circle",
    )

  if context.review_status.status != "approved":
    return ArchitectureExecutionGate(
      status="pending_approval",
      can_execute=False,
      label="Approval required",
      message=(
        "Execution requires a matching approval artifact for the Architecture "
        "Change Report fingerprint."
      ),
      badge_class="text-bg-warning",
      icon="bi-hourglass-split",
    )

  return ArchitectureExecutionGate(
    status="ready",
    can_execute=True,
    label="Ready for execution",
    message=(
      "The selected scope has a matching approval artifact and no blocking "
      "policy decisions."
    ),
    badge_class="text-bg-success",
    icon="bi-play-circle",
  )


def _is_verified_initial_deployment(
  context: ArchitectureControlContext,
) -> bool:
  """
  Return True only for a full-scope, physically verified initial deployment.
  """
  baseline = getattr(context, "baseline_resolution", None)
  return bool(
    getattr(getattr(context, "scope", None), "mode", None) == "all"
    and getattr(
      getattr(context, "review_status", None),
      "status",
      None,
    ) == "initial_deployment"
    and getattr(baseline, "is_initial_deployment", False)
  )


def _dataset_key(target_dataset: Any) -> str:
  """
  Return the stable dataset key for a TargetDataset-shaped object.
  """
  schema_short = getattr(
    getattr(target_dataset, "target_schema", None),
    "short_name",
    None,
  )
  target_name = getattr(target_dataset, "target_dataset_name", None)

  if not schema_short or not target_name:
    raise ArchitectureExecutionPreviewError(
      "TargetDataset must have a target schema and dataset name."
    )

  return f"{schema_short}.{target_name}"


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