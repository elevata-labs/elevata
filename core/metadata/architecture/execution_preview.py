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
from metadata.execution.load_graph import (
  resolve_execution_order,
  resolve_execution_order_all,
)
from metadata.models import TargetDataset


ArchitectureExecutionGateStatus = Literal[
  "ready",
  "ready_no_changes",
  "pending_approval",
  "blocked_by_policy",
]


class ArchitectureExecutionPreviewError(ValueError):
  """
  Raised when an Architecture Control execution preview cannot be built.
  """


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
) -> ArchitectureExecutionPreview:
  """
  Build an Architecture Control execution preview for the selected scope.
  """
  context = control_context or build_architecture_control_context(scope)
  roots = _resolve_execution_roots(scope)
  execution_order = _resolve_execution_order(
    scope,
    roots,
    no_deps=no_deps,
  )

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
  root_dataset_keys = tuple(_dataset_key(td) for td in roots)
  gate = _build_execution_gate(context)
  dependency_mode = _dependency_mode(
    scope=scope,
    no_deps=no_deps,
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
  )


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

  if not context.report.has_changes:
    return ArchitectureExecutionGate(
      status="ready_no_changes",
      can_execute=True,
      label="Ready for controlled load",
      message=(
        "The selected scope has no architecture changes. Controlled execution can run "
        "without an approval artifact because no architecture change approval is required."
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