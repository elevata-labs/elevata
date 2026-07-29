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

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Literal

from metadata.architecture.execution_impact import (
  ExecutionImpactExecutableDecision,
  ExecutionImpactSelection,
)
from metadata.architecture.paths import (
  ArchitectureArtifactContext,
  resolve_architecture_run_plan_dir,
)


EXECUTION_RUN_PLAN_ARTIFACT_TYPE = "execution_run_plan"
EXECUTION_RUN_PLAN_ARTIFACT_VERSION = 1

ExecutionRunPlanScopeMode = Literal[
  "target_dataset",
  "schema",
  "all",
]
ExecutionRunPlanDependencyMode = Literal[
  "with_dependencies",
  "target_only",
]

_EXECUTABLE_DECISION_ORDER: tuple[
  ExecutionImpactExecutableDecision,
  ...,
] = (
  "REUSE",
  "INCREMENTAL_EXECUTE",
  "FULL_REBUILD",
)
_ALLOWED_SCOPE_MODES = frozenset({
  "target_dataset",
  "schema",
  "all",
})
_ALLOWED_DEPENDENCY_MODES = frozenset({
  "with_dependencies",
  "target_only",
})
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")

_PAYLOAD_KEYS = frozenset({
  "artifact_type",
  "artifact_version",
  "run_plan_id",
  "batch_run_id",
  "created_at",
  "profile_name",
  "target_system_short",
  "scope_mode",
  "scope_key",
  "scope_label",
  "dependency_mode",
  "review_status",
  "approval_id",
  "architecture_fingerprint",
  "report_fingerprint",
  "impact_plan_fingerprint",
  "preview_fingerprint",
  "execution_plan_fingerprint",
  "root_dataset_keys",
  "dataset_count",
  "decision_counts",
  "dataset_decisions",
  "run_plan_fingerprint",
})
_DATASET_DECISION_KEYS = frozenset({
  "dataset_key",
  "decision",
})


@dataclass(frozen=True)
class ExecutionRunPlan:
  """
  Immutable public execution contract for one controlled scheduler run.

  The run plan binds one exact ordered dataset selection to architecture,
  review, impact and execution evidence. It contains only executable impact
  decisions. BLOCKED and REVALIDATE plans cannot become scheduler run plans.
  """
  run_plan_id: str
  batch_run_id: str
  created_at: str
  profile_name: str
  target_system_short: str
  scope_mode: ExecutionRunPlanScopeMode
  scope_key: str
  scope_label: str
  dependency_mode: ExecutionRunPlanDependencyMode
  review_status: str
  approval_id: str | None
  architecture_fingerprint: str
  report_fingerprint: str
  impact_plan_fingerprint: str
  preview_fingerprint: str
  execution_plan_fingerprint: str
  root_dataset_keys: tuple[str, ...]
  dataset_decisions: tuple[
    tuple[str, ExecutionImpactExecutableDecision],
    ...,
  ]

  def __post_init__(self) -> None:
    run_plan_id = _required_text(
      self.run_plan_id,
      label="Execution Run Plan id",
    )
    batch_run_id = _required_text(
      self.batch_run_id,
      label="Execution Run Plan batch run id",
    )
    created_at = _normalize_utc_timestamp(self.created_at)
    profile_name = _required_text(
      self.profile_name,
      label="Execution Run Plan profile",
    )
    target_system_short = _required_text(
      self.target_system_short,
      label="Execution Run Plan target system",
    )

    scope_mode = str(self.scope_mode or "").strip()
    if scope_mode not in _ALLOWED_SCOPE_MODES:
      raise ValueError(
        f"Unsupported Execution Run Plan scope mode: {scope_mode}"
      )

    scope_key = _required_text(
      self.scope_key,
      label="Execution Run Plan scope key",
    )
    scope_label = _required_text(
      self.scope_label,
      label="Execution Run Plan scope label",
    )

    dependency_mode = str(self.dependency_mode or "").strip()
    if dependency_mode not in _ALLOWED_DEPENDENCY_MODES:
      raise ValueError(
        "Unsupported Execution Run Plan dependency mode: "
        f"{dependency_mode}"
      )

    review_status = _required_text(
      self.review_status,
      label="Execution Run Plan review status",
    )
    approval_id = _optional_text(self.approval_id)
    if review_status == "approved":
      if approval_id is None:
        raise ValueError(
          "Approved Execution Run Plans require an approval id."
        )
    else:
      # Only an approved review status establishes an approval binding for
      # this concrete scheduler run. A stale approval artifact may still be
      # visible in the broader Architecture Control context, but it must not
      # be represented as evidence that authorized this run plan.
      approval_id = None

    architecture_fingerprint = _fingerprint(
      self.architecture_fingerprint,
      label="architecture",
    )
    report_fingerprint = _fingerprint(
      self.report_fingerprint,
      label="report",
    )
    impact_plan_fingerprint = _fingerprint(
      self.impact_plan_fingerprint,
      label="impact plan",
    )
    preview_fingerprint = _fingerprint(
      self.preview_fingerprint,
      label="preview",
    )
    execution_plan_fingerprint = _fingerprint(
      self.execution_plan_fingerprint,
      label="execution plan",
    )

    selection = ExecutionImpactSelection(
      plan_fingerprint=impact_plan_fingerprint,
      dataset_decisions=tuple(self.dataset_decisions or ()),
    )
    dataset_decisions = selection.dataset_decisions
    dataset_keys = selection.dataset_keys
    dataset_key_set = set(dataset_keys)

    root_dataset_keys = tuple(
      _required_text(
        dataset_key,
        label="Execution Run Plan root dataset key",
      )
      for dataset_key in tuple(self.root_dataset_keys or ())
    )
    if not root_dataset_keys:
      raise ValueError(
        "Execution Run Plan requires at least one root dataset key."
      )
    if len(root_dataset_keys) != len(set(root_dataset_keys)):
      raise ValueError(
        "Execution Run Plan contains duplicate root dataset keys."
      )

    unknown_roots = tuple(
      dataset_key
      for dataset_key in root_dataset_keys
      if dataset_key not in dataset_key_set
    )
    if unknown_roots:
      raise ValueError(
        "Execution Run Plan root datasets are outside the execution scope: "
        + ", ".join(unknown_roots)
        + "."
      )

    if scope_mode == "target_dataset" and len(root_dataset_keys) != 1:
      raise ValueError(
        "Target-dataset Execution Run Plans require exactly one root dataset."
      )

    if dependency_mode == "target_only":
      if scope_mode != "target_dataset":
        raise ValueError(
          "Target-only Execution Run Plans require target-dataset scope."
        )
      if dataset_keys != root_dataset_keys:
        raise ValueError(
          "Target-only Execution Run Plans must contain exactly their "
          "selected root dataset."
        )

    object.__setattr__(self, "run_plan_id", run_plan_id)
    object.__setattr__(self, "batch_run_id", batch_run_id)
    object.__setattr__(self, "created_at", created_at)
    object.__setattr__(self, "profile_name", profile_name)
    object.__setattr__(
      self,
      "target_system_short",
      target_system_short,
    )
    object.__setattr__(self, "scope_mode", scope_mode)
    object.__setattr__(self, "scope_key", scope_key)
    object.__setattr__(self, "scope_label", scope_label)
    object.__setattr__(self, "dependency_mode", dependency_mode)
    object.__setattr__(self, "review_status", review_status)
    object.__setattr__(self, "approval_id", approval_id)
    object.__setattr__(
      self,
      "architecture_fingerprint",
      architecture_fingerprint,
    )
    object.__setattr__(
      self,
      "report_fingerprint",
      report_fingerprint,
    )
    object.__setattr__(
      self,
      "impact_plan_fingerprint",
      impact_plan_fingerprint,
    )
    object.__setattr__(
      self,
      "preview_fingerprint",
      preview_fingerprint,
    )
    object.__setattr__(
      self,
      "execution_plan_fingerprint",
      execution_plan_fingerprint,
    )
    object.__setattr__(
      self,
      "root_dataset_keys",
      root_dataset_keys,
    )
    object.__setattr__(
      self,
      "dataset_decisions",
      dataset_decisions,
    )

  @property
  def dataset_keys(self) -> tuple[str, ...]:
    """
    Return dataset keys in exact scheduler execution order.
    """
    return tuple(
      dataset_key
      for dataset_key, _decision in self.dataset_decisions
    )

  @property
  def dataset_count(self) -> int:
    """
    Return the number of dataset decisions in the run plan.
    """
    return len(self.dataset_decisions)

  @property
  def decision_counts(
    self,
  ) -> dict[ExecutionImpactExecutableDecision, int]:
    """
    Return stable counts for every executable impact decision.
    """
    return {
      decision: sum(
        1
        for _dataset_key, item_decision in self.dataset_decisions
        if item_decision == decision
      )
      for decision in _EXECUTABLE_DECISION_ORDER
    }

  @property
  def selection(self) -> ExecutionImpactSelection:
    """
    Return the existing in-process impact selection represented by the plan.
    """
    return ExecutionImpactSelection(
      plan_fingerprint=self.impact_plan_fingerprint,
      dataset_decisions=self.dataset_decisions,
    )

  @property
  def run_plan_fingerprint(self) -> str:
    """
    Return the deterministic fingerprint of the complete run plan.
    """
    return _stable_json_hash(
      self.to_dict(include_fingerprint=False)
    )

  def decision_for_dataset(
    self,
    dataset_key: str,
  ) -> ExecutionImpactExecutableDecision:
    """
    Return the bound decision for one dataset or raise KeyError.
    """
    normalized_key = _required_text(
      dataset_key,
      label="Execution Run Plan dataset lookup key",
    )
    for item_key, decision in self.dataset_decisions:
      if item_key == normalized_key:
        return decision

    raise KeyError(
      f"Dataset is not part of the Execution Run Plan: {normalized_key}"
    )

  def to_dict(
    self,
    *,
    include_fingerprint: bool = True,
  ) -> dict[str, Any]:
    """
    Return the canonical public Execution Run Plan payload.
    """
    payload = {
      "artifact_type": EXECUTION_RUN_PLAN_ARTIFACT_TYPE,
      "artifact_version": EXECUTION_RUN_PLAN_ARTIFACT_VERSION,
      "run_plan_id": self.run_plan_id,
      "batch_run_id": self.batch_run_id,
      "created_at": self.created_at,
      "profile_name": self.profile_name,
      "target_system_short": self.target_system_short,
      "scope_mode": self.scope_mode,
      "scope_key": self.scope_key,
      "scope_label": self.scope_label,
      "dependency_mode": self.dependency_mode,
      "review_status": self.review_status,
      "approval_id": self.approval_id,
      "architecture_fingerprint": self.architecture_fingerprint,
      "report_fingerprint": self.report_fingerprint,
      "impact_plan_fingerprint": self.impact_plan_fingerprint,
      "preview_fingerprint": self.preview_fingerprint,
      "execution_plan_fingerprint": self.execution_plan_fingerprint,
      "root_dataset_keys": list(self.root_dataset_keys),
      "dataset_count": self.dataset_count,
      "decision_counts": self.decision_counts,
      "dataset_decisions": [
        {
          "dataset_key": dataset_key,
          "decision": decision,
        }
        for dataset_key, decision in self.dataset_decisions
      ],
    }

    if include_fingerprint:
      payload["run_plan_fingerprint"] = self.run_plan_fingerprint

    return payload


def build_execution_run_plan(
  *,
  run_plan_id: str,
  batch_run_id: str,
  created_at: str,
  profile_name: str,
  target_system_short: str,
  scope_mode: ExecutionRunPlanScopeMode,
  scope_key: str,
  scope_label: str,
  dependency_mode: ExecutionRunPlanDependencyMode,
  review_status: str,
  approval_id: str | None,
  architecture_fingerprint: str,
  report_fingerprint: str,
  preview_fingerprint: str,
  execution_plan_fingerprint: str,
  root_dataset_keys: tuple[str, ...],
  impact_selection: ExecutionImpactSelection,
) -> ExecutionRunPlan:
  """
  Build one public run plan from an executable impact selection.
  """
  if not isinstance(impact_selection, ExecutionImpactSelection):
    raise ValueError(
      "Execution Run Plan creation requires an Execution Impact Selection."
    )

  return ExecutionRunPlan(
    run_plan_id=run_plan_id,
    batch_run_id=batch_run_id,
    created_at=created_at,
    profile_name=profile_name,
    target_system_short=target_system_short,
    scope_mode=scope_mode,
    scope_key=scope_key,
    scope_label=scope_label,
    dependency_mode=dependency_mode,
    review_status=review_status,
    approval_id=approval_id,
    architecture_fingerprint=architecture_fingerprint,
    report_fingerprint=report_fingerprint,
    impact_plan_fingerprint=impact_selection.plan_fingerprint,
    preview_fingerprint=preview_fingerprint,
    execution_plan_fingerprint=execution_plan_fingerprint,
    root_dataset_keys=root_dataset_keys,
    dataset_decisions=impact_selection.dataset_decisions,
  )


def build_execution_plan_fingerprint(
  target_datasets: Sequence[Any],
) -> str:
  """
  Return the stable metadata fingerprint for one planned execution set.

  The payload intentionally matches the existing load-runner plan fingerprint:
  dataset identity, incremental strategy, materialization override and the
  best available metadata change timestamp are bound into one SHA-256 value.

  Dataset order is represented separately by the immutable run-plan decisions,
  so the execution-set fingerprint remains order-independent.
  """
  datasets = tuple(target_datasets or ())
  if not datasets:
    raise ValueError(
      "Execution plan fingerprint requires at least one TargetDataset."
    )

  parts: list[str] = []
  for target_dataset in datasets:
    schema_short = str(
      getattr(
        getattr(target_dataset, "target_schema", None),
        "short_name",
        "<?>",
      )
    )
    dataset_name = str(
      getattr(
        target_dataset,
        "target_dataset_name",
        "<?>",
      )
    )
    incremental_strategy = str(
      getattr(
        target_dataset,
        "incremental_strategy",
        "",
      )
      or ""
    )
    materialization_type = str(
      getattr(
        target_dataset,
        "materialization_type",
        "",
      )
      or ""
    )
    changed_at = _resolve_target_dataset_timestamp(
      target_dataset
    )

    parts.append(
      f"{schema_short}.{dataset_name}"
      f"|{incremental_strategy}"
      f"|{materialization_type}"
      f"|{changed_at}"
    )

  payload = "\n".join(sorted(parts)).encode("utf-8")
  return hashlib.sha256(payload).hexdigest()


def execution_run_plan_from_dict(
  payload: Mapping[str, Any],
) -> ExecutionRunPlan:
  """
  Parse and validate one versioned public Execution Run Plan payload.
  """
  if not isinstance(payload, Mapping):
    raise ValueError(
      "Execution Run Plan payload must be a JSON object."
    )

  if payload.get("artifact_type") != EXECUTION_RUN_PLAN_ARTIFACT_TYPE:
    raise ValueError(
      "Unsupported Execution Run Plan artifact type: "
      f"{payload.get('artifact_type')}"
    )

  if (
    payload.get("artifact_version")
    != EXECUTION_RUN_PLAN_ARTIFACT_VERSION
  ):
    raise ValueError(
      "Unsupported Execution Run Plan artifact version: "
      f"{payload.get('artifact_version')}"
    )

  _require_exact_keys(
    payload,
    expected=_PAYLOAD_KEYS,
    label="Execution Run Plan payload",
  )

  raw_dataset_decisions = _require_sequence(
    payload.get("dataset_decisions"),
    label="dataset decisions",
  )
  dataset_decisions: list[
    tuple[str, ExecutionImpactExecutableDecision]
  ] = []

  for raw_item in raw_dataset_decisions:
    item = _require_mapping(
      raw_item,
      label="dataset decision",
    )
    _require_exact_keys(
      item,
      expected=_DATASET_DECISION_KEYS,
      label="Execution Run Plan dataset decision",
    )
    dataset_decisions.append((
      _required_text(
        item.get("dataset_key"),
        label="Execution Run Plan dataset key",
      ),
      str(item.get("decision") or "").strip(),
    ))

  raw_root_dataset_keys = _require_sequence(
    payload.get("root_dataset_keys"),
    label="root dataset keys",
  )

  plan = ExecutionRunPlan(
    run_plan_id=payload.get("run_plan_id"),
    batch_run_id=payload.get("batch_run_id"),
    created_at=payload.get("created_at"),
    profile_name=payload.get("profile_name"),
    target_system_short=payload.get("target_system_short"),
    scope_mode=payload.get("scope_mode"),
    scope_key=payload.get("scope_key"),
    scope_label=payload.get("scope_label"),
    dependency_mode=payload.get("dependency_mode"),
    review_status=payload.get("review_status"),
    approval_id=payload.get("approval_id"),
    architecture_fingerprint=payload.get(
      "architecture_fingerprint"
    ),
    report_fingerprint=payload.get("report_fingerprint"),
    impact_plan_fingerprint=payload.get(
      "impact_plan_fingerprint"
    ),
    preview_fingerprint=payload.get("preview_fingerprint"),
    execution_plan_fingerprint=payload.get(
      "execution_plan_fingerprint"
    ),
    root_dataset_keys=tuple(
      _required_text(
        dataset_key,
        label="Execution Run Plan root dataset key",
      )
      for dataset_key in raw_root_dataset_keys
    ),
    dataset_decisions=tuple(dataset_decisions),
  )

  dataset_count = _require_non_negative_int(
    payload.get("dataset_count"),
    label="dataset count",
  )
  if dataset_count != plan.dataset_count:
    raise ValueError(
      "Execution Run Plan dataset count does not match its decisions."
    )

  raw_decision_counts = _require_mapping(
    payload.get("decision_counts"),
    label="decision counts",
  )
  _require_exact_keys(
    raw_decision_counts,
    expected=frozenset(_EXECUTABLE_DECISION_ORDER),
    label="Execution Run Plan decision counts",
  )
  decision_counts = {
    decision: _require_non_negative_int(
      raw_decision_counts.get(decision),
      label=f"{decision} decision count",
    )
    for decision in _EXECUTABLE_DECISION_ORDER
  }
  if decision_counts != plan.decision_counts:
    raise ValueError(
      "Execution Run Plan decision counts do not match its decisions."
    )

  expected_fingerprint = _fingerprint(
    payload.get("run_plan_fingerprint"),
    label="run plan",
  )
  if expected_fingerprint != plan.run_plan_fingerprint:
    raise ValueError(
      "Execution Run Plan fingerprint does not match its canonical payload."
    )

  return plan


def parse_execution_run_plan_json(value: str) -> ExecutionRunPlan:
  """
  Parse and validate one Execution Run Plan JSON document.
  """
  try:
    payload = json.loads(str(value))
  except (TypeError, ValueError) as exc:
    raise ValueError(
      "Execution Run Plan JSON is invalid."
    ) from exc

  return execution_run_plan_from_dict(payload)


def render_execution_run_plan_json(plan: ExecutionRunPlan) -> str:
  """
  Render one Execution Run Plan as canonical human-readable JSON.
  """
  if not isinstance(plan, ExecutionRunPlan):
    raise ValueError(
      "Execution Run Plan rendering requires a validated run plan."
    )

  return json.dumps(
    plan.to_dict(),
    sort_keys=True,
    ensure_ascii=False,
    indent=2,
  ) + "\n"


class ExecutionRunPlanStore:
  """
  Immutable file-backed store for public Execution Run Plans.

  A run-plan identifier may be written repeatedly only when the canonical
  fingerprint is unchanged. Reusing an identifier for another plan fails
  closed instead of silently replacing scheduler evidence.
  """

  def __init__(
    self,
    base_path: str | Path | None = None,
    *,
    context: ArchitectureArtifactContext | None = None,
  ):
    self.base_path = (
      Path(base_path).expanduser()
      if base_path is not None
      else resolve_architecture_run_plan_dir(context=context)
    )

  def path_for(self, run_plan_id: str) -> Path:
    """
    Return the canonical path for one run-plan identifier.
    """
    safe_run_plan_id = "".join(
      character
      for character in str(run_plan_id or "")
      if character.isalnum() or character in {"-", "_"}
    )
    if not safe_run_plan_id:
      raise ValueError(
        "Execution Run Plan id does not contain a safe file name."
      )

    return self.base_path / f"{safe_run_plan_id}.run_plan.json"

  def save(
    self,
    plan: ExecutionRunPlan,
    *,
    output_path: str | Path | None = None,
  ) -> Path:
    """
    Store one immutable Execution Run Plan and return its path.
    """
    if not isinstance(plan, ExecutionRunPlan):
      raise ValueError(
        "Execution Run Plan storage requires a validated run plan."
      )

    path = (
      Path(output_path).expanduser()
      if output_path is not None
      else self.path_for(plan.run_plan_id)
    )

    if path.exists():
      existing_plan = self.load_path(path)
      if (
        existing_plan.run_plan_fingerprint
        == plan.run_plan_fingerprint
      ):
        return path

      raise ValueError(
        "Execution Run Plan path already contains another immutable "
        f"plan: {path}"
      )

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")

    try:
      temporary_path.write_text(
        render_execution_run_plan_json(plan),
        encoding="utf-8",
      )
      temporary_path.replace(path)
    finally:
      if temporary_path.exists():
        temporary_path.unlink()

    return path

  def load(self, run_plan_id: str) -> ExecutionRunPlan:
    """
    Load and validate one stored Execution Run Plan by identifier.
    """
    return self.load_path(self.path_for(run_plan_id))

  def load_path(
    self,
    path: str | Path,
  ) -> ExecutionRunPlan:
    """
    Load and validate one Execution Run Plan from an explicit path.
    """
    run_plan_path = Path(path).expanduser()

    try:
      value = run_plan_path.read_text(encoding="utf-8")
    except OSError as exc:
      raise ValueError(
        f"Execution Run Plan cannot be read: {run_plan_path}"
      ) from exc

    try:
      return parse_execution_run_plan_json(value)
    except ValueError as exc:
      raise ValueError(
        f"Execution Run Plan is invalid: {run_plan_path}: {exc}"
      ) from exc

  def iter_plan_paths(self) -> tuple[Path, ...]:
    """
    Return stored run-plan paths ordered by file name.
    """
    if not self.base_path.exists():
      return ()

    return tuple(
      sorted(self.base_path.glob("*.run_plan.json"))
    )


def _required_text(
  value: Any,
  *,
  label: str,
) -> str:
  """
  Normalize one required non-empty string.
  """
  normalized = str(value or "").strip()
  if not normalized:
    raise ValueError(f"{label} must not be empty.")
  return normalized


def _optional_text(value: Any) -> str | None:
  """
  Normalize one optional string.
  """
  normalized = str(value or "").strip()
  return normalized or None


def _normalize_utc_timestamp(value: Any) -> str:
  """
  Normalize one timezone-aware ISO timestamp to canonical UTC.
  """
  raw_value = _required_text(
    value,
    label="Execution Run Plan created_at",
  )

  try:
    parsed = datetime.fromisoformat(
      raw_value.replace("Z", "+00:00")
    )
  except ValueError as exc:
    raise ValueError(
      "Execution Run Plan created_at must be a valid ISO timestamp."
    ) from exc

  if parsed.tzinfo is None or parsed.utcoffset() is None:
    raise ValueError(
      "Execution Run Plan created_at must include a timezone."
    )

  return parsed.astimezone(timezone.utc).isoformat()


def _fingerprint(
  value: Any,
  *,
  label: str,
) -> str:
  """
  Normalize and validate one SHA-256 fingerprint.
  """
  normalized = _required_text(
    value,
    label=f"Execution Run Plan {label} fingerprint",
  )
  if not _SHA256_RE.fullmatch(normalized):
    raise ValueError(
      f"Execution Run Plan {label} fingerprint must be SHA-256."
    )
  return normalized.lower()


def _require_mapping(
  value: Any,
  *,
  label: str,
) -> Mapping[str, Any]:
  """
  Return one required JSON-object-shaped value.
  """
  if not isinstance(value, Mapping):
    raise ValueError(
      f"Execution Run Plan {label} must be a JSON object."
    )
  return value


def _require_sequence(
  value: Any,
  *,
  label: str,
) -> Sequence[Any]:
  """
  Return one required JSON-array-shaped value.
  """
  if (
    not isinstance(value, Sequence)
    or isinstance(value, (str, bytes, bytearray))
  ):
    raise ValueError(
      f"Execution Run Plan {label} must be a JSON array."
    )
  return value


def _require_exact_keys(
  value: Mapping[str, Any],
  *,
  expected: frozenset[str],
  label: str,
) -> None:
  """
  Enforce the exact field set for one versioned contract object.
  """
  actual = frozenset(str(key) for key in value.keys())
  if actual == expected:
    return

  missing = tuple(sorted(expected - actual))
  unexpected = tuple(sorted(actual - expected))
  details: list[str] = []

  if missing:
    details.append("missing: " + ", ".join(missing))
  if unexpected:
    details.append("unexpected: " + ", ".join(unexpected))

  raise ValueError(
    f"{label} fields do not match artifact version "
    f"{EXECUTION_RUN_PLAN_ARTIFACT_VERSION}; "
    + "; ".join(details)
    + "."
  )


def _require_non_negative_int(
  value: Any,
  *,
  label: str,
) -> int:
  """
  Return one strict non-negative integer.
  """
  if isinstance(value, bool) or not isinstance(value, int):
    raise ValueError(
      f"Execution Run Plan {label} must be an integer."
    )
  if value < 0:
    raise ValueError(
      f"Execution Run Plan {label} must not be negative."
    )
  return value


def _resolve_target_dataset_timestamp(
  target_dataset: Any,
) -> str:
  """
  Return the best available TargetDataset metadata change marker.
  """
  for attribute_name in (
    "updated_at",
    "modified_at",
    "last_modified",
    "changed_at",
    "created_at",
  ):
    value = getattr(
      target_dataset,
      attribute_name,
      None,
    )
    if value is None:
      continue

    try:
      return value.isoformat()
    except Exception:
      return str(value)

  return "<?>"


def _stable_json_hash(value: Any) -> str:
  """
  Return a deterministic SHA-256 hash for a JSON-serializable value.
  """
  payload = json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
  )
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()
