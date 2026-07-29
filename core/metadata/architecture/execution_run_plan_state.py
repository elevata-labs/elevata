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
from pathlib import Path
from typing import Any

from metadata.architecture.diff import (
  ArchitectureDiff,
  diff_architecture_states,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
  ExecutionRunPlanStore,
)
from metadata.architecture.state import (
  ArchitectureState,
  DatasetState,
)
from metadata.architecture.store import ArchitectureStateStore


@dataclass(frozen=True)
class ExecutionRunPlanArchitectureDrift:
  """
  Describe metadata drift detected after an immutable Run Plan was created.
  """
  status: str
  planned_fingerprint: str
  current_fingerprint: str | None
  dataset_changes: tuple[str, ...] = ()
  column_changes: tuple[str, ...] = ()
  message: str = ""

  @property
  def has_changes(self) -> bool:
    """
    Return whether current metadata differs from the planned architecture.
    """
    return self.status == "changed"

  def to_dict(self) -> dict[str, Any]:
    """
    Return a JSON-compatible operator-facing drift description.
    """
    return {
      "status": self.status,
      "planned_fingerprint": self.planned_fingerprint,
      "current_fingerprint": self.current_fingerprint,
      "dataset_changes": list(self.dataset_changes),
      "column_changes": list(self.column_changes),
      "message": self.message,
    }


def execution_run_plan_planned_state_path(
  run_plan_path: str | Path,
) -> Path:
  """
  Return the deterministic planned Architecture State path for one Run Plan.
  """
  path = Path(run_plan_path).expanduser()
  return path.with_name(
    f"{path.stem}.planned_architecture_state.json"
  )


def validate_execution_run_plan_planned_state(
  *,
  plan: ExecutionRunPlan,
  planned_state: ArchitectureState,
) -> ArchitectureState:
  """
  Validate the immutable Architecture State bound to one Execution Run Plan.
  """
  if not isinstance(plan, ExecutionRunPlan):
    raise ValueError(
      "Invalid Execution Run Plan planned-state contract."
    )
  if not isinstance(planned_state, ArchitectureState):
    raise ValueError(
      "Execution Run Plan requires a planned Architecture State."
    )

  if planned_state.fingerprint != plan.architecture_fingerprint:
    raise ValueError(
      "Execution Run Plan planned Architecture State fingerprint does not "
      "match the immutable plan: "
      f"planned={plan.architecture_fingerprint} "
      f"state={planned_state.fingerprint}."
    )

  missing_dataset_keys = tuple(
    dataset_key
    for dataset_key in plan.dataset_keys
    if dataset_key not in planned_state.datasets_by_key
  )
  if missing_dataset_keys:
    raise ValueError(
      "Execution Run Plan planned Architecture State is missing datasets: "
      + ", ".join(missing_dataset_keys)
      + "."
    )

  return planned_state


def write_execution_run_plan_planned_state(
  *,
  run_plan_path: str | Path,
  plan: ExecutionRunPlan,
  planned_state: ArchitectureState,
) -> Path:
  """
  Persist the immutable planned Architecture State beside one Run Plan.
  """
  validated_state = validate_execution_run_plan_planned_state(
    plan=plan,
    planned_state=planned_state,
  )
  path = execution_run_plan_planned_state_path(
    run_plan_path
  )

  if path.exists():
    existing = load_execution_run_plan_planned_state(
      run_plan_path=run_plan_path,
      plan=plan,
    )
    if existing.fingerprint != validated_state.fingerprint:
      raise ValueError(
        "Execution Run Plan planned Architecture State already exists with "
        "another fingerprint."
      )
    return path

  ArchitectureStateStore.save_file(
    path,
    validated_state,
  )
  recorded = load_execution_run_plan_planned_state(
    run_plan_path=run_plan_path,
    plan=plan,
  )
  if recorded.fingerprint != validated_state.fingerprint:
    raise ValueError(
      "Persisted Execution Run Plan planned Architecture State does not "
      "match the supplied state."
    )

  return path


def load_execution_run_plan_planned_state(
  *,
  run_plan_path: str | Path,
  plan: ExecutionRunPlan,
) -> ArchitectureState:
  """
  Load and validate the planned Architecture State bound to one Run Plan.
  """
  path = execution_run_plan_planned_state_path(
    run_plan_path
  )
  state = ArchitectureStateStore.load_file(path)
  if state is None:
    raise ValueError(
      "Execution Run Plan planned Architecture State is missing or invalid: "
      f"{path}. Start a new DAG run."
    )

  return validate_execution_run_plan_planned_state(
    plan=plan,
    planned_state=state,
  )


def save_execution_run_plan_bundle(
  *,
  store: ExecutionRunPlanStore,
  plan: ExecutionRunPlan,
  planned_state: ArchitectureState,
  output_path: str | Path | None = None,
) -> tuple[Path, Path]:
  """
  Persist one immutable Run Plan together with its planned Architecture State.
  """
  path = (
    Path(output_path).expanduser()
    if output_path is not None
    else store.path_for(plan.run_plan_id)
  )

  if path.exists():
    existing_plan = store.load_path(path)
    if existing_plan.run_plan_fingerprint != plan.run_plan_fingerprint:
      raise ValueError(
        "Execution Run Plan path already contains another immutable "
        f"plan: {path}"
      )

    state_path = write_execution_run_plan_planned_state(
      run_plan_path=path,
      plan=existing_plan,
      planned_state=planned_state,
    )
    return path, state_path

  state_path = execution_run_plan_planned_state_path(path)
  state_existed = state_path.exists()
  write_execution_run_plan_planned_state(
    run_plan_path=path,
    plan=plan,
    planned_state=planned_state,
  )

  try:
    stored_path = store.save(
      plan,
      output_path=path,
    )
  except Exception:
    if not state_existed and not path.exists():
      try:
        state_path.unlink()
      except OSError:
        pass
    raise

  return stored_path, state_path


def validate_execution_run_plan_dataset_state(
  *,
  plan: ExecutionRunPlan,
  planned_state: ArchitectureState,
  dataset_key: str,
  current_dataset_state: DatasetState,
) -> DatasetState:
  """
  Verify one scheduler step still matches its planned dataset contract.
  """
  validate_execution_run_plan_planned_state(
    plan=plan,
    planned_state=planned_state,
  )

  normalized_dataset_key = str(dataset_key or "").strip()
  if normalized_dataset_key not in plan.dataset_keys:
    raise ValueError(
      "Dataset is not part of the Execution Run Plan: "
      f"{normalized_dataset_key or '<missing>'}."
    )

  planned_dataset = planned_state.datasets_by_key.get(
    normalized_dataset_key
  )
  if planned_dataset is None:
    raise ValueError(
      "Execution Run Plan planned Architecture State is missing dataset: "
      f"{normalized_dataset_key}."
    )

  if not isinstance(current_dataset_state, DatasetState):
    raise ValueError(
      "Execution Run Plan current dataset state is invalid: "
      f"{normalized_dataset_key}."
    )
  if current_dataset_state.dataset_key != normalized_dataset_key:
    raise ValueError(
      "Execution Run Plan current dataset state belongs to another dataset: "
      f"{current_dataset_state.dataset_key}."
    )

  if current_dataset_state.fingerprint != planned_dataset.fingerprint:
    diff = diff_architecture_states(
      ArchitectureState(datasets=(planned_dataset,)),
      ArchitectureState(datasets=(current_dataset_state,)),
    )
    details = _architecture_diff_details(diff)
    detail_text = (
      "; ".join(details)
      if details
      else "semantic dataset fingerprint changed"
    )
    raise ValueError(
      "Execution Run Plan was superseded by metadata changes for "
      f"{normalized_dataset_key}. Start a new DAG run. "
      f"Changes: {detail_text}."
    )

  return planned_dataset


def build_execution_run_plan_architecture_drift(
  *,
  planned_state: ArchitectureState,
  current_state: ArchitectureState,
) -> ExecutionRunPlanArchitectureDrift:
  """
  Compare the applied planned state with metadata visible at finalization.
  """
  if not isinstance(planned_state, ArchitectureState):
    raise ValueError("Planned Architecture State is invalid.")
  if not isinstance(current_state, ArchitectureState):
    raise ValueError("Current Architecture State is invalid.")

  diff = diff_architecture_states(
    planned_state,
    current_state,
  )
  if not diff.has_changes():
    return ExecutionRunPlanArchitectureDrift(
      status="unchanged",
      planned_fingerprint=planned_state.fingerprint,
      current_fingerprint=current_state.fingerprint,
      message=(
        "Current metadata still matches the finalized planned architecture."
      ),
    )

  dataset_changes = tuple(
    _dataset_change_text(change)
    for change in diff.dataset_changes
  )
  column_changes = tuple(
    _column_change_text(change)
    for change in diff.column_changes
  )
  return ExecutionRunPlanArchitectureDrift(
    status="changed",
    planned_fingerprint=planned_state.fingerprint,
    current_fingerprint=current_state.fingerprint,
    dataset_changes=dataset_changes,
    column_changes=column_changes,
    message=(
      "Metadata changed after the Execution Run Plan was created. The planned "
      "state was finalized; a new DAG run must evaluate the remaining drift."
    ),
  )


def unavailable_execution_run_plan_architecture_drift(
  *,
  planned_state: ArchitectureState,
  error: object,
) -> ExecutionRunPlanArchitectureDrift:
  """
  Return a non-blocking drift result when current metadata cannot be rebuilt.
  """
  return ExecutionRunPlanArchitectureDrift(
    status="unavailable",
    planned_fingerprint=planned_state.fingerprint,
    current_fingerprint=None,
    message=(
      "Current metadata could not be compared after finalization: "
      f"{error}"
    ),
  )


def _architecture_diff_details(
  diff: ArchitectureDiff,
) -> tuple[str, ...]:
  """
  Return concise deterministic details for one architecture diff.
  """
  return tuple(
    [
      _dataset_change_text(change)
      for change in diff.dataset_changes
    ]
    + [
      _column_change_text(change)
      for change in diff.column_changes
    ]
  )


def _dataset_change_text(change) -> str:
  """
  Return one concise dataset-level change description.
  """
  text = f"{change.change_type} {change.dataset_key}"
  changed_fields = tuple(
    change.details.get("changed_fields", ())
    if isinstance(change.details, dict)
    else ()
  )
  if changed_fields:
    text += " fields=" + ",".join(changed_fields)
  return text


def _column_change_text(change) -> str:
  """
  Return one concise column-level change description.
  """
  text = (
    f"{change.change_type} "
    f"{change.dataset_key}.{change.column_name}"
  )
  changed_fields = tuple(
    change.details.get("changed_fields", ())
    if isinstance(change.details, dict)
    else ()
  )
  if changed_fields:
    text += " fields=" + ",".join(changed_fields)
  return text
