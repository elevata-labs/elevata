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

from pathlib import Path
import json

from django.core.management.base import BaseCommand, CommandError

from metadata.architecture.execution_run_finalization import (
  build_execution_run_plan_finalization,
  execution_run_plan_finalization_path,
  load_execution_run_plan_finalization,
  load_execution_run_plan_outcomes,
  write_execution_run_plan_finalization,
)
from metadata.architecture.execution_run_recovery import (
  execution_run_plan_recovery_path,
  load_execution_run_plan_recovery,
  validate_interrupted_initial_deployment_scope,
  write_execution_run_plan_recovery,
)
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
  ExecutionRunPlanStore,
)
from metadata.architecture.execution_run_plan_state import (
  ExecutionRunPlanArchitectureDrift,
  build_execution_run_plan_architecture_drift,
  execution_run_plan_planned_state_path,
  load_execution_run_plan_planned_state,
  unavailable_execution_run_plan_architecture_drift,
)
from metadata.architecture.paths import (
  resolve_architecture_artifact_context,
)
from metadata.architecture.physical_state import (
  resolve_architecture_baseline,
)
from metadata.architecture.service import ArchitectureStateService
from metadata.architecture.state import ArchitectureState
from metadata.architecture.store import ArchitectureStateStore


def _load_execution_run_plan(
  path: str | Path,
) -> ExecutionRunPlan:
  """
  Load one immutable public Execution Run Plan.
  """
  run_plan_path = Path(path).expanduser()

  try:
    return ExecutionRunPlanStore(
      base_path=run_plan_path.parent
    ).load_path(run_plan_path)
  except ValueError as exc:
    raise CommandError(str(exc)) from exc


def _resolve_post_plan_architecture_drift(
  *,
  planned_state: ArchitectureState,
) -> ExecutionRunPlanArchitectureDrift:
  """
  Compare current metadata with the planned state without blocking finalization.
  """
  try:
    current_state = (
      ArchitectureStateService()
      .build_current_state()
    )
    return build_execution_run_plan_architecture_drift(
      planned_state=planned_state,
      current_state=current_state,
    )
  except Exception as exc:
    return unavailable_execution_run_plan_architecture_drift(
      planned_state=planned_state,
      error=exc,
    )


def _persist_planned_architecture_state(
  *,
  plan: ExecutionRunPlan,
  planned_state: ArchitectureState,
) -> Path:
  """
  Strictly persist and verify the successfully applied architecture state.

  Distributed scheduler steps intentionally do not advance Architecture State.
  The finalizer is the single controlled writer after every bound dataset
  outcome has been validated as successful.
  """
  context = resolve_architecture_artifact_context(
    profile_name=plan.profile_name,
    target_system_short=plan.target_system_short,
  )

  try:
    state_store = ArchitectureStateStore(
      context=context
    )
    state_path = state_store.state_file_path()
    state_store.save(planned_state)
    recorded_state = state_store.load()
  except Exception as exc:
    raise CommandError(
      "Execution Run Plan Architecture State could not be "
      f"persisted: {exc}"
    ) from exc

  if recorded_state is None:
    raise CommandError(
      "Execution Run Plan Architecture State could not be "
      f"reloaded after persistence: {state_path}"
    )

  recorded_fingerprint = str(
    getattr(
      recorded_state,
      "fingerprint",
      "",
    )
    or ""
  ).strip()

  if (
    recorded_fingerprint
    != plan.architecture_fingerprint
  ):
    raise CommandError(
      "Persisted Architecture State fingerprint does not match "
      "the finalized Execution Run Plan: "
      f"planned={plan.architecture_fingerprint} "
      f"recorded={recorded_fingerprint or '<missing>'}."
    )

  return state_path


def _resolve_existing_finalized_state_path(
  *,
  plan: ExecutionRunPlan,
  planned_state: ArchitectureState,
) -> Path:
  """
  Return the applied-state path without rolling back a later finalized state.
  """
  context = resolve_architecture_artifact_context(
    profile_name=plan.profile_name,
    target_system_short=plan.target_system_short,
  )
  state_store = ArchitectureStateStore(context=context)
  state_path = state_store.state_file_path()
  recorded_state = state_store.load()

  if recorded_state is None:
    return _persist_planned_architecture_state(
      plan=plan,
      planned_state=planned_state,
    )

  return state_path


def _resolve_recovery_architecture(
  *,
  plan: ExecutionRunPlan,
  state_store: ArchitectureStateStore,
) -> tuple[ArchitectureState, ArchitectureState, tuple[str, ...]]:
  """
  Resolve current metadata and verify its complete physical target state.
  """
  try:
    current_state = (
      ArchitectureStateService()
      .build_current_state()
    )
  except Exception as exc:
    raise CommandError(
      "Current Architecture State could not be built for interrupted "
      f"initial-deployment recovery: {exc}"
    ) from exc

  context = resolve_architecture_artifact_context(
    profile_name=plan.profile_name,
    target_system_short=plan.target_system_short,
  )
  try:
    baseline = resolve_architecture_baseline(
      current_state=current_state,
      artifact_context=context,
      state_store=state_store,
      allow_physical_discovery=True,
    )
  except Exception as exc:
    raise CommandError(
      "Physical Architecture State could not be discovered for interrupted "
      f"initial-deployment recovery: {exc}"
    ) from exc

  if baseline.source != "discovered_physical_state":
    raise CommandError(
      "Interrupted initial-deployment recovery requires a populated target "
      "platform discovered without a recorded Architecture State. "
      f"Resolved baseline source: {baseline.source}. "
      f"{baseline.message}"
    )

  physical_state = baseline.previous_state
  if not isinstance(physical_state, ArchitectureState):
    raise CommandError(
      "Interrupted initial-deployment recovery did not produce a valid "
      "physical Architecture State."
    )

  return (
    current_state,
    physical_state,
    tuple(baseline.warnings or ()),
  )


def _persist_recovered_architecture_state(
  *,
  state_store: ArchitectureStateStore,
  recovered_state: ArchitectureState,
) -> Path:
  """
  Persist and verify one recovered applied Architecture State.
  """
  state_path = state_store.state_file_path()
  try:
    existing_state = state_store.load()
    if existing_state is not None:
      if existing_state.fingerprint != recovered_state.fingerprint:
        raise ValueError(
          "Another Architecture State was persisted while recovery was "
          "running."
        )
      return state_path

    state_store.save(recovered_state)
    recorded_state = state_store.load()
  except Exception as exc:
    raise CommandError(
      "Recovered Architecture State could not be persisted: "
      f"{exc}"
    ) from exc

  if recorded_state is None:
    raise CommandError(
      "Recovered Architecture State could not be reloaded after "
      f"persistence: {state_path}"
    )
  if recorded_state.fingerprint != recovered_state.fingerprint:
    raise CommandError(
      "Persisted recovered Architecture State fingerprint does not match "
      "the physically validated state."
    )

  return state_path


def recover_interrupted_initial_deployment(
  *,
  run_plan_path: str | Path,
) -> tuple[
  ExecutionRunPlan,
  tuple[dict[str, object], ...],
  Path,
  Path,
  dict[str, object],
]:
  """
  Establish a missing initial baseline from complete scheduler evidence.

  Recovery is intentionally explicit. It is available only for legacy Run
  Plans that predate planned-state snapshots, have complete successful
  all-datasets outcomes, and whose physical target matches current metadata.
  """
  path = Path(run_plan_path).expanduser()
  plan = _load_execution_run_plan(path)

  try:
    validate_interrupted_initial_deployment_scope(plan)
  except ValueError as exc:
    raise CommandError(str(exc)) from exc

  if execution_run_plan_planned_state_path(path).exists():
    raise CommandError(
      "Execution Run Plan already has a planned Architecture State. Use "
      "normal finalization instead of interrupted-deployment recovery."
    )
  if execution_run_plan_finalization_path(path).exists():
    raise CommandError(
      "Execution Run Plan already has a finalization artifact. Recovery is "
      "not applicable."
    )

  context = resolve_architecture_artifact_context(
    profile_name=plan.profile_name,
    target_system_short=plan.target_system_short,
  )
  try:
    state_store = ArchitectureStateStore(context=context)
    state_path = state_store.state_file_path()
    recorded_state = state_store.load()
  except Exception as exc:
    raise CommandError(
      "Recorded Architecture State could not be inspected for interrupted "
      f"initial-deployment recovery: {exc}"
    ) from exc

  if state_path.exists() and recorded_state is None:
    raise CommandError(
      "Interrupted initial-deployment recovery found an existing but invalid "
      f"Architecture State artifact: {state_path}."
    )

  if recorded_state is not None:
    recovery_path = execution_run_plan_recovery_path(path)
    if recovery_path.exists():
      try:
        recovery = load_execution_run_plan_recovery(
          recovery_path,
          plan=plan,
        )
        recovered_fingerprint = str(
          recovery.get(
            "recovered_architecture_fingerprint"
          )
          or ""
        ).strip()
        if recorded_state.fingerprint != recovered_fingerprint:
          raise ValueError(
            "Recorded Architecture State does not match the existing "
            "recovery artifact."
          )
        outcomes = load_execution_run_plan_outcomes(
          run_plan_path=path,
          plan=plan,
        )
        build_execution_run_plan_finalization(
          plan=plan,
          outcomes=outcomes,
        )
      except (OSError, TypeError, ValueError) as exc:
        raise CommandError(
          "Existing interrupted initial-deployment recovery is invalid: "
          f"{exc}"
        ) from exc

      return (
        plan,
        outcomes,
        state_store.state_file_path(),
        recovery_path,
        recovery,
      )

    raise CommandError(
      "Interrupted initial-deployment recovery requires a missing recorded "
      "Architecture State. A baseline already exists for this runtime."
    )

  try:
    outcomes = load_execution_run_plan_outcomes(
      run_plan_path=path,
      plan=plan,
    )
    finalization = build_execution_run_plan_finalization(
      plan=plan,
      outcomes=outcomes,
    )
  except (OSError, TypeError, ValueError) as exc:
    raise CommandError(
      "Interrupted initial-deployment recovery requires complete successful "
      f"scheduler outcomes: {exc}"
    ) from exc

  current_state, physical_state, warnings = (
    _resolve_recovery_architecture(
      plan=plan,
      state_store=state_store,
    )
  )

  try:
    recovery_path = write_execution_run_plan_recovery(
      run_plan_path=path,
      plan=plan,
      finalization=finalization,
      recovered_state=current_state,
      physical_state=physical_state,
      warnings=warnings,
    )
    recovery = load_execution_run_plan_recovery(
      recovery_path,
      plan=plan,
    )
  except (OSError, TypeError, ValueError) as exc:
    raise CommandError(
      "Interrupted initial-deployment recovery validation failed: "
      f"{exc}"
    ) from exc

  state_path = _persist_recovered_architecture_state(
    state_store=state_store,
    recovered_state=current_state,
  )

  return (
    plan,
    outcomes,
    state_path,
    recovery_path,
    recovery,
  )


def finalize_execution_run_plan(
  *,
  run_plan_path: str | Path,
) -> tuple[
  ExecutionRunPlan,
  tuple[dict[str, object], ...],
  Path,
  Path,
  dict[str, object],
  ExecutionRunPlanArchitectureDrift,
]:
  """
  Validate and finalize one complete distributed scheduler run.

  Architecture State is advanced exactly once, after every scheduler-step
  outcome has passed the immutable finalization contract.
  """
  path = Path(
    run_plan_path
  ).expanduser()

  plan = _load_execution_run_plan(
    path
  )

  try:
    planned_state = load_execution_run_plan_planned_state(
      run_plan_path=path,
      plan=plan,
    )
  except ValueError as exc:
    raise CommandError(
      f"{exc} If this is a completed legacy initial deployment, rerun this "
      "command with --recover-interrupted-initial-deployment."
    ) from exc

  try:
    outcomes = (
      load_execution_run_plan_outcomes(
        run_plan_path=path,
        plan=plan,
      )
    )

    finalization_preview = (
      build_execution_run_plan_finalization(
        plan=plan,
        outcomes=outcomes,
      )
    )

  except (
    OSError,
    TypeError,
    ValueError,
  ) as exc:
    raise CommandError(
      "Execution Run Plan could not be finalized: "
      f"{exc}"
    ) from exc

  existing_finalization_path = (
    execution_run_plan_finalization_path(path)
  )
  if existing_finalization_path.exists():
    try:
      finalization = load_execution_run_plan_finalization(
        existing_finalization_path,
        plan=plan,
      )
    except (OSError, TypeError, ValueError) as exc:
      raise CommandError(
        "Execution Run Plan finalization artifact could not be "
        f"loaded: {exc}"
      ) from exc

    if (
      finalization.get(
        "finalization_fingerprint"
      )
      != finalization_preview.get(
        "finalization_fingerprint"
      )
    ):
      raise CommandError(
        "Stored Execution Run Plan finalization does not match "
        "the current scheduler outcome evidence."
      )

    state_path = _resolve_existing_finalized_state_path(
      plan=plan,
      planned_state=planned_state,
    )
    post_plan_drift = _resolve_post_plan_architecture_drift(
      planned_state=planned_state,
    )
    return (
      plan,
      outcomes,
      state_path,
      existing_finalization_path,
      finalization,
      post_plan_drift,
    )

  state_path = _persist_planned_architecture_state(
    plan=plan,
    planned_state=planned_state,
  )

  try:
    finalization_path = (
      write_execution_run_plan_finalization(
        run_plan_path=path,
        plan=plan,
        outcomes=outcomes,
      )
    )

    finalization = (
      load_execution_run_plan_finalization(
        finalization_path,
        plan=plan,
      )
    )
  except (
    OSError,
    TypeError,
    ValueError,
  ) as exc:
    raise CommandError(
      "Execution Run Plan finalization artifact could not be "
      f"persisted: {exc}"
    ) from exc

  if (
    finalization.get(
      "finalization_fingerprint"
    )
    != finalization_preview.get(
      "finalization_fingerprint"
    )
  ):
    raise CommandError(
      "Persisted Execution Run Plan finalization does not match "
      "the validated scheduler evidence."
    )

  post_plan_drift = _resolve_post_plan_architecture_drift(
    planned_state=planned_state,
  )

  return (
    plan,
    outcomes,
    state_path,
    finalization_path,
    finalization,
    post_plan_drift,
  )


class Command(BaseCommand):
  help = (
    "Validate all scheduler-step outcomes and finalize one "
    "immutable Execution Run Plan."
  )

  def add_arguments(
    self,
    parser,
  ) -> None:
    parser.add_argument(
      "run_plan_path",
      help=(
        "Path to the immutable Execution Run Plan "
        "JSON artifact."
      ),
    )
    parser.add_argument(
      "--print-json",
      action="store_true",
      help=(
        "Print the validated finalization artifact "
        "as JSON."
      ),
    )
    parser.add_argument(
      "--recover-interrupted-initial-deployment",
      action="store_true",
      help=(
        "Recover a missing initial Architecture State from a legacy "
        "all-datasets Run Plan with complete successful outcomes and a "
        "physically verified target platform."
      ),
    )

  def handle(
    self,
    *args,
    **options,
  ):
    run_plan_path = options.get(
      "run_plan_path"
    )

    if not str(
      run_plan_path or ""
    ).strip():
      raise CommandError(
        "Execution Run Plan path is required."
      )

    if options.get(
      "recover_interrupted_initial_deployment"
    ):
      (
        plan,
        outcomes,
        state_path,
        recovery_path,
        recovery,
      ) = recover_interrupted_initial_deployment(
        run_plan_path=run_plan_path,
      )

      self.stdout.write(self.style.SUCCESS(
        "Interrupted initial deployment recovered: "
        f"{recovery_path}"
      ))
      self.stdout.write(
        f"Run plan id: {plan.run_plan_id}"
      )
      self.stdout.write(
        f"Batch run id: {plan.batch_run_id}"
      )
      self.stdout.write(
        "Runtime: "
        f"{plan.profile_name}/"
        f"{plan.target_system_short}"
      )
      self.stdout.write(
        f"Datasets: {len(outcomes)}"
      )
      self.stdout.write(
        f"Recovered Architecture State: {state_path}"
      )
      self.stdout.write(
        "Physical validation: "
        f"{recovery.get('physical_validation')}"
      )
      self.stdout.write(
        "Recovered fingerprint: "
        f"{recovery.get('recovered_architecture_fingerprint')}"
      )
      self.stdout.write(
        "Metadata changed after original plan: "
        f"{recovery.get('metadata_changed_after_plan')}"
      )
      self.stdout.write(
        "Recovery fingerprint: "
        f"{recovery.get('recovery_fingerprint')}"
      )

      for warning in recovery.get("warnings") or ():
        self.stdout.write(self.style.WARNING(
          f"Physical discovery warning: {warning}"
        ))

      if options.get("print_json"):
        self.stdout.write(
          json.dumps(
            recovery,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
          )
        )
      return

    (
      plan,
      outcomes,
      state_path,
      finalization_path,
      finalization,
      post_plan_drift,
    ) = finalize_execution_run_plan(
      run_plan_path=run_plan_path,
    )

    self.stdout.write(
      self.style.SUCCESS(
        "Execution Run Plan finalized: "
        f"{finalization_path}"
      )
    )
    self.stdout.write(
      f"Run plan id: {plan.run_plan_id}"
    )
    self.stdout.write(
      f"Batch run id: {plan.batch_run_id}"
    )
    self.stdout.write(
      "Runtime: "
      f"{plan.profile_name}/"
      f"{plan.target_system_short}"
    )
    self.stdout.write(
      f"Datasets: {len(outcomes)}"
    )
    self.stdout.write(
      f"Architecture state: {state_path}"
    )
    self.stdout.write(
      "Decisions: "
      f"{finalization.get('decision_counts')}"
    )
    self.stdout.write(
      "Statuses: "
      f"{finalization.get('status_counts')}"
    )
    self.stdout.write(
      "Finalization fingerprint: "
      f"{finalization.get('finalization_fingerprint')}"
    )

    if post_plan_drift.status == "changed":
      self.stdout.write(self.style.WARNING(
        "Post-plan metadata drift detected. The planned architecture was "
        "finalized successfully; start a new DAG run to evaluate the changes."
      ))
      for detail in (
        post_plan_drift.dataset_changes
        + post_plan_drift.column_changes
      ):
        self.stdout.write(self.style.WARNING(
          f"  - {detail}"
        ))
    elif post_plan_drift.status == "unavailable":
      self.stdout.write(self.style.WARNING(
        post_plan_drift.message
      ))
    else:
      self.stdout.write(
        "Post-plan metadata drift: none"
      )

    if options.get("print_json"):
      self.stdout.write(
        json.dumps(
          finalization,
          indent=2,
          sort_keys=True,
          ensure_ascii=False,
          allow_nan=False,
        )
      )
