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
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from metadata.architecture.control import ArchitectureControlScope
from metadata.architecture.execution_run_plan import (
  ExecutionRunPlan,
  ExecutionRunPlanStore,
  render_execution_run_plan_json,
)
from metadata.architecture.execution_run_plan_service import (
  ArchitectureExecutionRunPlanError,
  build_architecture_execution_run_plan,
)
from metadata.architecture.execution_run_plan_state import (
  execution_run_plan_planned_state_path,
  load_execution_run_plan_planned_state,
  save_execution_run_plan_bundle,
)
from metadata.architecture.paths import (
  ArchitectureArtifactContext,
  resolve_architecture_artifact_context,
)
from metadata.config.profiles import load_profile
from metadata.config.targets import get_target_system


class Command(BaseCommand):
  """
  Generate one immutable, scheduler-facing Execution Run Plan.
  """
  help = (
    "Generate and store an immutable Execution Run Plan for one "
    "Architecture Control scope."
  )

  def add_arguments(self, parser) -> None:
    scope_group = parser.add_mutually_exclusive_group(required=True)
    scope_group.add_argument(
      "--all-datasets",
      action="store_true",
      help="Create a run plan for all active TargetDatasets.",
    )
    scope_group.add_argument(
      "--schema",
      dest="schema_short",
      help="Create a run plan for one target schema.",
    )
    scope_group.add_argument(
      "--dataset",
      dest="dataset_key",
      help=(
        "Create a run plan for one dataset key in "
        "<schema>.<dataset> form."
      ),
    )
    scope_group.add_argument(
      "--partial-load",
      dest="partial_load_name",
      help=(
        "Create a run plan for one named Partial Load using its "
        "resolved execution roots and dependencies."
      ),
    )

    parser.add_argument(
      "--target-only",
      action="store_true",
      help=(
        "For --dataset only: plan the selected TargetDataset "
        "without execution dependencies."
      ),
    )
    parser.add_argument(
      "--profile",
      required=False,
      help=(
        "Profile name. If omitted, the active elevata profile is used."
      ),
    )
    parser.add_argument(
      "--target-system",
      required=False,
      help=(
        "Target system short name. If omitted, the active target "
        "system is used."
      ),
    )
    parser.add_argument(
      "--run-plan-id",
      required=False,
      help=(
        "Optional stable run-plan identifier. A random identifier "
        "is used when omitted."
      ),
    )
    parser.add_argument(
      "--batch-run-id",
      required=False,
      help=(
        "Optional batch identifier shared by the later dataset "
        "executions."
      ),
    )
    parser.add_argument(
      "--output",
      required=False,
      help=(
        "Optional explicit JSON output path. Otherwise the "
        "context-scoped run-plan store path is used."
      ),
    )
    parser.add_argument(
      "--reuse-existing",
      action="store_true",
      help=(
        "Reuse and validate an existing immutable plan at --output "
        "or at the path selected by --run-plan-id. Intended for "
        "scheduler retries."
      ),
    )
    parser.add_argument(
      "--print-json",
      action="store_true",
      help="Print the canonical run-plan JSON after the summary.",
    )

  def handle(self, *args, **options):
    scope, no_deps = _resolve_scope(options)
    profile, target_system, artifact_context = _resolve_runtime(
      options
    )

    requested_run_plan_id = _optional_text(
      options.get("run_plan_id")
    )
    requested_batch_run_id = _optional_text(
      options.get("batch_run_id")
    )
    output_path = _optional_path(options.get("output"))
    reuse_existing = bool(options.get("reuse_existing"))

    store = ExecutionRunPlanStore(context=artifact_context)

    if reuse_existing:
      existing_path = _resolve_existing_plan_path(
        store=store,
        output_path=output_path,
        run_plan_id=requested_run_plan_id,
      )

      if existing_path.exists():
        try:
          plan = store.load_path(existing_path)
          _validate_existing_plan(
            plan,
            profile_name=profile.name,
            target_system_short=target_system.short_name,
            scope=scope,
            no_deps=no_deps,
            requested_run_plan_id=requested_run_plan_id,
            requested_batch_run_id=requested_batch_run_id,
          )
        except ValueError as exc:
          raise CommandError(str(exc)) from exc

        try:
          load_execution_run_plan_planned_state(
            run_plan_path=existing_path,
            plan=plan,
          )
        except ValueError as exc:
          raise CommandError(str(exc)) from exc

        _write_summary(
          self,
          plan=plan,
          path=existing_path,
          planned_state_path=(
            execution_run_plan_planned_state_path(
              existing_path
            )
          ),
          reused=True,
          print_json=bool(options.get("print_json")),
        )
        return str(existing_path)

    try:
      result = build_architecture_execution_run_plan(
        scope,
        artifact_context=artifact_context,
        no_deps=no_deps,
        run_plan_id=requested_run_plan_id,
        batch_run_id=requested_batch_run_id,
      )
      planned_state = getattr(
        result.control_context,
        "current_state",
        None,
      )
      path, planned_state_path = (
        save_execution_run_plan_bundle(
          store=store,
          plan=result.run_plan,
          planned_state=planned_state,
          output_path=output_path,
        )
      )
    except (
      ArchitectureExecutionRunPlanError,
      OSError,
      ValueError,
    ) as exc:
      raise CommandError(str(exc)) from exc

    _write_summary(
      self,
      plan=result.run_plan,
      path=path,
      planned_state_path=planned_state_path,
      reused=False,
      print_json=bool(options.get("print_json")),
    )
    return str(path)


def _resolve_scope(
  options: dict[str, Any],
) -> tuple[ArchitectureControlScope, bool]:
  """
  Resolve and validate the exact Architecture Control scope.
  """
  all_datasets = bool(options.get("all_datasets"))
  schema_short = _optional_text(options.get("schema_short"))
  dataset_key = _optional_text(options.get("dataset_key"))
  partial_load_name = _optional_text(
    options.get("partial_load_name")
  )
  target_only = bool(options.get("target_only"))

  selected_scope_count = sum((
    1 if all_datasets else 0,
    1 if schema_short else 0,
    1 if dataset_key else 0,
    1 if partial_load_name else 0,
  ))
  if selected_scope_count != 1:
    raise CommandError(
      "Select exactly one run-plan scope: --all-datasets, "
      "--schema, --dataset or --partial-load."
    )

  if target_only and dataset_key is None:
    raise CommandError(
      "--target-only is supported only together with --dataset."
    )

  if all_datasets:
    return ArchitectureControlScope.for_all(), False

  if schema_short is not None:
    try:
      return ArchitectureControlScope.for_schema(
        schema_short
      ), False
    except ValueError as exc:
      raise CommandError(str(exc)) from exc

  if partial_load_name is not None:
    try:
      return ArchitectureControlScope.for_partial_load(
        partial_load_name
      ), False
    except ValueError as exc:
      raise CommandError(str(exc)) from exc

  if dataset_key is None or dataset_key.count(".") != 1:
    raise CommandError(
      "--dataset requires exactly one <schema>.<dataset> key."
    )

  schema_value, target_name = (
    part.strip()
    for part in dataset_key.split(".", 1)
  )
  if not schema_value or not target_name:
    raise CommandError(
      "--dataset requires exactly one <schema>.<dataset> key."
    )

  normalized_key = f"{schema_value}.{target_name}"
  return (
    ArchitectureControlScope(
      mode="target_dataset",
      schema_short=schema_value,
      target_name=target_name,
      dataset_key=normalized_key,
    ),
    target_only,
  )


def _resolve_runtime(
  options: dict[str, Any],
) -> tuple[Any, Any, ArchitectureArtifactContext]:
  """
  Resolve the profile, target system and matching artifact context.
  """
  try:
    profile = load_profile(options.get("profile"))
    target_system = get_target_system(
      options.get("target_system")
    )
    artifact_context = resolve_architecture_artifact_context(
      profile_name=profile.name,
      target_system_short=target_system.short_name,
    )
  except Exception as exc:
    raise CommandError(str(exc)) from exc

  return profile, target_system, artifact_context


def _resolve_existing_plan_path(
  *,
  store: ExecutionRunPlanStore,
  output_path: Path | None,
  run_plan_id: str | None,
) -> Path:
  """
  Return the path eligible for explicit scheduler-retry reuse.
  """
  if output_path is not None:
    return output_path

  if run_plan_id is not None:
    try:
      return store.path_for(run_plan_id)
    except ValueError as exc:
      raise CommandError(str(exc)) from exc

  raise CommandError(
    "--reuse-existing requires --output or --run-plan-id."
  )


def _validate_existing_plan(
  plan: ExecutionRunPlan,
  *,
  profile_name: str,
  target_system_short: str,
  scope: ArchitectureControlScope,
  no_deps: bool,
  requested_run_plan_id: str | None,
  requested_batch_run_id: str | None,
) -> None:
  """
  Verify that a reused plan matches the requested scheduler contract.
  """
  expected_dependency_mode = (
    "target_only"
    if no_deps
    else "with_dependencies"
  )
  mismatches: list[str] = []

  if plan.profile_name != profile_name:
    mismatches.append(
      f"profile {plan.profile_name!r} != {profile_name!r}"
    )
  if plan.target_system_short != target_system_short:
    mismatches.append(
      "target system "
      f"{plan.target_system_short!r} != {target_system_short!r}"
    )
  if plan.scope_mode != scope.mode:
    mismatches.append(
      f"scope mode {plan.scope_mode!r} != {scope.mode!r}"
    )
  if plan.scope_key != scope.key:
    mismatches.append(
      f"scope key {plan.scope_key!r} != {scope.key!r}"
    )
  if plan.dependency_mode != expected_dependency_mode:
    mismatches.append(
      "dependency mode "
      f"{plan.dependency_mode!r} != {expected_dependency_mode!r}"
    )
  if (
    requested_run_plan_id is not None
    and plan.run_plan_id != requested_run_plan_id
  ):
    mismatches.append(
      "run-plan id "
      f"{plan.run_plan_id!r} != {requested_run_plan_id!r}"
    )
  if (
    requested_batch_run_id is not None
    and plan.batch_run_id != requested_batch_run_id
  ):
    mismatches.append(
      "batch-run id "
      f"{plan.batch_run_id!r} != {requested_batch_run_id!r}"
    )

  if mismatches:
    raise ValueError(
      "Existing Execution Run Plan does not match the requested "
      "scheduler context: "
      + "; ".join(mismatches)
      + "."
    )


def _write_summary(
  command: Command,
  *,
  plan: ExecutionRunPlan,
  path: Path,
  planned_state_path: Path,
  reused: bool,
  print_json: bool,
) -> None:
  """
  Write a compact scheduler-facing run-plan summary.
  """
  action = "reused" if reused else "written"
  command.stdout.write(command.style.SUCCESS(
    f"Execution Run Plan {action}: {path}"
  ))
  command.stdout.write(
    f"Planned Architecture State: {planned_state_path}"
  )
  command.stdout.write(f"Run plan id: {plan.run_plan_id}")
  command.stdout.write(f"Batch run id: {plan.batch_run_id}")
  command.stdout.write(
    f"Runtime: {plan.profile_name}/{plan.target_system_short}"
  )
  command.stdout.write(
    f"Scope: {plan.scope_label} ({plan.dependency_mode})"
  )
  command.stdout.write(f"Datasets: {plan.dataset_count}")
  command.stdout.write(
    "Decisions: "
    + ", ".join(
      f"{decision}={count}"
      for decision, count in plan.decision_counts.items()
    )
  )
  command.stdout.write(
    "Impact plan fingerprint: "
    f"{plan.impact_plan_fingerprint}"
  )
  command.stdout.write(
    "Execution plan fingerprint: "
    f"{plan.execution_plan_fingerprint}"
  )
  command.stdout.write(
    f"Run plan fingerprint: {plan.run_plan_fingerprint}"
  )

  if print_json:
    command.stdout.write(
      render_execution_run_plan_json(plan).rstrip()
    )


def _optional_text(value: Any) -> str | None:
  """
  Normalize one optional command-line string.
  """
  normalized = str(value or "").strip()
  return normalized or None


def _optional_path(value: Any) -> Path | None:
  """
  Normalize one optional explicit output path.
  """
  normalized = _optional_text(value)
  return (
    Path(normalized).expanduser()
    if normalized is not None
    else None
  )
