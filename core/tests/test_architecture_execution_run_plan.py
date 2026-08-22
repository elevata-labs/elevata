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

import json

import pytest

from metadata.architecture.execution_impact import (
  ExecutionImpactSelection,
)
from metadata.architecture.execution_run_plan import (
  EXECUTION_RUN_PLAN_ARTIFACT_TYPE,
  EXECUTION_RUN_PLAN_ARTIFACT_VERSION,
  build_execution_run_plan,
  execution_run_plan_from_dict,
  parse_execution_run_plan_json,
  render_execution_run_plan_json,
)


def _fingerprint(character: str) -> str:
  """
  Return one deterministic SHA-256-shaped test fingerprint.
  """
  return character * 64


def _selection(
  *,
  second_decision: str = "FULL_REBUILD",
) -> ExecutionImpactSelection:
  """
  Return an executable impact selection in execution order.
  """
  return ExecutionImpactSelection(
    plan_fingerprint=_fingerprint("c"),
    dataset_decisions=(
      ("raw.customer", "REUSE"),
      ("rawcore.customer", second_decision),
    ),
  )


def _run_plan(
  *,
  second_decision: str = "FULL_REBUILD",
  dependency_mode: str = "with_dependencies",
  scope_mode: str = "all",
  root_dataset_keys: tuple[str, ...] = ("raw.customer",),
  review_status: str = "no_changes",
  approval_id: str | None = None,
):
  """
  Return one valid public Execution Run Plan.
  """
  return build_execution_run_plan(
    run_plan_id="run-plan-001",
    batch_run_id="batch-001",
    created_at="2026-07-24T06:15:30.123456+02:00",
    profile_name="dev",
    target_system_short="dwh",
    scope_mode=scope_mode,
    scope_key=(
      "all"
      if scope_mode == "all"
      else "rawcore.customer"
    ),
    scope_label=(
      "All datasets"
      if scope_mode == "all"
      else "rawcore.customer"
    ),
    dependency_mode=dependency_mode,
    review_status=review_status,
    approval_id=approval_id,
    architecture_fingerprint=_fingerprint("a"),
    report_fingerprint=_fingerprint("b"),
    preview_fingerprint=_fingerprint("d"),
    execution_plan_fingerprint=_fingerprint("e"),
    root_dataset_keys=root_dataset_keys,
    impact_selection=_selection(
      second_decision=second_decision,
    ),
  )


def test_execution_run_plan_round_trip_is_canonical_and_executable() -> None:
  """
  Verify UTC normalization, JSON round-trip and selection recovery.
  """
  plan = _run_plan()

  assert plan.created_at == "2026-07-24T04:15:30.123456+00:00"
  assert plan.dataset_keys == (
    "raw.customer",
    "rawcore.customer",
  )
  assert plan.decision_counts == {
    "REUSE": 1,
    "INCREMENTAL_EXECUTE": 0,
    "FULL_REBUILD": 1,
  }
  assert plan.selection == _selection()
  assert plan.decision_for_dataset("raw.customer") == "REUSE"

  rendered = render_execution_run_plan_json(plan)
  loaded = parse_execution_run_plan_json(rendered)
  payload = json.loads(rendered)

  assert loaded == plan
  assert payload["artifact_type"] == EXECUTION_RUN_PLAN_ARTIFACT_TYPE
  assert (
    payload["artifact_version"]
    == EXECUTION_RUN_PLAN_ARTIFACT_VERSION
  )
  assert payload["batch_run_id"] == "batch-001"
  assert payload["dataset_count"] == 2
  assert payload["run_plan_fingerprint"] == plan.run_plan_fingerprint


def test_execution_run_plan_removes_non_binding_approval_id() -> None:
  """
  Verify a stale approval id is not exposed for a no-changes run plan.
  """
  plan = _run_plan(
    review_status="no_changes",
    approval_id="apr_stale",
  )

  assert plan.approval_id is None
  assert plan.to_dict()["approval_id"] is None


def test_execution_run_plan_preserves_approved_binding() -> None:
  """
  Verify an approved run plan retains its required approval binding.
  """
  plan = _run_plan(
    review_status="approved",
    approval_id="apr_123",
  )

  assert plan.approval_id == "apr_123"
  assert plan.to_dict()["approval_id"] == "apr_123"


def test_execution_run_plan_requires_approved_binding() -> None:
  """
  Verify an approved review status cannot omit its approval identifier.
  """
  with pytest.raises(
    ValueError,
    match="Approved Execution Run Plans require an approval id",
  ):
    _run_plan(review_status="approved")


def test_execution_run_plan_fingerprint_changes_with_decision() -> None:
  """
  Verify dataset decisions are bound into the run-plan fingerprint.
  """
  rebuild_plan = _run_plan(
    second_decision="FULL_REBUILD",
  )
  incremental_plan = _run_plan(
    second_decision="INCREMENTAL_EXECUTE",
  )

  assert (
    rebuild_plan.run_plan_fingerprint
    != incremental_plan.run_plan_fingerprint
  )


def test_execution_run_plan_rejects_non_executable_decision() -> None:
  """
  Verify BLOCKED and REVALIDATE cannot enter a scheduler run plan.
  """
  payload = json.loads(
    render_execution_run_plan_json(_run_plan())
  )
  payload["dataset_decisions"][1]["decision"] = "BLOCKED"

  with pytest.raises(
    ValueError,
    match="cannot execute decision BLOCKED",
  ):
    execution_run_plan_from_dict(payload)


def test_execution_run_plan_rejects_tampered_payload() -> None:
  """
  Verify a structurally valid modification fails closed.
  """
  payload = json.loads(
    render_execution_run_plan_json(_run_plan())
  )
  payload["dataset_decisions"][1]["decision"] = "INCREMENTAL_EXECUTE"
  payload["decision_counts"] = {
    "REUSE": 1,
    "INCREMENTAL_EXECUTE": 1,
    "FULL_REBUILD": 0,
  }

  with pytest.raises(
    ValueError,
    match="fingerprint does not match",
  ):
    execution_run_plan_from_dict(payload)


def test_target_only_run_plan_rejects_additional_datasets() -> None:
  """
  Verify target-only scope cannot silently carry dependency decisions.
  """
  with pytest.raises(
    ValueError,
    match="must contain exactly their selected root dataset",
  ):
    _run_plan(
      dependency_mode="target_only",
      scope_mode="target_dataset",
      root_dataset_keys=("rawcore.customer",),
    )


def test_execution_run_plan_rejects_unknown_artifact_version() -> None:
  """
  Verify public scheduler contracts fail closed on unknown versions.
  """
  payload = json.loads(
    render_execution_run_plan_json(_run_plan())
  )
  payload["artifact_version"] = 2

  with pytest.raises(
    ValueError,
    match="Unsupported Execution Run Plan artifact version",
  ):
    execution_run_plan_from_dict(payload)


def test_partial_load_run_plan_round_trip_binds_named_scope_and_roots() -> None:
  """
  Verify Partial Load identity, explicit roots and resolved decisions are bound.
  """
  selection = _selection()
  plan = build_execution_run_plan(
    run_plan_id="run-plan-sales",
    batch_run_id="batch-sales",
    created_at="2026-08-20T04:30:00+00:00",
    profile_name="dev",
    target_system_short="dwh",
    scope_mode="partial_load",
    scope_key="partial_load:sales",
    scope_label="Partial load: sales",
    dependency_mode="with_dependencies",
    review_status="no_changes",
    approval_id=None,
    architecture_fingerprint=_fingerprint("a"),
    report_fingerprint=_fingerprint("b"),
    preview_fingerprint=_fingerprint("d"),
    execution_plan_fingerprint=_fingerprint("e"),
    root_dataset_keys=("rawcore.customer",),
    impact_selection=selection,
  )

  loaded = parse_execution_run_plan_json(
    render_execution_run_plan_json(plan)
  )

  assert loaded == plan
  assert plan.scope_mode == "partial_load"
  assert plan.scope_key == "partial_load:sales"
  assert plan.root_dataset_keys == ("rawcore.customer",)
  assert plan.dataset_keys == selection.dataset_keys


def test_partial_load_run_plan_fingerprint_binds_explicit_root_intent() -> None:
  """
  Verify redundant root changes alter the plan even if execution scope is equal.
  """
  common = dict(
    run_plan_id="run-plan-sales",
    batch_run_id="batch-sales",
    created_at="2026-08-20T04:30:00+00:00",
    profile_name="dev",
    target_system_short="dwh",
    scope_mode="partial_load",
    scope_key="partial_load:sales",
    scope_label="Partial load: sales",
    dependency_mode="with_dependencies",
    review_status="no_changes",
    approval_id=None,
    architecture_fingerprint=_fingerprint("a"),
    report_fingerprint=_fingerprint("b"),
    preview_fingerprint=_fingerprint("d"),
    execution_plan_fingerprint=_fingerprint("e"),
    impact_selection=_selection(),
  )
  original = build_execution_run_plan(
    **common,
    root_dataset_keys=("rawcore.customer",),
  )
  redundant_root_added = build_execution_run_plan(
    **common,
    root_dataset_keys=(
      "raw.customer",
      "rawcore.customer",
    ),
  )

  assert original.dataset_keys == redundant_root_added.dataset_keys
  assert (
    original.run_plan_fingerprint
    != redundant_root_added.run_plan_fingerprint
  )


def test_partial_load_run_plan_requires_named_dependency_scope() -> None:
  """
  Verify Partial Load run plans fail closed on invalid identity or target-only mode.
  """
  common = dict(
    run_plan_id="run-plan-sales",
    batch_run_id="batch-sales",
    created_at="2026-08-20T04:30:00+00:00",
    profile_name="dev",
    target_system_short="dwh",
    scope_mode="partial_load",
    scope_label="Partial load: sales",
    review_status="no_changes",
    approval_id=None,
    architecture_fingerprint=_fingerprint("a"),
    report_fingerprint=_fingerprint("b"),
    preview_fingerprint=_fingerprint("d"),
    execution_plan_fingerprint=_fingerprint("e"),
    root_dataset_keys=("rawcore.customer",),
    impact_selection=_selection(),
  )

  with pytest.raises(ValueError, match="partial_load:<name>"):
    build_execution_run_plan(
      **common,
      scope_key="sales",
      dependency_mode="with_dependencies",
    )

  with pytest.raises(ValueError, match="require dependency execution"):
    build_execution_run_plan(
      **common,
      scope_key="partial_load:sales",
      dependency_mode="target_only",
    )
