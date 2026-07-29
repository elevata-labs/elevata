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

import pytest

from metadata.architecture.execution_impact import (
  ExecutionImpactEvidenceReference,
  ExecutionImpactItem,
  ExecutionImpactPlan,
)
from metadata.architecture.execution_impact_propagation import (
  propagate_execution_impact_plan,
)


@dataclass(frozen=True)
class DummySchema:
  short_name: str
  default_materialization_type: str = "table"


@dataclass(frozen=True)
class DummyTargetDataset:
  target_schema: DummySchema
  target_dataset_name: str
  incremental_strategy: str = "merge"
  materialization_type: str | None = None

  @property
  def effective_materialization_type(self) -> str:
    return (
      self.materialization_type
      or self.target_schema.default_materialization_type
    )


@dataclass(frozen=True)
class DummyDependency:
  upstream: DummyTargetDataset
  reason: str
  reference_id: int | None = None


def _target(
  dataset_key: str,
  *,
  incremental_strategy: str = "merge",
  materialization_type: str | None = None,
) -> DummyTargetDataset:
  schema_short, target_name = dataset_key.split(".", 1)
  return DummyTargetDataset(
    target_schema=DummySchema(schema_short),
    target_dataset_name=target_name,
    incremental_strategy=incremental_strategy,
    materialization_type=materialization_type,
  )


def _item(
  dataset_key: str,
  *,
  decision: str = "REUSE",
  reason_codes: tuple[str, ...] | None = None,
) -> ExecutionImpactItem:
  reasons = reason_codes
  if reasons is None:
    reasons = {
      "REUSE": ("NO_RELEVANT_CHANGE",),
      "REVALIDATE": ("REVALIDATION_REQUIRED",),
      "INCREMENTAL_EXECUTE": ("SOURCE_CHANGE_DETECTED",),
      "FULL_REBUILD": ("DATASET_DEFINITION_CHANGED",),
      "BLOCKED": ("POLICY_BLOCKED",),
    }[decision]

  return ExecutionImpactItem(
    dataset_key=dataset_key,
    decision=decision,
    reason_codes=reasons,
    architecture_fingerprint=f"arch-{dataset_key}",
    evidence=(ExecutionImpactEvidenceReference(
      evidence_type="architecture_state",
      evidence_key=f"current:{dataset_key}",
      status="available",
      fingerprint=f"arch-{dataset_key}",
    ),),
  )


def _plan(*items: ExecutionImpactItem) -> ExecutionImpactPlan:
  return ExecutionImpactPlan(
    scope_key="all",
    architecture_fingerprint="architecture-1",
    baseline_fingerprint="baseline-1",
    report_fingerprint="report-1",
    items=tuple(items),
    evidence=(ExecutionImpactEvidenceReference(
      evidence_type="architecture_change_report",
      evidence_key="scope:all",
      status="available",
      fingerprint="report-1",
    ),),
  )


def _resolver(
  dependencies: dict[str, tuple[DummyDependency, ...]],
):
  def resolve(target_dataset: DummyTargetDataset):
    key = (
      f"{target_dataset.target_schema.short_name}."
      f"{target_dataset.target_dataset_name}"
    )
    return dependencies.get(key, ())

  return resolve


def _items_by_key(plan: ExecutionImpactPlan):
  return {
    item.dataset_key: item
    for item in plan.items
  }


def test_lineage_full_rebuild_propagates_transitively() -> None:
  raw = _target("raw.customer")
  stage = _target("stage.customer", incremental_strategy="full")
  core = _target("rawcore.customer", incremental_strategy="full")
  local_plan = _plan(
    _item("raw.customer", decision="FULL_REBUILD"),
    _item("stage.customer"),
    _item("rawcore.customer"),
  )

  result = propagate_execution_impact_plan(
    local_plan=local_plan,
    target_datasets=(core, raw, stage),
    dependency_resolver=_resolver({
      "stage.customer": (DummyDependency(raw, "lineage_input"),),
      "rawcore.customer": (DummyDependency(stage, "lineage_input"),),
    }),
  )
  items = _items_by_key(result)

  assert items["stage.customer"].decision == "FULL_REBUILD"
  assert items["stage.customer"].reason_codes == (
    "UPSTREAM_REBUILD_REQUIRED",
  )
  assert items["rawcore.customer"].decision == "FULL_REBUILD"
  assert items["rawcore.customer"].reason_codes == (
    "UPSTREAM_REBUILD_REQUIRED",
  )
  assert items["raw.customer"].downstream_dataset_keys == (
    "stage.customer",
  )
  assert items["stage.customer"].upstream_dataset_keys == (
    "raw.customer",
  )


def test_incremental_data_change_propagates_through_source_raw_ready() -> None:
  raw = _target("raw.order")
  stage = _target("stage.order")
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("raw.order", decision="INCREMENTAL_EXECUTE"),
      _item("stage.order"),
    ),
    target_datasets=(stage, raw),
    dependency_resolver=_resolver({
      "stage.order": (DummyDependency(raw, "source_raw_ready"),),
    }),
  )
  stage_item = _items_by_key(result)["stage.order"]

  assert stage_item.decision == "INCREMENTAL_EXECUTE"
  assert stage_item.reason_codes == (
    "INCREMENTAL_LOAD_STRATEGY",
    "UPSTREAM_EXECUTION_REQUIRED",
  )
  assert stage_item.propagations[0].dependency_reason == "source_raw_ready"



def test_incremental_upstream_requires_full_rebuild_for_full_refresh_child() -> None:
  raw = _target("raw.order")
  stage = _target("stage.order", incremental_strategy="full")
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("raw.order", decision="INCREMENTAL_EXECUTE"),
      _item("stage.order"),
    ),
    target_datasets=(raw, stage),
    dependency_resolver=_resolver({
      "stage.order": (DummyDependency(raw, "lineage_input"),),
    }),
  )
  stage_item = _items_by_key(result)["stage.order"]

  assert stage_item.decision == "FULL_REBUILD"
  assert stage_item.reason_codes == (
    "UPSTREAM_EXECUTION_REQUIRED",
  )


def test_view_is_rebuilt_after_upstream_full_rebuild() -> None:
  raw = _target("raw.customer")
  serving = _target(
    "serving.customer",
    incremental_strategy="full",
    materialization_type="view",
  )
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("raw.customer", decision="FULL_REBUILD"),
      _item("serving.customer"),
    ),
    target_datasets=(serving, raw),
    dependency_resolver=_resolver({
      "serving.customer": (DummyDependency(raw, "lineage_input"),),
    }),
  )
  serving_item = _items_by_key(result)["serving.customer"]

  assert serving_item.decision == "FULL_REBUILD"
  assert serving_item.reason_codes == (
    "UPSTREAM_REBUILD_REQUIRED",
  )
  assert serving_item.propagations[0].reason_code == (
    "UPSTREAM_REBUILD_REQUIRED"
  )


def test_view_reuses_virtual_materialization_for_incremental_upstream() -> None:
  raw = _target("raw.customer")
  serving = _target(
    "serving.customer",
    incremental_strategy="full",
    materialization_type="view",
  )
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item(
        "raw.customer",
        decision="INCREMENTAL_EXECUTE",
      ),
      _item("serving.customer"),
    ),
    target_datasets=(serving, raw),
    dependency_resolver=_resolver({
      "serving.customer": (
        DummyDependency(raw, "lineage_input"),
      ),
    }),
  )
  serving_item = _items_by_key(result)["serving.customer"]

  assert serving_item.decision == "REUSE"
  assert serving_item.reason_codes == (
    "UPSTREAM_EXECUTION_REQUIRED",
    "VIRTUAL_MATERIALIZATION_REUSED",
  )
  assert serving_item.propagations[0].reason_code == (
    "UPSTREAM_EXECUTION_REQUIRED"
  )


def test_revalidation_does_not_propagate_data_execution() -> None:
  parent = _target("rawcore.parent")
  child = _target("bizcore.child")
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("rawcore.parent", decision="REVALIDATE"),
      _item("bizcore.child"),
    ),
    target_datasets=(parent, child),
    dependency_resolver=_resolver({
      "bizcore.child": (DummyDependency(parent, "lineage_input"),),
    }),
  )
  child_item = _items_by_key(result)["bizcore.child"]

  assert child_item.decision == "REUSE"
  assert child_item.reason_codes == ("NO_RELEVANT_CHANGE",)
  assert child_item.propagations == ()


def test_blocked_data_dependency_blocks_downstream_transitively() -> None:
  first = _target("raw.first")
  second = _target("stage.second")
  third = _target("rawcore.third")
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("raw.first", decision="BLOCKED"),
      _item("stage.second"),
      _item("rawcore.third"),
    ),
    target_datasets=(third, second, first),
    dependency_resolver=_resolver({
      "stage.second": (DummyDependency(first, "lineage_input"),),
      "rawcore.third": (DummyDependency(second, "lineage_input"),),
    }),
  )
  items = _items_by_key(result)

  assert items["stage.second"].decision == "BLOCKED"
  assert items["stage.second"].reason_codes == ("UPSTREAM_BLOCKED",)
  assert items["rawcore.third"].decision == "BLOCKED"
  assert items["rawcore.third"].reason_codes == ("UPSTREAM_BLOCKED",)


def test_reference_parent_execution_does_not_force_child_execution() -> None:
  parent = _target("rawcore.parent")
  child = _target("bizcore.child")
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("rawcore.parent", decision="FULL_REBUILD"),
      _item("bizcore.child"),
    ),
    target_datasets=(child, parent),
    dependency_resolver=_resolver({
      "bizcore.child": (
        DummyDependency(parent, "reference_parent_ready", reference_id=7),
      ),
    }),
  )
  child_item = _items_by_key(result)["bizcore.child"]

  assert child_item.decision == "REUSE"
  assert child_item.reason_codes == ("NO_RELEVANT_CHANGE",)
  assert child_item.propagations == ()
  assert child_item.upstream_dataset_keys == ("rawcore.parent",)


def test_blocked_reference_parent_does_not_block_reused_child() -> None:
  parent = _target("rawcore.parent")
  child = _target("bizcore.child")
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("rawcore.parent", decision="BLOCKED"),
      _item("bizcore.child"),
    ),
    target_datasets=(parent, child),
    dependency_resolver=_resolver({
      "bizcore.child": (
        DummyDependency(parent, "reference_parent_ready", reference_id=8),
      ),
    }),
  )

  assert _items_by_key(result)["bizcore.child"].decision == "REUSE"


def test_blocked_reference_parent_blocks_child_that_requires_execution() -> None:
  parent = _target("rawcore.parent")
  child = _target("bizcore.child")
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("rawcore.parent", decision="BLOCKED"),
      _item("bizcore.child", decision="FULL_REBUILD"),
    ),
    target_datasets=(parent, child),
    dependency_resolver=_resolver({
      "bizcore.child": (
        DummyDependency(parent, "reference_parent_ready", reference_id=9),
      ),
    }),
  )
  child_item = _items_by_key(result)["bizcore.child"]

  assert child_item.decision == "BLOCKED"
  assert child_item.reason_codes == (
    "DATASET_DEFINITION_CHANGED",
    "UPSTREAM_BLOCKED",
  )
  assert child_item.propagations[0].reason_code == "UPSTREAM_BLOCKED"


def test_reference_parent_is_checked_after_other_dependency_propagation() -> None:
  parent = _target("rawcore.parent")
  source = _target("raw.source")
  child = _target("bizcore.child")
  result = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("rawcore.parent", decision="BLOCKED"),
      _item("raw.source", decision="INCREMENTAL_EXECUTE"),
      _item("bizcore.child"),
    ),
    target_datasets=(child, source, parent),
    dependency_resolver=_resolver({
      "bizcore.child": (
        DummyDependency(parent, "reference_parent_ready", reference_id=10),
        DummyDependency(source, "lineage_input"),
      ),
    }),
  )
  child_item = _items_by_key(result)["bizcore.child"]

  assert child_item.decision == "BLOCKED"
  assert child_item.reason_codes == (
    "UPSTREAM_BLOCKED",
    "UPSTREAM_EXECUTION_REQUIRED",
  )


def test_missing_lineage_upstream_blocks_incomplete_scope() -> None:
  external = _target("raw.external")
  child = _target("stage.child")
  result = propagate_execution_impact_plan(
    local_plan=_plan(_item("stage.child")),
    target_datasets=(child,),
    dependency_resolver=_resolver({
      "stage.child": (DummyDependency(external, "lineage_input"),),
    }),
  )
  child_item = result.items[0]

  assert child_item.decision == "BLOCKED"
  assert child_item.reason_codes == ("MANUAL_REVIEW_REQUIRED",)
  missing = next(
    item
    for item in child_item.evidence
    if item.evidence_type == "execution_dependency"
  )
  assert missing.status == "unavailable"
  assert missing.required is True


def test_missing_reference_parent_is_not_required_for_reuse() -> None:
  external = _target("rawcore.external_parent")
  child = _target("bizcore.child")
  result = propagate_execution_impact_plan(
    local_plan=_plan(_item("bizcore.child")),
    target_datasets=(child,),
    dependency_resolver=_resolver({
      "bizcore.child": (
        DummyDependency(external, "reference_parent_ready", reference_id=11),
      ),
    }),
  )
  child_item = result.items[0]

  assert child_item.decision == "REUSE"
  dependency = next(
    item
    for item in child_item.evidence
    if item.evidence_type == "execution_dependency"
  )
  assert dependency.status == "available"
  assert dependency.required is False


def test_missing_reference_parent_blocks_required_execution() -> None:
  external = _target("rawcore.external_parent")
  child = _target("bizcore.child")
  result = propagate_execution_impact_plan(
    local_plan=_plan(_item("bizcore.child", decision="FULL_REBUILD")),
    target_datasets=(child,),
    dependency_resolver=_resolver({
      "bizcore.child": (
        DummyDependency(external, "reference_parent_ready", reference_id=12),
      ),
    }),
  )
  child_item = result.items[0]

  assert child_item.decision == "BLOCKED"
  assert child_item.reason_codes == (
    "DATASET_DEFINITION_CHANGED",
    "MANUAL_REVIEW_REQUIRED",
  )
  assert child_item.has_unavailable_required_evidence is True


def test_unknown_dependency_reason_blocks_instead_of_guessing() -> None:
  parent = _target("raw.parent")
  child = _target("stage.child")
  result = propagate_execution_impact_plan(
    local_plan=_plan(_item("raw.parent"), _item("stage.child")),
    target_datasets=(parent, child),
    dependency_resolver=_resolver({
      "stage.child": (DummyDependency(parent, "unknown_dependency"),),
    }),
  )
  child_item = _items_by_key(result)["stage.child"]

  assert child_item.decision == "BLOCKED"
  assert child_item.reason_codes == ("MANUAL_REVIEW_REQUIRED",)
  assert child_item.propagations[0].reason_code == "MANUAL_REVIEW_REQUIRED"


def test_dependency_resolution_error_blocks_affected_dataset() -> None:
  child = _target("stage.child")

  def failing_resolver(_target_dataset):
    raise RuntimeError("dependency lookup failed")

  result = propagate_execution_impact_plan(
    local_plan=_plan(_item("stage.child")),
    target_datasets=(child,),
    dependency_resolver=failing_resolver,
  )
  child_item = result.items[0]

  assert child_item.decision == "BLOCKED"
  assert child_item.reason_codes == ("MANUAL_REVIEW_REQUIRED",)
  assert child_item.has_unavailable_required_evidence is True


def test_dependency_cycle_is_rejected_explicitly() -> None:
  left = _target("stage.left")
  right = _target("stage.right")

  with pytest.raises(ValueError, match="Cycle detected"):
    propagate_execution_impact_plan(
      local_plan=_plan(_item("stage.left"), _item("stage.right")),
      target_datasets=(left, right),
      dependency_resolver=_resolver({
        "stage.left": (DummyDependency(right, "lineage_input"),),
        "stage.right": (DummyDependency(left, "lineage_input"),),
      }),
    )


def test_propagated_plan_is_deterministic_across_input_order() -> None:
  raw = _target("raw.customer")
  stage = _target("stage.customer")
  dependencies = {
    "stage.customer": (DummyDependency(raw, "lineage_input"),),
  }
  first = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("raw.customer", decision="INCREMENTAL_EXECUTE"),
      _item("stage.customer"),
    ),
    target_datasets=(raw, stage),
    dependency_resolver=_resolver(dependencies),
  )
  second = propagate_execution_impact_plan(
    local_plan=_plan(
      _item("stage.customer"),
      _item("raw.customer", decision="INCREMENTAL_EXECUTE"),
    ),
    target_datasets=(stage, raw),
    dependency_resolver=_resolver(dependencies),
  )

  assert first.plan_fingerprint == second.plan_fingerprint
  assert first.decision_counts == {
    "REUSE": 0,
    "REVALIDATE": 0,
    "INCREMENTAL_EXECUTE": 2,
    "FULL_REBUILD": 0,
    "BLOCKED": 0,
  }


def test_full_rebuild_upstream_keeps_historized_target_incremental() -> None:
  upstream = _target("rawcore.customer", incremental_strategy="full")
  hist = _target(
    "rawcore.customer_hist",
    incremental_strategy="historize",
  )
  plan = _plan(
    _item(
      "rawcore.customer",
      decision="FULL_REBUILD",
      reason_codes=("FULL_REFRESH_STRATEGY",),
    ),
    _item(
      "rawcore.customer_hist",
      decision="INCREMENTAL_EXECUTE",
      reason_codes=("INCREMENTAL_LOAD_STRATEGY",),
    ),
  )

  result = propagate_execution_impact_plan(
    local_plan=plan,
    target_datasets=(hist, upstream),
    dependency_resolver=_resolver({
      "rawcore.customer_hist": (
        DummyDependency(upstream, "lineage_input"),
      ),
    }),
  )
  item = _items_by_key(result)["rawcore.customer_hist"]

  assert item.decision == "INCREMENTAL_EXECUTE"
  assert item.reason_codes == (
    "INCREMENTAL_LOAD_STRATEGY",
    "UPSTREAM_EXECUTION_REQUIRED",
  )
  assert item.propagations[0].reason_code == (
    "UPSTREAM_EXECUTION_REQUIRED"
  )


def test_incremental_upstream_keeps_historized_target_incremental() -> None:
  upstream = _target("rawcore.customer", incremental_strategy="merge")
  hist = _target(
    "rawcore.customer_hist",
    incremental_strategy="historize",
  )
  plan = _plan(
    _item(
      "rawcore.customer",
      decision="INCREMENTAL_EXECUTE",
      reason_codes=("SOURCE_CHANGE_DETECTED",),
    ),
    _item(
      "rawcore.customer_hist",
      decision="INCREMENTAL_EXECUTE",
      reason_codes=("INCREMENTAL_LOAD_STRATEGY",),
    ),
  )

  result = propagate_execution_impact_plan(
    local_plan=plan,
    target_datasets=(hist, upstream),
    dependency_resolver=_resolver({
      "rawcore.customer_hist": (
        DummyDependency(upstream, "lineage_input"),
      ),
    }),
  )
  item = _items_by_key(result)["rawcore.customer_hist"]

  assert item.decision == "INCREMENTAL_EXECUTE"
  assert item.reason_codes == (
    "INCREMENTAL_LOAD_STRATEGY",
    "UPSTREAM_EXECUTION_REQUIRED",
  )


def test_target_only_assumes_resolved_external_upstream_readiness() -> None:
  upstream = _target("stage.customer", incremental_strategy="full")
  target = _target("rawcore.customer", incremental_strategy="merge")
  plan = _plan(
    _item(
      "rawcore.customer",
      decision="INCREMENTAL_EXECUTE",
      reason_codes=("INCREMENTAL_LOAD_STRATEGY",),
    ),
  )

  result = propagate_execution_impact_plan(
    local_plan=plan,
    target_datasets=(target, upstream),
    dependency_resolver=_resolver({
      "rawcore.customer": (
        DummyDependency(upstream, "lineage_input"),
      ),
    }),
    dependency_mode="target_only",
  )
  item = _items_by_key(result)["rawcore.customer"]

  assert item.decision == "INCREMENTAL_EXECUTE"
  assert item.reason_codes == ("INCREMENTAL_LOAD_STRATEGY",)
  assert item.has_unavailable_required_evidence is False
  assert any(
    evidence.required is False
    and "target-only execution" in (evidence.message or "")
    for evidence in item.evidence
  )
  assert any(
    evidence.evidence_key == "dependency_mode:all"
    and evidence.artifact_reference == "target_only"
    for evidence in result.evidence
  )
