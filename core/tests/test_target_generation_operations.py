"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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
from types import SimpleNamespace

import pytest

from metadata.generation.target_generation_control import (
  build_target_generation_approval,
  build_target_generation_review,
)
from metadata.generation.target_generation_operations import (
  TargetGenerationOperationsError,
  apply_target_generation_operations_plan,
  build_target_generation_operations_context,
  build_target_generation_sequence_context,
  create_target_generation_operations_approval,
)
from metadata.generation.target_generation_plan import (
  TargetGenerationAction,
  build_target_generation_plan,
)


class FakeApprovalStore:
  """In-memory Generation Approval store for operations tests."""

  def __init__(self):
    self.base_path = Path(".elevata/approvals/dev/dwh/generation")
    self.artifacts = {}

  def load_for_review_fingerprint(self, review_fingerprint):
    return self.artifacts.get(review_fingerprint)

  def save(self, artifact):
    review_fingerprint = artifact.review["review_fingerprint"]
    self.artifacts[review_fingerprint] = artifact
    return self.base_path / f"{review_fingerprint}.generation.approval.json"


class FakeTargetDatasetManager:
  """Related-manager test double for human-readable target labels."""

  def __init__(self, dataset, *, matches=True):
    self.dataset = dataset
    self.matches = matches

  def filter(self, **kwargs):
    matches = self.matches
    if "lineage_key" in kwargs:
      matches = matches and (
        kwargs["lineage_key"] == getattr(self.dataset, "lineage_key", None)
      )
    if "pk" in kwargs:
      matches = matches and (
        kwargs["pk"] == getattr(self.dataset, "pk", None)
      )
    if "target_dataset_name__endswith" in kwargs:
      matches = matches and str(
        getattr(self.dataset, "target_dataset_name", "")
      ).endswith(kwargs["target_dataset_name__endswith"])
    return FakeTargetDatasetManager(self.dataset, matches=matches)

  def exclude(self, **kwargs):
    matches = self.matches
    if "target_dataset_name__endswith" in kwargs:
      matches = matches and not str(
        getattr(self.dataset, "target_dataset_name", "")
      ).endswith(kwargs["target_dataset_name__endswith"])
    return FakeTargetDatasetManager(self.dataset, matches=matches)

  def first(self):
    return self.dataset if self.matches else None


class FakeGenerationService:
  """Generation-service test double with optional post-apply residual plan."""

  def __init__(self, plan, *, residual_plan=None):
    self.plan = plan
    self.residual_plan = residual_plan or plan
    self.applied = False
    self.apply_calls = []
    target_dataset = SimpleNamespace(
      pk=91,
      lineage_key="1:23",
      target_dataset_name="raw_crm_customer",
    )
    self.schema = SimpleNamespace(
      short_name="raw",
      physical_prefix="raw",
      target_datasets=FakeTargetDatasetManager(target_dataset),
    )

  def get_target_schemas_in_scope(self):
    return (self.schema,)

  def get_eligible_source_datasets_for_schema(self, target_schema):
    assert target_schema is self.schema
    return (
      SimpleNamespace(
        pk=23,
        source_system=SimpleNamespace(short_name="crm"),
        schema_name="sales",
        source_dataset_name="customer",
      ),
    )

  def build_plan(self, eligible, target_schema, *, reconcile_lifecycle):
    assert tuple(eligible)
    assert target_schema is self.schema
    assert reconcile_lifecycle is True
    return self.residual_plan if self.applied else self.plan

  def apply_plan(self, plan, *, approval=None, require_approval=False):
    self.apply_calls.append({
      "plan": plan,
      "approval": approval,
      "require_approval": require_approval,
    })
    self.applied = True
    return SimpleNamespace(
      summary_text=(
        f"Applied Target Generation Plan {plan.plan_fingerprint[:12]}: "
        f"{plan.action_count} planned actions consumed; "
        f"{self.residual_plan.action_count} residual actions."
      ),
      plan_fingerprint=plan.plan_fingerprint,
      generation_review_fingerprint=(
        build_target_generation_review(plan).review_fingerprint
      ),
      generation_approval_id=(
        approval.approval_id if approval is not None else None
      ),
      source_metadata_fingerprint=plan.source_metadata_fingerprint,
      target_metadata_fingerprint_before=plan.target_metadata_fingerprint,
      target_metadata_fingerprint_after="3" * 64,
      planned_action_count=plan.action_count,
      consumed_action_count=plan.action_count,
      residual_action_count=self.residual_plan.action_count,
      residual_plan_fingerprint=self.residual_plan.plan_fingerprint,
      processed_dataset_count=1,
      processed_column_count=1,
      retired_dataset_count=0,
      reactivated_dataset_count=0,
      converged=self.residual_plan.action_count == 0,
    )


def _plan(*, length=110, classification="BREAKING", actions=True):
  planned_actions = ()
  if actions:
    planned_actions = (
      TargetGenerationAction(
        action_type="UPDATE_TARGET_COLUMN",
        dataset_key="raw:1:23:base",
        object_key="raw:1:23:base:column:source_column:158",
        effect_origin="DIRECT",
        change_classification=classification,
        source_keys=("source_dataset:23",),
        before={
          "target_column_name": "name",
          "max_length": 100,
        },
        after={
          "target_column_name": "name",
          "max_length": length,
        },
        reason="Generated column follows Source Metadata.",
      ),
    )
  return build_target_generation_plan(
    scope_mode="schema",
    target_schema_short_names=("raw",),
    source_dataset_keys=("source_dataset:23",),
    reconcile_lifecycle=True,
    source_metadata_fingerprint="1" * 64,
    target_metadata_fingerprint="2" * 64,
    actions=planned_actions,
  )


def test_generation_operations_context_exposes_review_and_missing_approval():
  plan = _plan()
  store = FakeApprovalStore()

  context = build_target_generation_operations_context(
    "raw",
    approval_store=store,
    service=FakeGenerationService(plan),
  )

  assert context.plan is plan
  assert context.review.plan_fingerprint == plan.plan_fingerprint
  assert context.approval_check.status == "missing"
  assert context.approval_required is True
  assert context.can_apply is False
  assert context.classification_counts == {
    "BREAKING": 1,
    "ADDITIVE": 0,
    "NEUTRAL": 0,
  }
  impact = context.dataset_impact_preview[0]
  assert impact.target_dataset_label == "raw.raw_crm_customer"
  assert impact.dataset_key == "raw:1:23:base"
  assert impact.sources[0].label == "crm · sales.customer"
  assert impact.sources[0].source_key == "source_dataset:23"
  action = context.action_preview[0]
  assert action.action_type_label == "Update target column"
  assert action.object_label == "raw.raw_crm_customer.name"
  assert action.before_json.find('"max_length": 100') >= 0
  assert action.after_json.find('"max_length": 110') >= 0


def test_generation_action_preview_hides_internal_keys_for_input_sync():
  plan = build_target_generation_plan(
    scope_mode="schema",
    target_schema_short_names=("raw",),
    source_dataset_keys=("source_dataset:23",),
    reconcile_lifecycle=True,
    source_metadata_fingerprint="1" * 64,
    target_metadata_fingerprint="2" * 64,
    actions=(
      TargetGenerationAction(
        action_type="SYNC_TARGET_COLUMN_INPUTS",
        dataset_key="raw:1:23:base",
        object_key="raw:1:23:base:column:source_column:158",
        effect_origin="DIRECT",
        change_classification="BREAKING",
        source_keys=("source_dataset:23",),
        before={"inputs": []},
        after={
          "inputs": [{
            "source_key": "source_column:158",
          }],
        },
        reason="Column-level lineage synchronized.",
      ),
    ),
  )

  context = build_target_generation_operations_context(
    "raw",
    approval_store=FakeApprovalStore(),
    service=FakeGenerationService(plan),
  )

  action = context.action_preview[0]
  assert action.action_type_label == "Synchronize column inputs"
  assert action.object_label == "Column in raw.raw_crm_customer"
  assert "source_column:158" not in action.object_label


def test_generation_operations_approval_is_bound_to_visible_review():
  plan = _plan()
  store = FakeApprovalStore()
  service = FakeGenerationService(plan)
  review = build_target_generation_review(plan)

  result = create_target_generation_operations_approval(
    "raw",
    expected_review_fingerprint=review.review_fingerprint,
    approved_by="Ilona Tag",
    note="Reviewed in Architecture Control.",
    approval_store=store,
    service=service,
  )

  assert result.artifact.review["plan_fingerprint"] == plan.plan_fingerprint
  assert result.artifact.review["review_fingerprint"] == review.review_fingerprint
  assert store.load_for_review_fingerprint(review.review_fingerprint) is result.artifact

  with pytest.raises(
    TargetGenerationOperationsError,
    match="preview changed",
  ):
    create_target_generation_operations_approval(
      "raw",
      expected_review_fingerprint="f" * 64,
      approved_by="Ilona Tag",
      approval_store=store,
      service=service,
    )


def test_breaking_generation_apply_requires_and_consumes_exact_approval():
  plan = _plan()
  residual_plan = _plan(actions=False)
  store = FakeApprovalStore()
  review = build_target_generation_review(plan)
  approval = build_target_generation_approval(
    review=review,
    decided_by="Ilona Tag",
    decided_at="2026-07-31T04:00:00Z",
  )
  store.save(approval)
  service = FakeGenerationService(plan, residual_plan=residual_plan)

  result = apply_target_generation_operations_plan(
    "raw",
    expected_review_fingerprint=review.review_fingerprint,
    approval_store=store,
    service=service,
  )

  assert service.apply_calls == [{
    "plan": plan,
    "approval": approval,
    "require_approval": True,
  }]
  assert result.apply_result.generation_approval_id == approval.approval_id
  assert result.residual_context.plan is residual_plan
  assert result.residual_context.review.action_count == 0


def test_breaking_generation_apply_rejects_missing_approval():
  plan = _plan()
  service = FakeGenerationService(plan)
  review = build_target_generation_review(plan)

  with pytest.raises(
    TargetGenerationOperationsError,
    match="matching Generation Approval is required",
  ):
    apply_target_generation_operations_plan(
      "raw",
      expected_review_fingerprint=review.review_fingerprint,
      approval_store=FakeApprovalStore(),
      service=service,
    )

  assert service.apply_calls == []


def test_neutral_generation_apply_remains_guarded_without_mandatory_approval():
  plan = _plan(classification="NEUTRAL")
  residual_plan = _plan(classification="NEUTRAL", actions=False)
  store = FakeApprovalStore()
  service = FakeGenerationService(plan, residual_plan=residual_plan)
  review = build_target_generation_review(plan)

  result = apply_target_generation_operations_plan(
    "raw",
    expected_review_fingerprint=review.review_fingerprint,
    approval_store=store,
    service=service,
  )

  assert service.apply_calls[0]["approval"] is None
  assert service.apply_calls[0]["require_approval"] is False
  assert result.apply_result.generation_approval_id is None
  assert result.apply_result.converged is True


def test_invalid_stored_approval_blocks_neutral_guarded_apply():
  plan = _plan(classification="NEUTRAL")
  different_plan = _plan(length=120, classification="NEUTRAL")
  review = build_target_generation_review(plan)
  different_approval = build_target_generation_approval(
    review=build_target_generation_review(different_plan),
    decided_by="Ilona Tag",
    decided_at="2026-07-31T04:00:00Z",
  )
  store = FakeApprovalStore()
  store.artifacts[review.review_fingerprint] = different_approval
  service = FakeGenerationService(plan)

  context = build_target_generation_operations_context(
    "raw",
    approval_store=store,
    service=service,
  )

  assert context.approval_invalid is True
  assert context.can_apply is False

  with pytest.raises(
    TargetGenerationOperationsError,
    match="stored Generation Approval is invalid",
  ):
    apply_target_generation_operations_plan(
      "raw",
      expected_review_fingerprint=review.review_fingerprint,
      approval_store=store,
      service=service,
    )

  assert service.apply_calls == []


def test_generation_operations_reject_non_generated_schema_scope():
  with pytest.raises(
    TargetGenerationOperationsError,
    match="raw, stage, rawcore",
  ):
    build_target_generation_operations_context(
      "serving",
      approval_store=FakeApprovalStore(),
      service=FakeGenerationService(_plan()),
    )


class FakeSequenceGenerationService:
  """Generation-service test double for ordered layer guidance."""

  def __init__(self, plans):
    self.plans = dict(plans)
    self.apply_calls = []
    self.schemas = {}
    for index, schema_short_name in enumerate(("raw", "stage", "rawcore"), start=1):
      target_dataset = SimpleNamespace(
        pk=90 + index,
        lineage_key="1:23",
        target_dataset_name=f"{schema_short_name}_crm_customer",
      )
      self.schemas[schema_short_name] = SimpleNamespace(
        short_name=schema_short_name,
        physical_prefix=schema_short_name,
        target_datasets=FakeTargetDatasetManager(target_dataset),
      )

  def get_target_schemas_in_scope(self):
    return tuple(self.schemas.values())

  def get_eligible_source_datasets_for_schema(self, target_schema):
    return (
      SimpleNamespace(
        pk=23,
        source_system=SimpleNamespace(short_name="crm"),
        schema_name="sales",
        source_dataset_name="customer",
      ),
    )

  def build_plan(self, eligible, target_schema, *, reconcile_lifecycle):
    assert tuple(eligible)
    assert reconcile_lifecycle is True
    return self.plans[target_schema.short_name]

  def apply_plan(self, *args, **kwargs):
    self.apply_calls.append((args, kwargs))
    raise AssertionError("A provisional downstream plan must not be applied.")


def _schema_plan(schema_short_name, *, actions=True):
  planned_actions = ()
  if actions:
    planned_actions = (
      TargetGenerationAction(
        action_type="UPDATE_TARGET_COLUMN",
        dataset_key=f"{schema_short_name}:1:23:base",
        object_key=(
          f"{schema_short_name}:1:23:base:column:source_column:158"
        ),
        effect_origin="DIRECT",
        change_classification="NEUTRAL",
        source_keys=("source_dataset:23",),
        before={
          "target_column_name": "name",
          "max_length": 100,
        },
        after={
          "target_column_name": "name",
          "max_length": 110,
        },
        reason="Generated column follows Source Metadata.",
      ),
    )
  return build_target_generation_plan(
    scope_mode="schema",
    target_schema_short_names=(schema_short_name,),
    source_dataset_keys=("source_dataset:23",),
    reconcile_lifecycle=True,
    source_metadata_fingerprint="1" * 64,
    target_metadata_fingerprint="2" * 64,
    actions=planned_actions,
  )


def test_generation_sequence_surfaces_first_pending_downstream_layer():
  service = FakeSequenceGenerationService({
    "raw": _schema_plan("raw", actions=False),
    "stage": _schema_plan("stage"),
    "rawcore": _schema_plan("rawcore"),
  })

  sequence = build_target_generation_sequence_context(
    selected_schema_short_name="raw",
    service=service,
  )

  assert sequence.next_actionable_schema_short_name == "stage"
  assert sequence.continuation_required is True
  assert [item.status for item in sequence.items] == [
    "up_to_date",
    "review",
    "recalculate",
  ]
  assert sequence.items[1].action_count == 1
  assert sequence.items[1].is_actionable is True
  assert sequence.items[2].blocked_by_schema_short_name == "stage"


def test_downstream_generation_context_is_blocked_by_pending_upstream_layer():
  stage_plan = _schema_plan("stage")
  service = FakeSequenceGenerationService({
    "raw": _schema_plan("raw"),
    "stage": stage_plan,
    "rawcore": _schema_plan("rawcore", actions=False),
  })
  store = FakeApprovalStore()

  context = build_target_generation_operations_context(
    "stage",
    approval_store=store,
    service=service,
  )

  assert context.upstream_pending_schema_short_name == "raw"
  assert context.upstream_ready is False
  assert context.can_approve is False
  assert context.can_apply is False

  with pytest.raises(
    TargetGenerationOperationsError,
    match="RAW generation must converge",
  ):
    apply_target_generation_operations_plan(
      "stage",
      expected_review_fingerprint=context.review.review_fingerprint,
      approval_store=store,
      service=service,
    )

  assert service.apply_calls == []


def test_generation_sequence_reports_all_layers_converged():
  service = FakeSequenceGenerationService({
    "raw": _schema_plan("raw", actions=False),
    "stage": _schema_plan("stage", actions=False),
    "rawcore": _schema_plan("rawcore", actions=False),
  })

  sequence = build_target_generation_sequence_context(service=service)

  assert sequence.all_up_to_date is True
  assert sequence.next_actionable_item is None
  assert all(item.status == "up_to_date" for item in sequence.items)
