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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from django.apps import apps
from django.db import transaction
from django.db.models import Model

from metadata.promotion.approval import (
  EnvironmentPromotionApprovalArtifact,
  EnvironmentPromotionApprovalError,
  require_environment_promotion_approval,
)
from metadata.promotion.contracts import (
  MetadataTransportRegistry,
)
from metadata.promotion.identities import build_metadata_object_identity
from metadata.promotion.model_contracts import METADATA_TRANSPORT_REGISTRY
from metadata.promotion.plan import (
  EnvironmentPromotionAction,
  EnvironmentPromotionActionType,
  EnvironmentPromotionPlan,
  EnvironmentPromotionReadinessStatus,
  EnvironmentPromotionSubjectType,
)
from metadata.promotion.planner import (
  EnvironmentPromotionPlanner,
  EnvironmentPromotionPlannerError,
)
from metadata.promotion.record import (
  EnvironmentPromotionActionResult,
  EnvironmentPromotionRecord,
)
from metadata.promotion.record_store import (
  EnvironmentPromotionRecordStore,
  EnvironmentPromotionRecordStoreError,
)
from metadata.promotion.release import ArchitectureReleaseBundle
from metadata.promotion.release_validation import (
  ArchitectureReleaseValidationError,
  require_valid_architecture_release_bundle,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataObject,
  EnvironmentMetadataRelationship,
  EnvironmentMetadataSnapshot,
)
from metadata.promotion.snapshot_builder import (
  EnvironmentMetadataSnapshotBuildError,
  EnvironmentMetadataSnapshotBuilder,
  MetadataIdentityResolver,
)
from metadata.transport_context import (
  metadata_artifact_reconstruction_context,
)


class EnvironmentPromotionApplyError(RuntimeError):
  """
  Raised when a guarded Environment Promotion Apply cannot complete.
  """


class EnvironmentPromotionDriftError(EnvironmentPromotionApplyError):
  """
  Raised when target metadata changed after promotion planning.
  """


class EnvironmentPromotionPostApplyValidationError(
  EnvironmentPromotionApplyError
):
  """
  Raised when the resulting target metadata does not converge to the release.
  """


class EnvironmentPromotionActionApplyError(EnvironmentPromotionApplyError):
  """
  Raised when one exact planned action cannot be consumed.
  """


class EnvironmentPromotionExecutor:
  """
  Consume one ready promotion plan against the locked local metadata graph.
  """

  def __init__(
    self,
    *,
    plan: EnvironmentPromotionPlan,
    pre_snapshot: EnvironmentMetadataSnapshot,
    registry: MetadataTransportRegistry = METADATA_TRANSPORT_REGISTRY,
  ) -> None:
    self.plan = plan
    self.pre_snapshot = pre_snapshot
    self.registry = registry
    self.instances = _build_metadata_instance_index(registry)
    self.aliases = _build_plan_object_aliases(
      plan=plan,
      pre_snapshot=pre_snapshot,
      registry=registry,
    )
    self._install_aliases()
    self._deferred: list[
      tuple[
        EnvironmentPromotionAction,
        Model,
        EnvironmentMetadataObject,
        tuple[str, ...],
      ]
    ] = []
    self._results: list[EnvironmentPromotionActionResult] = []
    self._recorded_action_ids: set[str] = set()

  def execute(self) -> tuple[EnvironmentPromotionActionResult, ...]:
    """
    Apply positive metadata, deferred links, relationships and removals.
    """
    verify_actions = tuple(
      item
      for item in self.plan.actions
      if item.action_type == EnvironmentPromotionActionType.VERIFY
    )
    positive_object_actions = tuple(
      item
      for item in self.plan.actions
      if (
        item.subject_type == EnvironmentPromotionSubjectType.OBJECT
        and item.action_type in {
          EnvironmentPromotionActionType.CREATE,
          EnvironmentPromotionActionType.UPDATE,
          EnvironmentPromotionActionType.REACTIVATE,
        }
      )
    )
    add_relationship_actions = tuple(
      item
      for item in self.plan.actions
      if item.action_type == EnvironmentPromotionActionType.ADD_RELATIONSHIP
    )
    negative_actions = tuple(
      item
      for item in self.plan.actions
      if item.action_type in {
        EnvironmentPromotionActionType.REMOVE_RELATIONSHIP,
        EnvironmentPromotionActionType.DEACTIVATE,
        EnvironmentPromotionActionType.RETIRE,
        EnvironmentPromotionActionType.DELETE,
      }
    )

    for action in verify_actions:
      self._consume_verify(action)
    for action in positive_object_actions:
      self._consume_positive_object(action)
    self._consume_deferred_fields()
    for action in add_relationship_actions:
      self._consume_add_relationship(action)
    for action in negative_actions:
      if action.subject_type == EnvironmentPromotionSubjectType.RELATIONSHIP:
        self._consume_remove_relationship(action)
      else:
        self._consume_negative_object(action)

    expected = {item.action_id for item in self.plan.actions}
    actual = {item.action_id for item in self._results}
    if actual != expected:
      missing = sorted(expected - actual)
      unexpected = sorted(actual - expected)
      details = []
      if missing:
        details.append("missing: " + ", ".join(missing))
      if unexpected:
        details.append("unexpected: " + ", ".join(unexpected))
      raise EnvironmentPromotionActionApplyError(
        "Promotion actions were not consumed exactly"
        + (": " + "; ".join(details) if details else ".")
      )
    return tuple(self._results)

  def _consume_verify(self, action: EnvironmentPromotionAction) -> None:
    if action.blocked:
      raise EnvironmentPromotionActionApplyError(
        f"Blocked VERIFY action cannot be applied: {action.action_id}."
      )
    if action.current is None or action.desired is None:
      raise EnvironmentPromotionActionApplyError(
        f"VERIFY action requires current and desired metadata: "
        f"{action.action_id}."
      )
    self._record(action)

  def _consume_positive_object(
    self,
    action: EnvironmentPromotionAction,
  ) -> None:
    desired = _require_action_object(action.desired, "desired")
    contract = self.registry.get_model(desired.model_name)

    if action.action_type == EnvironmentPromotionActionType.CREATE:
      model = apps.get_model("metadata", desired.model_name)
      instance = model()
      immediate_fields = tuple(sorted(
        contract.transport_fields - contract.deferred_fields
      ))
      self._assign_fields(
        instance=instance,
        desired=desired,
        field_names=immediate_fields,
        action=action,
      )
      self._save_transport_instance(instance=instance, action=action)
      self.instances[desired.object_key] = instance
      if contract.deferred_fields:
        self._deferred.append((
          action,
          instance,
          desired,
          tuple(sorted(contract.deferred_fields)),
        ))
      else:
        self._record(action)
      return

    current = _require_action_object(action.current, "current")
    instance = self._require_instance(
      current.object_key,
      expected_model=desired.model_name,
      action=action,
    )

    if action.action_type == EnvironmentPromotionActionType.UPDATE:
      immediate_fields = tuple(
        field_name
        for field_name in action.changed_fields
        if field_name not in contract.deferred_fields
      )
      deferred_fields = tuple(
        field_name
        for field_name in action.changed_fields
        if field_name in contract.deferred_fields
      )
      if immediate_fields:
        self._assign_fields(
          instance=instance,
          desired=desired,
          field_names=immediate_fields,
          action=action,
        )
        self._save_transport_instance(instance=instance, action=action)
      if action.desired_key:
        self.instances[action.desired_key] = instance
      if deferred_fields:
        self._deferred.append((
          action,
          instance,
          desired,
          deferred_fields,
        ))
      else:
        self._record(action)
      return

    if action.action_type == EnvironmentPromotionActionType.REACTIVATE:
      self._set_active(
        instance=instance,
        value=True,
        action=action,
      )
      if action.desired_key:
        self.instances[action.desired_key] = instance
      self._record(action)
      return

    raise EnvironmentPromotionActionApplyError(
      f"Unsupported positive object action: {action.action_type.value}."
    )

  def _consume_deferred_fields(self) -> None:
    for action, instance, desired, field_names in self._deferred:
      self._assign_fields(
        instance=instance,
        desired=desired,
        field_names=field_names,
        action=action,
      )
      self._save_transport_instance(instance=instance, action=action)
      if action.desired_key:
        self.instances[action.desired_key] = instance
      self._record(action)
    self._deferred.clear()

  def _consume_add_relationship(
    self,
    action: EnvironmentPromotionAction,
  ) -> None:
    relationship = _require_action_relationship(action.desired, "desired")
    contract = _relationship_contract(
      relationship.relationship_name,
      self.registry,
    )
    source = self._require_instance(
      relationship.source_key,
      expected_model=contract.source_model,
      action=action,
    )
    target = self._require_instance(
      relationship.target_key,
      expected_model=contract.target_model,
      action=action,
    )
    manager = getattr(source, contract.source_field)
    manager.add(target)
    if not manager.filter(pk=target.pk).exists():
      raise EnvironmentPromotionActionApplyError(
        f"Relationship was not added for action {action.action_id}."
      )
    self._record(action)

  def _consume_remove_relationship(
    self,
    action: EnvironmentPromotionAction,
  ) -> None:
    relationship = _require_action_relationship(action.current, "current")
    contract = _relationship_contract(
      relationship.relationship_name,
      self.registry,
    )
    source = self._require_instance(
      relationship.source_key,
      expected_model=contract.source_model,
      action=action,
    )
    target = self._require_instance(
      relationship.target_key,
      expected_model=contract.target_model,
      action=action,
    )
    manager = getattr(source, contract.source_field)
    manager.remove(target)
    if manager.filter(pk=target.pk).exists():
      raise EnvironmentPromotionActionApplyError(
        f"Relationship was not removed for action {action.action_id}."
      )
    self._record(action)

  def _consume_negative_object(
    self,
    action: EnvironmentPromotionAction,
  ) -> None:
    current = _require_action_object(action.current, "current")
    instance = self._require_instance(
      current.object_key,
      expected_model=current.model_name,
      action=action,
    )

    if action.action_type in {
      EnvironmentPromotionActionType.DEACTIVATE,
      EnvironmentPromotionActionType.RETIRE,
    }:
      self._set_active(
        instance=instance,
        value=False,
        action=action,
      )
      self._record(action)
      return

    if action.action_type == EnvironmentPromotionActionType.DELETE:
      model = type(instance)
      pk = instance.pk
      model._default_manager.filter(pk=pk).delete()
      if model._default_manager.filter(pk=pk).exists():
        raise EnvironmentPromotionActionApplyError(
          f"Metadata object was not deleted for action {action.action_id}."
        )
      self.instances = {
        key: value
        for key, value in self.instances.items()
        if not (type(value) is model and value.pk == pk)
      }
      self._record(action)
      return

    raise EnvironmentPromotionActionApplyError(
      f"Unsupported negative object action: {action.action_type.value}."
    )

  def _assign_fields(
    self,
    *,
    instance: Model,
    desired: EnvironmentMetadataObject,
    field_names: Sequence[str],
    action: EnvironmentPromotionAction,
  ) -> None:
    for field_name in field_names:
      try:
        field = instance._meta.get_field(field_name)
        raw_value = desired.fields[field_name]
        if field.is_relation:
          value = (
            None
            if raw_value is None
            else self._require_instance(
              str(raw_value),
              expected_model=field.remote_field.model.__name__,
              action=action,
            )
          )
        else:
          value = (
            None
            if raw_value is None
            else field.to_python(_thaw_value(raw_value))
          )
        setattr(instance, field_name, value)
      except EnvironmentPromotionActionApplyError:
        raise
      except Exception as exc:
        raise EnvironmentPromotionActionApplyError(
          f"Cannot assign {desired.model_name}.{field_name} for action "
          f"{action.action_id}: {exc}"
        ) from exc

  @staticmethod
  def _save_transport_instance(
    *,
    instance: Model,
    action: EnvironmentPromotionAction,
  ) -> None:
    """
    Persist approved artifact state without model-level mutation hooks.

    Environment Promotion reconstructs an immutable metadata artifact. Custom
    model save() overrides may derive additional metadata and therefore must not
    rewrite transported fields during CREATE/UPDATE. Lifecycle actions continue
    to use the normal model save path.
    """
    try:
      Model.save(instance)
    except Exception as exc:
      raise EnvironmentPromotionActionApplyError(
        f"Cannot save {instance.__class__.__name__} for action "
        f"{action.action_id}: {exc}"
      ) from exc

  @staticmethod
  def _save_instance(
    *,
    instance: Model,
    action: EnvironmentPromotionAction,
  ) -> None:
    try:
      instance.save()
    except Exception as exc:
      raise EnvironmentPromotionActionApplyError(
        f"Cannot save {instance.__class__.__name__} for action "
        f"{action.action_id}: {exc}"
      ) from exc

  def _set_active(
    self,
    *,
    instance: Model,
    value: bool,
    action: EnvironmentPromotionAction,
  ) -> None:
    try:
      instance._meta.get_field("active")
    except Exception as exc:
      raise EnvironmentPromotionActionApplyError(
        f"{instance.__class__.__name__} has no active lifecycle field for "
        f"action {action.action_id}."
      ) from exc
    instance.active = value
    self._save_instance(instance=instance, action=action)

  def _require_instance(
    self,
    object_key: str,
    *,
    expected_model: str,
    action: EnvironmentPromotionAction,
  ) -> Model:
    resolved_key = _resolve_alias(object_key, self.aliases)
    instance = self.instances.get(object_key) or self.instances.get(resolved_key)
    if instance is None:
      raise EnvironmentPromotionActionApplyError(
        f"Metadata object {object_key} is unavailable for action "
        f"{action.action_id}."
      )
    if instance.__class__.__name__ != expected_model:
      raise EnvironmentPromotionActionApplyError(
        f"Metadata object {object_key} is not a {expected_model} for action "
        f"{action.action_id}."
      )
    return instance

  def _install_aliases(self) -> None:
    for current_key, desired_key in self.aliases.items():
      instance = self.instances.get(current_key)
      if instance is None:
        continue
      existing = self.instances.get(desired_key)
      if existing is not None and existing is not instance:
        raise EnvironmentPromotionActionApplyError(
          f"Portable alias {desired_key} resolves to multiple local objects."
        )
      self.instances[desired_key] = instance

  def _record(self, action: EnvironmentPromotionAction) -> None:
    if action.action_id in self._recorded_action_ids:
      raise EnvironmentPromotionActionApplyError(
        f"Promotion action was consumed more than once: {action.action_id}."
      )
    self._recorded_action_ids.add(action.action_id)
    self._results.append(EnvironmentPromotionActionResult.from_action(action))


def apply_environment_promotion_plan(
  *,
  plan: EnvironmentPromotionPlan,
  bundle: ArchitectureReleaseBundle,
  approval: EnvironmentPromotionApprovalArtifact,
  applied_by: str,
  runtime_environment_label: str,
  applied_at: datetime | None = None,
  record_store: EnvironmentPromotionRecordStore | None = None,
  snapshot_builder: EnvironmentMetadataSnapshotBuilder | None = None,
  planner: EnvironmentPromotionPlanner | None = None,
  registry: MetadataTransportRegistry = METADATA_TRANSPORT_REGISTRY,
) -> EnvironmentPromotionRecord:
  """
  Apply one exact approved plan transactionally to the local metadata database.
  """
  actor = str(applied_by or "").strip()
  runtime_environment = str(runtime_environment_label or "").strip()
  if not actor:
    raise EnvironmentPromotionApplyError("Promotion apply actor is required.")
  if not runtime_environment:
    raise EnvironmentPromotionApplyError(
      "Promotion runtime environment label is required."
    )
  if runtime_environment != plan.target_environment_label:
    raise EnvironmentPromotionApplyError(
      "Environment Promotion Plan target does not match the local runtime "
      f"environment: expected {plan.target_environment_label}, received "
      f"{runtime_environment}."
    )
  timestamp = applied_at or datetime.now(timezone.utc)
  if timestamp.tzinfo is None:
    raise EnvironmentPromotionApplyError(
      "Promotion apply timestamp must be timezone-aware."
    )

  _validate_plan_bundle_binding(plan=plan, bundle=bundle)
  try:
    approved = require_environment_promotion_approval(
      plan=plan,
      approval=approval,
    )
  except EnvironmentPromotionApprovalError as exc:
    raise EnvironmentPromotionApplyError(str(exc)) from exc

  if plan.readiness.status != EnvironmentPromotionReadinessStatus.READY:
    raise EnvironmentPromotionApplyError(
      "Only a ready Environment Promotion Plan can be applied."
    )
  if any(item.blocked for item in plan.actions):
    raise EnvironmentPromotionApplyError(
      "Blocked Environment Promotion Plan actions cannot be applied."
    )

  store = record_store or EnvironmentPromotionRecordStore()
  builder = snapshot_builder or EnvironmentMetadataSnapshotBuilder(registry)
  plan_builder = planner or EnvironmentPromotionPlanner()
  record_path: Path | None = None
  record_path_existed = False

  try:
    with (
      metadata_artifact_reconstruction_context(),
      transaction.atomic(),
    ):
      _lock_portable_metadata(registry)
      pre_snapshot = _build_runtime_snapshot(
        builder=builder,
        environment_label=runtime_environment,
        actor=actor,
        timestamp=timestamp,
        phase="Pre-apply",
      )
      _require_unchanged_target(plan=plan, current_snapshot=pre_snapshot)

      executor = EnvironmentPromotionExecutor(
        plan=plan,
        pre_snapshot=pre_snapshot,
        registry=registry,
      )
      action_results = executor.execute()

      post_snapshot = _build_runtime_snapshot(
        builder=builder,
        environment_label=runtime_environment,
        actor=actor,
        timestamp=timestamp,
        phase="Post-apply",
      )
      try:
        post_plan = plan_builder.build(
          bundle=bundle,
          target_snapshot=post_snapshot,
          created_at=timestamp,
        )
      except EnvironmentPromotionPlannerError as exc:
        raise EnvironmentPromotionPostApplyValidationError(
          f"Post-apply promotion planning failed: {exc}"
        ) from exc
      _require_post_apply_convergence(post_plan)
      _require_exact_action_results(
        plan=plan,
        action_results=action_results,
      )

      record = EnvironmentPromotionRecord(
        plan_id=plan.plan_id,
        plan_fingerprint=plan.plan_fingerprint,
        approval_id=approved.approval_id,
        approval_fingerprint=approved.artifact_fingerprint,
        release_id=bundle.release_id,
        bundle_fingerprint=bundle.bundle_fingerprint,
        source_environment_label=plan.source_environment_label,
        target_environment_label=plan.target_environment_label,
        pre_target_snapshot_fingerprint=pre_snapshot.snapshot_fingerprint,
        pre_target_metadata_fingerprint=pre_snapshot.metadata_fingerprint,
        post_target_snapshot_fingerprint=post_snapshot.snapshot_fingerprint,
        post_target_metadata_fingerprint=post_snapshot.metadata_fingerprint,
        post_validation_plan_id=post_plan.plan_id,
        post_validation_plan_fingerprint=post_plan.plan_fingerprint,
        applied_at=timestamp,
        applied_by=actor,
        action_results=action_results,
      )
      record_path = store.record_file(record)
      record_path_existed = record_path.exists()
      try:
        store.save(record)
      except EnvironmentPromotionRecordStoreError as exc:
        raise EnvironmentPromotionApplyError(
          f"Promotion history could not be stored: {exc}"
        ) from exc
  except Exception:
    if record_path is not None and not record_path_existed:
      try:
        record_path.unlink(missing_ok=True)
      except OSError:
        pass
    raise

  return record


def _build_runtime_snapshot(
  *,
  builder: EnvironmentMetadataSnapshotBuilder,
  environment_label: str,
  actor: str,
  timestamp: datetime,
  phase: str,
) -> EnvironmentMetadataSnapshot:
  try:
    return builder.build(
      environment_label=environment_label,
      created_by=actor,
      created_at=timestamp,
    )
  except EnvironmentMetadataSnapshotBuildError as exc:
    raise EnvironmentPromotionApplyError(
      f"{phase} target metadata snapshot could not be built: {exc}"
    ) from exc


def _validate_plan_bundle_binding(
  *,
  plan: EnvironmentPromotionPlan,
  bundle: ArchitectureReleaseBundle,
) -> None:
  try:
    require_valid_architecture_release_bundle(bundle)
  except ArchitectureReleaseValidationError as exc:
    raise EnvironmentPromotionApplyError(str(exc)) from exc
  expected = {
    "release ID": (plan.release_id, bundle.release_id),
    "bundle fingerprint": (
      plan.bundle_fingerprint,
      bundle.bundle_fingerprint,
    ),
    "source environment": (
      plan.source_environment_label,
      bundle.source_environment_label,
    ),
    "source snapshot fingerprint": (
      plan.source_snapshot_fingerprint,
      bundle.source_snapshot_fingerprint,
    ),
    "source metadata fingerprint": (
      plan.source_metadata_fingerprint,
      bundle.metadata_fingerprint,
    ),
  }
  mismatches = [
    label
    for label, (actual, required) in expected.items()
    if actual != required
  ]
  if mismatches:
    raise EnvironmentPromotionApplyError(
      "Environment Promotion Plan does not match the release bundle: "
      + ", ".join(mismatches)
      + "."
    )


def _require_unchanged_target(
  *,
  plan: EnvironmentPromotionPlan,
  current_snapshot: EnvironmentMetadataSnapshot,
) -> None:
  if (
    current_snapshot.snapshot_fingerprint
    == plan.target_snapshot_fingerprint
    and current_snapshot.metadata_fingerprint
    == plan.target_metadata_fingerprint
  ):
    return
  raise EnvironmentPromotionDriftError(
    "Target metadata drift detected after promotion planning. Expected "
    f"snapshot {plan.target_snapshot_fingerprint}, received "
    f"{current_snapshot.snapshot_fingerprint}. Build and approve a new plan."
  )


def _require_post_apply_convergence(plan: EnvironmentPromotionPlan) -> None:
  if (
    plan.readiness.status == EnvironmentPromotionReadinessStatus.NO_CHANGES
    and not plan.mutating_actions
    and not any(item.blocked for item in plan.actions)
  ):
    return
  remaining = ", ".join(
    f"{item.action_type.value}:{item.subject_name}:"
    f"{item.desired_key or item.current_key or '?'}"
    for item in plan.mutating_actions[:10]
  )
  suffix = "" if len(plan.mutating_actions) <= 10 else ", ..."
  raise EnvironmentPromotionPostApplyValidationError(
    "Post-apply validation did not converge to a no-changes plan"
    + (f": {remaining}{suffix}." if remaining else ".")
  )


def _require_exact_action_results(
  *,
  plan: EnvironmentPromotionPlan,
  action_results: Sequence[EnvironmentPromotionActionResult],
) -> None:
  expected = {
    (item.action_id, item.action_fingerprint)
    for item in plan.actions
  }
  actual = {
    (item.action_id, item.action_fingerprint)
    for item in action_results
  }
  if expected != actual or len(action_results) != len(plan.actions):
    raise EnvironmentPromotionActionApplyError(
      "Promotion action results do not match the exact approved plan."
    )


def _lock_portable_metadata(registry: MetadataTransportRegistry) -> None:
  """
  Lock existing portable rows and implicit M2M rows for the apply transaction.
  """
  for contract in registry.creation_order:
    model = apps.get_model("metadata", contract.model_name)
    list(
      model._default_manager
      .select_for_update()
      .order_by("pk")
      .values_list("pk", flat=True)
    )
  locked_through_models = set()
  for relationship in registry.relationships:
    source_model = apps.get_model("metadata", relationship.source_model)
    field = source_model._meta.get_field(relationship.source_field)
    through_model = field.remote_field.through
    if through_model in locked_through_models:
      continue
    locked_through_models.add(through_model)
    list(
      through_model._default_manager
      .select_for_update()
      .order_by("pk")
      .values_list("pk", flat=True)
    )


def _build_metadata_instance_index(
  registry: MetadataTransportRegistry,
) -> dict[str, Model]:
  resolver = MetadataIdentityResolver(registry)
  instances: dict[str, Model] = {}
  for contract in registry.creation_order:
    model = apps.get_model("metadata", contract.model_name)
    queryset = model._default_manager.all()
    for field_name, expected_value in contract.transport_filter:
      queryset = queryset.filter(**{field_name: expected_value})
    for instance in queryset:
      key = resolver.object_key_for(instance)
      if key is None:
        continue
      existing = instances.get(key)
      if existing is not None and existing.pk != instance.pk:
        raise EnvironmentPromotionActionApplyError(
          f"Portable object key resolves to multiple local rows: {key}."
        )
      instances[key] = instance
  return instances


def _build_plan_object_aliases(
  *,
  plan: EnvironmentPromotionPlan,
  pre_snapshot: EnvironmentMetadataSnapshot,
  registry: MetadataTransportRegistry,
) -> dict[str, str]:
  aliases = {
    item.current_key: item.desired_key
    for item in plan.actions
    if (
      item.subject_type == EnvironmentPromotionSubjectType.OBJECT
      and item.current_key
      and item.desired_key
      and item.current_key != item.desired_key
    )
  }
  objects = {
    item.object_key: item
    for item in pre_snapshot.metadata.objects
  }
  while True:
    changed = False
    for current_key, current in objects.items():
      if current_key in aliases:
        continue
      resolved_current = _resolve_alias(current_key, aliases)
      values = {
        name: _replace_aliases(value, aliases)
        for name, value in current.identity.components
      }
      contract = registry.get_model(current.model_name)
      try:
        normalized_key = build_metadata_object_identity(
          model_name=current.model_name,
          contract=contract.identity,
          values=values,
        ).object_key
      except ValueError:
        continue
      normalized_key = _resolve_alias(normalized_key, aliases)
      if normalized_key == resolved_current:
        continue
      existing_source = next(
        (
          source
          for source, target in aliases.items()
          if target == normalized_key and source != current_key
        ),
        None,
      )
      if existing_source is not None:
        raise EnvironmentPromotionActionApplyError(
          "Portable aliases are ambiguous for desired object key "
          f"{normalized_key}."
        )
      aliases[current_key] = normalized_key
      changed = True
    if not changed:
      return aliases


def _relationship_contract(
  relationship_name: str,
  registry: MetadataTransportRegistry,
):
  try:
    return next(
      item
      for item in registry.relationships
      if item.name == relationship_name
    )
  except StopIteration as exc:
    raise EnvironmentPromotionActionApplyError(
      f"Unknown portable relationship: {relationship_name}."
    ) from exc


def _require_action_object(
  payload: Mapping[str, Any] | None,
  label: str,
) -> EnvironmentMetadataObject:
  if payload is None:
    raise EnvironmentPromotionActionApplyError(
      f"Promotion action {label} object is missing."
    )
  try:
    return EnvironmentMetadataObject.from_dict(payload)
  except Exception as exc:
    raise EnvironmentPromotionActionApplyError(
      f"Promotion action {label} object is invalid: {exc}"
    ) from exc


def _require_action_relationship(
  payload: Mapping[str, Any] | None,
  label: str,
) -> EnvironmentMetadataRelationship:
  if payload is None:
    raise EnvironmentPromotionActionApplyError(
      f"Promotion action {label} relationship is missing."
    )
  try:
    return EnvironmentMetadataRelationship.from_dict(payload)
  except Exception as exc:
    raise EnvironmentPromotionActionApplyError(
      f"Promotion action {label} relationship is invalid: {exc}"
    ) from exc


def _thaw_value(value: Any) -> Any:
  if isinstance(value, Mapping):
    return {key: _thaw_value(item) for key, item in value.items()}
  if isinstance(value, tuple):
    return [_thaw_value(item) for item in value]
  if isinstance(value, list):
    return [_thaw_value(item) for item in value]
  return value


def _replace_aliases(value: Any, aliases: Mapping[str, str]) -> Any:
  if isinstance(value, str):
    return _resolve_alias(value, aliases)
  if isinstance(value, Mapping):
    return {
      key: _replace_aliases(item, aliases)
      for key, item in value.items()
    }
  if isinstance(value, tuple):
    return tuple(_replace_aliases(item, aliases) for item in value)
  if isinstance(value, list):
    return [_replace_aliases(item, aliases) for item in value]
  return value


def _resolve_alias(value: str, aliases: Mapping[str, str]) -> str:
  current = value
  seen = set()
  while current in aliases:
    if current in seen:
      raise EnvironmentPromotionActionApplyError(
        f"Cyclic portable object alias detected for {value}."
      )
    seen.add(current)
    current = aliases[current]
  return current
