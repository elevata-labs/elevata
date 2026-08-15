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
from typing import Any

from metadata.promotion.contracts import (
  MetadataLifecycleStrategy,
  MetadataManagedApplyMode,
)
from metadata.promotion.identities import build_metadata_object_identity
from metadata.promotion.model_contracts import METADATA_TRANSPORT_REGISTRY
from metadata.promotion.plan import (
  EnvironmentPromotionAction,
  EnvironmentPromotionActionType,
  EnvironmentPromotionIssue,
  EnvironmentPromotionIssueSeverity,
  EnvironmentPromotionPlan,
  EnvironmentPromotionPlanError,
  EnvironmentPromotionSubjectType,
  derive_environment_promotion_readiness,
)
from metadata.promotion.release import ArchitectureReleaseBundle
from metadata.promotion.release_validation import (
  require_valid_architecture_release_bundle,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataObject,
  EnvironmentMetadataRelationship,
  EnvironmentMetadataSnapshot,
)


class EnvironmentPromotionPlannerError(RuntimeError):
  """
  Raised when two valid artifacts cannot form an unambiguous plan.
  """


class EnvironmentPromotionPlanner:
  """
  Compare one immutable release with one target environment snapshot.
  """

  def build(
    self,
    *,
    bundle: ArchitectureReleaseBundle,
    target_snapshot: EnvironmentMetadataSnapshot,
    created_at: datetime | None = None,
  ) -> EnvironmentPromotionPlan:
    require_valid_architecture_release_bundle(bundle)
    if not isinstance(target_snapshot, EnvironmentMetadataSnapshot):
      raise EnvironmentPromotionPlannerError(
        "target_snapshot must be an EnvironmentMetadataSnapshot."
      )

    desired_objects = {
      item.object_key: item
      for item in bundle.snapshot.metadata.objects
    }
    current_objects = {
      item.object_key: item
      for item in target_snapshot.metadata.objects
    }
    issues: list[EnvironmentPromotionIssue] = []
    matches, aliases = self._match_objects(
      desired_objects=desired_objects,
      current_objects=current_objects,
      issues=issues,
    )
    actions = self._build_object_actions(
      desired_objects=desired_objects,
      current_objects=current_objects,
      matches=matches,
      aliases=aliases,
      issues=issues,
    )
    actions.extend(self._build_relationship_actions(
      desired_relationships=bundle.snapshot.metadata.relationships,
      current_relationships=target_snapshot.metadata.relationships,
      aliases=aliases,
      issues=issues,
    ))

    if bundle.source_environment_label == target_snapshot.environment_label:
      issues.append(EnvironmentPromotionIssue(
        severity=EnvironmentPromotionIssueSeverity.WARNING,
        code="source_and_target_environment_match",
        message=(
          "Source and target snapshots use the same environment label. "
          "The plan remains reviewable but does not prove cross-environment "
          "promotion."
        ),
      ))

    readiness = derive_environment_promotion_readiness(
      actions=actions,
      issues=issues,
    )
    try:
      return EnvironmentPromotionPlan(
        release_id=bundle.release_id,
        bundle_fingerprint=bundle.bundle_fingerprint,
        source_environment_label=bundle.source_environment_label,
        source_snapshot_fingerprint=bundle.source_snapshot_fingerprint,
        source_metadata_fingerprint=bundle.metadata_fingerprint,
        target_environment_label=target_snapshot.environment_label,
        target_snapshot_fingerprint=target_snapshot.snapshot_fingerprint,
        target_metadata_fingerprint=target_snapshot.metadata_fingerprint,
        created_at=created_at or datetime.now(timezone.utc),
        actions=tuple(actions),
        readiness=readiness,
      )
    except EnvironmentPromotionPlanError as exc:
      raise EnvironmentPromotionPlannerError(str(exc)) from exc

  def _match_objects(
    self,
    *,
    desired_objects: Mapping[str, EnvironmentMetadataObject],
    current_objects: Mapping[str, EnvironmentMetadataObject],
    issues: list[EnvironmentPromotionIssue],
  ) -> tuple[dict[str, str], dict[str, str]]:
    """
    Return desired-to-current matches and current-to-desired key aliases.
    """
    matches = {
      key: key
      for key in desired_objects
      if key in current_objects
    }
    aliases = {
      current_key: desired_key
      for desired_key, current_key in matches.items()
      if current_key != desired_key
    }

    self._match_target_dataset_former_names(
      desired_objects=desired_objects,
      current_objects=current_objects,
      matches=matches,
      aliases=aliases,
      issues=issues,
    )
    self._match_normalized_identities(
      desired_objects=desired_objects,
      current_objects=current_objects,
      matches=matches,
      aliases=aliases,
      issues=issues,
    )
    self._match_target_column_former_names(
      desired_objects=desired_objects,
      current_objects=current_objects,
      matches=matches,
      aliases=aliases,
      issues=issues,
    )
    self._match_normalized_identities(
      desired_objects=desired_objects,
      current_objects=current_objects,
      matches=matches,
      aliases=aliases,
      issues=issues,
    )
    return matches, aliases

  def _match_target_dataset_former_names(
    self,
    *,
    desired_objects: Mapping[str, EnvironmentMetadataObject],
    current_objects: Mapping[str, EnvironmentMetadataObject],
    matches: dict[str, str],
    aliases: dict[str, str],
    issues: list[EnvironmentPromotionIssue],
  ) -> None:
    self._match_former_names(
      model_name="TargetDataset",
      parent_field="target_schema",
      name_field="target_dataset_name",
      desired_objects=desired_objects,
      current_objects=current_objects,
      matches=matches,
      aliases=aliases,
      issues=issues,
    )

  def _match_target_column_former_names(
    self,
    *,
    desired_objects: Mapping[str, EnvironmentMetadataObject],
    current_objects: Mapping[str, EnvironmentMetadataObject],
    matches: dict[str, str],
    aliases: dict[str, str],
    issues: list[EnvironmentPromotionIssue],
  ) -> None:
    self._match_former_names(
      model_name="TargetColumn",
      parent_field="target_dataset",
      name_field="target_column_name",
      desired_objects=desired_objects,
      current_objects=current_objects,
      matches=matches,
      aliases=aliases,
      issues=issues,
    )

  def _match_former_names(
    self,
    *,
    model_name: str,
    parent_field: str,
    name_field: str,
    desired_objects: Mapping[str, EnvironmentMetadataObject],
    current_objects: Mapping[str, EnvironmentMetadataObject],
    matches: dict[str, str],
    aliases: dict[str, str],
    issues: list[EnvironmentPromotionIssue],
  ) -> None:
    used_current = set(matches.values())
    desired_candidates: dict[str, list[str]] = {}

    for desired_key, desired in desired_objects.items():
      if desired.model_name != model_name or desired_key in matches:
        continue
      former_names = {
        str(value).strip()
        for value in (desired.fields.get("former_names") or ())
        if str(value).strip()
      }
      if not former_names:
        continue
      desired_parent = desired.fields.get(parent_field)
      candidates = []
      for current_key, current in current_objects.items():
        if (
          current.model_name != model_name
          or current_key in used_current
        ):
          continue
        current_parent = _replace_aliases(
          current.fields.get(parent_field),
          aliases,
        )
        current_name = str(current.fields.get(name_field) or "").strip()
        if current_parent == desired_parent and current_name in former_names:
          candidates.append(current_key)
      if candidates:
        desired_candidates[desired_key] = sorted(candidates)

    reverse_candidates: dict[str, list[str]] = {}
    for desired_key, candidates in desired_candidates.items():
      for current_key in candidates:
        reverse_candidates.setdefault(current_key, []).append(desired_key)

    for desired_key in sorted(desired_candidates):
      candidates = desired_candidates[desired_key]
      if (
        len(candidates) == 1
        and len(reverse_candidates[candidates[0]]) == 1
      ):
        current_key = candidates[0]
        matches[desired_key] = current_key
        if current_key != desired_key:
          aliases[current_key] = desired_key
        used_current.add(current_key)
        continue

      issues.append(EnvironmentPromotionIssue(
        severity=EnvironmentPromotionIssueSeverity.ERROR,
        code="ambiguous_former_name_match",
        message=(
          f"{model_name} former_names do not identify exactly one target "
          f"object; candidates: {', '.join(candidates)}."
        ),
        subject_name=model_name,
        desired_key=desired_key,
      ))

  def _match_normalized_identities(
    self,
    *,
    desired_objects: Mapping[str, EnvironmentMetadataObject],
    current_objects: Mapping[str, EnvironmentMetadataObject],
    matches: dict[str, str],
    aliases: dict[str, str],
    issues: list[EnvironmentPromotionIssue],
  ) -> None:
    reported: set[tuple[str, tuple[str, ...]]] = set()
    while True:
      used_current = set(matches.values())
      proposals: dict[str, list[str]] = {}
      for current_key, current in current_objects.items():
        if current_key in used_current:
          continue
        normalized_key = self._normalized_object_key(current, aliases)
        desired = desired_objects.get(normalized_key)
        if (
          desired is None
          or normalized_key in matches
          or desired.model_name != current.model_name
        ):
          continue
        proposals.setdefault(normalized_key, []).append(current_key)

      progressed = False
      for desired_key in sorted(proposals):
        candidates = sorted(proposals[desired_key])
        if len(candidates) == 1:
          current_key = candidates[0]
          matches[desired_key] = current_key
          if current_key != desired_key:
            aliases[current_key] = desired_key
          progressed = True
          continue
        marker = (desired_key, tuple(candidates))
        if marker in reported:
          continue
        reported.add(marker)
        issues.append(EnvironmentPromotionIssue(
          severity=EnvironmentPromotionIssueSeverity.ERROR,
          code="ambiguous_normalized_identity_match",
          message=(
            "Multiple current objects normalize to one desired identity: "
            + ", ".join(candidates)
          ),
          subject_name=desired_objects[desired_key].model_name,
          desired_key=desired_key,
        ))
      if not progressed:
        return

  def _normalized_object_key(
    self,
    current: EnvironmentMetadataObject,
    aliases: Mapping[str, str],
  ) -> str:
    values = {
      name: _replace_aliases(value, aliases)
      for name, value in current.identity.components
    }
    contract = METADATA_TRANSPORT_REGISTRY.get_model(current.model_name)
    try:
      return build_metadata_object_identity(
        model_name=current.model_name,
        contract=contract.identity,
        values=values,
      ).object_key
    except ValueError:
      return current.object_key

  def _build_object_actions(
    self,
    *,
    desired_objects: Mapping[str, EnvironmentMetadataObject],
    current_objects: Mapping[str, EnvironmentMetadataObject],
    matches: Mapping[str, str],
    aliases: Mapping[str, str],
    issues: list[EnvironmentPromotionIssue],
  ) -> list[EnvironmentPromotionAction]:
    actions: list[EnvironmentPromotionAction] = []
    used_current = set(matches.values())

    for desired_key in sorted(desired_objects):
      desired = desired_objects[desired_key]
      contract = METADATA_TRANSPORT_REGISTRY.get_model(desired.model_name)
      current_key = matches.get(desired_key)
      current = current_objects.get(current_key) if current_key else None
      verify_only = _is_verify_only(desired, contract)

      if verify_only:
        changed_fields = _changed_fields(
          current=current,
          desired=desired,
          aliases=aliases,
        )
        update_fields = tuple(
          field_name
          for field_name in changed_fields
          if field_name in contract.system_managed_update_fields
        )
        protected_fields = tuple(
          field_name
          for field_name in changed_fields
          if field_name not in contract.system_managed_update_fields
        )
        blocked = current is None or bool(protected_fields)
        message = (
          "Required system-managed metadata is missing in the target."
          if current is None
          else (
            "Protected system-managed metadata differs from the release "
            "definition."
            if protected_fields
            else (
              "Protected system-managed metadata matches the release "
              "definition."
            )
          )
        )
        actions.append(_object_action(
          action_type=EnvironmentPromotionActionType.VERIFY,
          current=current,
          desired=desired,
          changed_fields=protected_fields,
          dependency_phase=int(contract.dependency_phase),
          blocked=blocked,
          message=message,
        ))
        if current is not None and update_fields:
          actions.append(_object_action(
            action_type=EnvironmentPromotionActionType.UPDATE,
            current=current,
            desired=desired,
            changed_fields=update_fields,
            dependency_phase=int(contract.dependency_phase),
            message=(
              "Editable system-managed metadata differs from the release "
              "definition."
            ),
          ))
        if blocked:
          issues.append(EnvironmentPromotionIssue(
            severity=EnvironmentPromotionIssueSeverity.ERROR,
            code=(
              "verify_only_object_missing"
              if current is None
              else "verify_only_object_mismatch"
            ),
            message=message,
            subject_name=desired.model_name,
            current_key=current.object_key if current else None,
            desired_key=desired.object_key,
          ))
        continue

      if current is None:
        actions.append(_object_action(
          action_type=EnvironmentPromotionActionType.CREATE,
          current=None,
          desired=desired,
          dependency_phase=int(contract.dependency_phase),
        ))
        continue

      changed_fields = list(_changed_fields(
        current=current,
        desired=desired,
        aliases=aliases,
      ))
      lifecycle_action = None
      if (
        "active" in changed_fields
        and contract.lifecycle in {
          MetadataLifecycleStrategy.DEACTIVATE,
          MetadataLifecycleStrategy.RETIRE,
        }
      ):
        changed_fields.remove("active")
        current_active = bool(current.fields.get("active"))
        desired_active = bool(desired.fields.get("active"))
        if not current_active and desired_active:
          lifecycle_action = EnvironmentPromotionActionType.REACTIVATE
        elif current_active and not desired_active:
          lifecycle_action = (
            EnvironmentPromotionActionType.RETIRE
            if contract.lifecycle == MetadataLifecycleStrategy.RETIRE
            else EnvironmentPromotionActionType.DEACTIVATE
          )

      if changed_fields:
        actions.append(_object_action(
          action_type=EnvironmentPromotionActionType.UPDATE,
          current=current,
          desired=desired,
          changed_fields=tuple(changed_fields),
          dependency_phase=int(contract.dependency_phase),
        ))
      if lifecycle_action is not None:
        actions.append(_object_action(
          action_type=lifecycle_action,
          current=current,
          desired=desired,
          changed_fields=("active",),
          dependency_phase=int(contract.dependency_phase),
        ))

    for current_key in sorted(set(current_objects) - used_current):
      current = current_objects[current_key]
      contract = METADATA_TRANSPORT_REGISTRY.get_model(current.model_name)
      if _is_verify_only(current, contract):
        message = (
          "The target contains system-managed metadata not present in the "
          "release definition."
        )
        actions.append(_object_action(
          action_type=EnvironmentPromotionActionType.VERIFY,
          current=current,
          desired=None,
          dependency_phase=int(contract.dependency_phase),
          blocked=True,
          message=message,
        ))
        issues.append(EnvironmentPromotionIssue(
          severity=EnvironmentPromotionIssueSeverity.ERROR,
          code="unexpected_verify_only_object",
          message=message,
          subject_name=current.model_name,
          current_key=current.object_key,
        ))
        continue

      if contract.lifecycle == MetadataLifecycleStrategy.DELETE:
        action_type = EnvironmentPromotionActionType.DELETE
      else:
        if not bool(current.fields.get("active")):
          continue
        action_type = (
          EnvironmentPromotionActionType.RETIRE
          if contract.lifecycle == MetadataLifecycleStrategy.RETIRE
          else EnvironmentPromotionActionType.DEACTIVATE
        )
      actions.append(_object_action(
        action_type=action_type,
        current=current,
        desired=None,
        dependency_phase=int(contract.dependency_phase),
      ))

    return actions

  def _build_relationship_actions(
    self,
    *,
    desired_relationships: Sequence[EnvironmentMetadataRelationship],
    current_relationships: Sequence[EnvironmentMetadataRelationship],
    aliases: Mapping[str, str],
    issues: list[EnvironmentPromotionIssue],
  ) -> list[EnvironmentPromotionAction]:
    actions = []
    relationship_contracts = {
      item.name: item
      for item in METADATA_TRANSPORT_REGISTRY.relationships
    }
    desired_by_key = {
      item.relationship_key: item
      for item in desired_relationships
    }
    normalized_current: dict[str, list[EnvironmentMetadataRelationship]] = {}
    for current in current_relationships:
      normalized = EnvironmentMetadataRelationship(
        relationship_name=current.relationship_name,
        source_model=current.source_model,
        source_key=_replace_aliases(current.source_key, aliases),
        target_model=current.target_model,
        target_key=_replace_aliases(current.target_key, aliases),
      )
      normalized_current.setdefault(
        normalized.relationship_key,
        [],
      ).append(current)

    matched_current_keys = set()
    for desired_key, desired in desired_by_key.items():
      candidates = normalized_current.get(desired_key, [])
      contract = relationship_contracts[desired.relationship_name]
      if len(candidates) == 1:
        matched_current_keys.add(candidates[0].relationship_key)
        continue
      if len(candidates) > 1:
        issues.append(EnvironmentPromotionIssue(
          severity=EnvironmentPromotionIssueSeverity.ERROR,
          code="ambiguous_relationship_match",
          message=(
            "Multiple current relationships normalize to one desired "
            "relationship."
          ),
          subject_name=desired.relationship_name,
          desired_key=desired.relationship_key,
        ))
        continue
      actions.append(EnvironmentPromotionAction(
        action_type=EnvironmentPromotionActionType.ADD_RELATIONSHIP,
        subject_type=EnvironmentPromotionSubjectType.RELATIONSHIP,
        subject_name=desired.relationship_name,
        dependency_phase=int(contract.dependency_phase),
        desired=desired.to_dict(),
      ))

    for current in current_relationships:
      if current.relationship_key in matched_current_keys:
        continue
      normalized = EnvironmentMetadataRelationship(
        relationship_name=current.relationship_name,
        source_model=current.source_model,
        source_key=_replace_aliases(current.source_key, aliases),
        target_model=current.target_model,
        target_key=_replace_aliases(current.target_key, aliases),
      )
      if normalized.relationship_key in desired_by_key:
        continue
      contract = relationship_contracts[current.relationship_name]
      actions.append(EnvironmentPromotionAction(
        action_type=EnvironmentPromotionActionType.REMOVE_RELATIONSHIP,
        subject_type=EnvironmentPromotionSubjectType.RELATIONSHIP,
        subject_name=current.relationship_name,
        dependency_phase=int(contract.dependency_phase),
        current=current.to_dict(),
      ))
    return actions


def build_environment_promotion_plan(
  *,
  bundle: ArchitectureReleaseBundle,
  target_snapshot: EnvironmentMetadataSnapshot,
  created_at: datetime | None = None,
) -> EnvironmentPromotionPlan:
  """
  Build an immutable promotion plan for one target environment snapshot.
  """
  return EnvironmentPromotionPlanner().build(
    bundle=bundle,
    target_snapshot=target_snapshot,
    created_at=created_at,
  )


def _is_verify_only(item, contract) -> bool:
  return (
    contract.system_managed_apply_mode == MetadataManagedApplyMode.VERIFY_ONLY
    and bool(contract.managed_field)
    and bool(item.fields.get(contract.managed_field))
  )


def _changed_fields(
  *,
  current: EnvironmentMetadataObject | None,
  desired: EnvironmentMetadataObject,
  aliases: Mapping[str, str],
) -> tuple[str, ...]:
  if current is None:
    return ()
  current_fields = {
    name: _replace_aliases(value, aliases)
    for name, value in current.fields.items()
  }
  return tuple(sorted(
    name
    for name, desired_value in desired.fields.items()
    if current_fields.get(name) != desired_value
  ))


def _object_action(
  *,
  action_type: EnvironmentPromotionActionType,
  current: EnvironmentMetadataObject | None,
  desired: EnvironmentMetadataObject | None,
  dependency_phase: int,
  changed_fields: Sequence[str] = (),
  blocked: bool = False,
  message: str = "",
) -> EnvironmentPromotionAction:
  subject = desired or current
  if subject is None:
    raise EnvironmentPromotionPlannerError(
      "Object action requires current or desired metadata."
    )
  return EnvironmentPromotionAction(
    action_type=action_type,
    subject_type=EnvironmentPromotionSubjectType.OBJECT,
    subject_name=subject.model_name,
    dependency_phase=dependency_phase,
    current=current.to_dict() if current is not None else None,
    desired=desired.to_dict() if desired is not None else None,
    changed_fields=tuple(changed_fields),
    blocked=blocked,
    message=message,
  )


def _replace_aliases(value: Any, aliases: Mapping[str, str]) -> Any:
  if isinstance(value, str):
    seen = set()
    resolved = value
    while resolved in aliases and resolved not in seen:
      seen.add(resolved)
      resolved = aliases[resolved]
    return resolved
  if isinstance(value, Mapping):
    return {
      str(key): _replace_aliases(item, aliases)
      for key, item in value.items()
    }
  if isinstance(value, tuple):
    return tuple(_replace_aliases(item, aliases) for item in value)
  if isinstance(value, list):
    return [_replace_aliases(item, aliases) for item in value]
  return value
