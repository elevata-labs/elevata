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

from collections import Counter
from typing import Any

from django.conf import settings
from django.contrib.auth.decorators import login_required, permission_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils.dateparse import parse_datetime
from django.views.decorators.http import require_GET, require_POST

from metadata.promotion.approval import (
  EnvironmentPromotionApprovalError,
  build_environment_promotion_approval,
)
from metadata.promotion.approval_store import (
  EnvironmentPromotionApprovalStore,
  EnvironmentPromotionApprovalStoreError,
)
from metadata.promotion.deployment import (
  EnvironmentPromotionDeploymentPackageError,
  build_environment_promotion_deployment_package,
  serialize_environment_promotion_deployment_package,
)
from metadata.promotion.deployment_package_store import (
  EnvironmentPromotionDeploymentPackageStore,
  EnvironmentPromotionDeploymentPackageStoreError,
)
from metadata.promotion.planner import (
  EnvironmentPromotionPlannerError,
  build_environment_promotion_plan,
)
from metadata.promotion.release import serialize_architecture_release_bundle
from metadata.promotion.release_service import (
  create_architecture_release_bundle,
  store_architecture_release_bundle,
)
from metadata.promotion.release_store import (
  ArchitectureReleaseStore,
  ArchitectureReleaseStoreError,
)
from metadata.promotion.release_validation import ArchitectureReleaseValidationError
from metadata.promotion.snapshot_builder import EnvironmentMetadataSnapshotBuildError
from metadata.promotion.target_client import (
  EnvironmentPromotionTargetClient,
  EnvironmentPromotionTargetClientError,
)
from metadata.promotion.target_registry import (
  EnvironmentPromotionTarget,
  EnvironmentPromotionTargetRegistryError,
  get_environment_promotion_target,
  load_environment_promotion_targets,
)


class EnvironmentPromotionUIError(ValueError):
  """Raised when a UI workflow cannot preserve exact promotion bindings."""


def _metadata_database_binding() -> dict[str, str]:
  """Describe the authoring metadata DB bound to this control-plane process."""
  database = settings.DATABASES.get("default", {})
  return {
    "engine": str(database.get("ENGINE") or ""),
    "name": str(database.get("NAME") or ""),
  }


def _release_row(bundle) -> dict[str, Any]:
  """Build one compact Architecture Release presentation row."""
  metadata = getattr(getattr(bundle, "snapshot", None), "metadata", None)
  objects = tuple(getattr(metadata, "objects", ()) or ())
  relationships = tuple(getattr(metadata, "relationships", ()) or ())
  return {
    "release_id": bundle.release_id,
    "release_name": bundle.release_name,
    "release_version": bundle.release_version,
    "description": bundle.description,
    "source_environment_label": bundle.source_environment_label,
    "created_at": bundle.created_at,
    "created_by": bundle.created_by,
    "object_count": len(objects),
    "relationship_count": len(relationships),
    "bundle_fingerprint": bundle.bundle_fingerprint,
    "metadata_fingerprint": bundle.metadata_fingerprint,
  }


def _remote_history_row(
  record: dict[str, Any],
  release_by_id: dict[str, Any],
  *,
  expected_target_environment: str,
) -> dict[str, Any]:
  """Build one validated remote target Promotion History presentation row."""
  if not isinstance(record, dict):
    raise EnvironmentPromotionUIError(
      "Promotion target history record must be a JSON object."
    )

  def require_text(field_name: str) -> str:
    value = str(record.get(field_name) or "").strip()
    if not value:
      raise EnvironmentPromotionUIError(
        f"Promotion target history record is missing {field_name}."
      )
    return value

  target_environment = require_text("target_environment_label")
  if target_environment != expected_target_environment:
    raise EnvironmentPromotionUIError(
      "Promotion target history record environment mismatch: expected "
      f"{expected_target_environment}, received {target_environment}."
    )

  applied_at_raw = require_text("applied_at")
  applied_at = parse_datetime(applied_at_raw)
  if applied_at is None or applied_at.tzinfo is None:
    raise EnvironmentPromotionUIError(
      "Promotion target history record applied_at must be a timezone-aware "
      "ISO-8601 timestamp."
    )

  summary = record.get("summary")
  if not isinstance(summary, dict):
    raise EnvironmentPromotionUIError(
      "Promotion target history record summary must be a JSON object."
    )
  counts: dict[str, int] = {}
  for field_name in (
    "action_count",
    "applied_action_count",
    "verified_action_count",
  ):
    value = summary.get(field_name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
      raise EnvironmentPromotionUIError(
        f"Promotion target history record summary {field_name} must be a "
        "non-negative integer."
      )
    counts[field_name] = value

  release_id = require_text("release_id")
  bundle = release_by_id.get(release_id)
  release_metadata_match = None
  release_coordinate = ""
  if bundle is not None:
    release_coordinate = f"{bundle.release_name} {bundle.release_version}"
    release_metadata_match = (
      bundle.metadata_fingerprint
      == require_text("post_target_metadata_fingerprint")
    )

  return {
    "record_id": require_text("record_id"),
    "record_fingerprint": require_text("record_fingerprint"),
    "release_id": release_id,
    "release_coordinate": release_coordinate,
    "plan_id": require_text("plan_id"),
    "approval_id": require_text("approval_id"),
    "target_environment_label": target_environment,
    "source_environment_label": require_text("source_environment_label"),
    "applied_at": applied_at,
    "applied_by": require_text("applied_by"),
    "action_count": counts["action_count"],
    "applied_action_count": counts["applied_action_count"],
    "verified_action_count": counts["verified_action_count"],
    "post_target_metadata_fingerprint": require_text(
      "post_target_metadata_fingerprint"
    ),
    "release_metadata_match": release_metadata_match,
  }


def _target_row(target: EnvironmentPromotionTarget) -> dict[str, Any]:
  """Build one non-secret authoring-side Promotion Target row."""
  return {
    "environment_label": target.environment_label,
    "base_url": target.base_url,
    "timeout_seconds": target.timeout_seconds,
  }


def _next_release_version_suggestion(bundles) -> str:
  """Return a conservative next numeric release-version suggestion."""
  if not bundles:
    return ""
  latest = str(bundles[0].release_version or "").strip()
  parts = latest.split(".")
  if not parts or any(not part.isdigit() for part in parts):
    return ""
  width = len(parts[-1])
  parts[-1] = str(int(parts[-1]) + 1).zfill(width)
  return ".".join(parts)


def _build_environment_promotion_context(
  *,
  release_store: ArchitectureReleaseStore | None = None,
  targets: tuple[EnvironmentPromotionTarget, ...] | None = None,
) -> dict[str, Any]:
  """Build the Environment Promotion workspace overview without remote I/O."""
  release_store = release_store or ArchitectureReleaseStore()

  release_error = ""
  target_config_error = ""

  try:
    bundles = tuple(release_store.load_all())
  except ArchitectureReleaseStoreError as exc:
    bundles = ()
    release_error = str(exc)

  if targets is None:
    try:
      targets = load_environment_promotion_targets()
    except EnvironmentPromotionTargetRegistryError as exc:
      targets = ()
      target_config_error = str(exc)

  ordered_bundles = tuple(sorted(
    bundles,
    key=lambda item: (
      item.created_at,
      item.release_name,
      item.release_version,
      item.release_id,
    ),
    reverse=True,
  ))

  return {
    "title": "Environment Promotion",
    "runtime_mode": getattr(settings, "ELEVATA_RUNTIME_MODE", "authoring"),
    "control_plane_environment": getattr(settings, "ELEVATA_ENVIRONMENT", ""),
    "metadata_database": _metadata_database_binding(),
    "release_store_path": str(release_store.base_path),
    "releases": tuple(_release_row(bundle) for bundle in ordered_bundles),
    "release_count": len(ordered_bundles),
    "release_error": release_error,
    "release_name_suggestion": (
      ordered_bundles[0].release_name if ordered_bundles else ""
    ),
    "release_version_suggestion": _next_release_version_suggestion(ordered_bundles),
    "promotion_targets": tuple(_target_row(target) for target in targets),
    "target_count": len(targets),
    "target_config_error": target_config_error,
    "remote_history": None,
    "remote_history_error": "",
    "selected_history_target_environment": "",
  }


def _require_authoring_environment() -> str:
  """Return the explicit authoring environment used for release creation."""
  runtime_mode = str(
    getattr(settings, "ELEVATA_RUNTIME_MODE", "authoring") or ""
  ).strip()
  if runtime_mode != "authoring":
    raise EnvironmentPromotionUIError(
      "Architecture Releases can only be created in authoring runtime mode."
    )
  environment = str(
    getattr(settings, "ELEVATA_ENVIRONMENT", "") or ""
  ).strip()
  if not environment:
    raise EnvironmentPromotionUIError(
      "ELEVATA_ENVIRONMENT must be configured before creating an "
      "Architecture Release."
    )
  return environment


def _create_authoring_release(
  *,
  release_name: str,
  release_version: str,
  description: str,
  actor: str,
  release_store: ArchitectureReleaseStore | None = None,
):
  """Create and immutably store one release from the bound authoring DB."""
  name = str(release_name or "").strip()
  version = str(release_version or "").strip()
  if not name:
    raise EnvironmentPromotionUIError("Architecture release name is required.")
  if not version:
    raise EnvironmentPromotionUIError("Architecture release version is required.")

  bundle = create_architecture_release_bundle(
    release_name=name,
    release_version=version,
    created_by=str(actor or "").strip(),
    environment_label=_require_authoring_environment(),
    description=str(description or ""),
  )
  store_architecture_release_bundle(
    bundle,
    store=release_store or ArchitectureReleaseStore(),
  )
  return bundle


def _architecture_release_download_response(bundle) -> HttpResponse:
  """Return one validated Architecture Release as a no-store JSON download."""
  payload = serialize_architecture_release_bundle(bundle)
  response = HttpResponse(
    payload,
    content_type="application/json; charset=utf-8",
  )
  response["Cache-Control"] = "no-store"
  response["Content-Disposition"] = (
    f'attachment; filename="{bundle.release_id}.release.json"'
  )
  return response


def _fetch_target_state(
  target: EnvironmentPromotionTarget,
  *,
  actor: str,
  client_factory=EnvironmentPromotionTargetClient,
):
  """Fetch health + current snapshot from one exact configured target."""
  client = client_factory(target)
  health = client.health()
  target_version = str(health.get("elevata_version") or "").strip()
  if target_version != settings.ELEVATA_VERSION:
    raise EnvironmentPromotionTargetClientError(
      "Promotion target elevata version mismatch: expected "
      f"{settings.ELEVATA_VERSION}, received {target_version or '<empty>'}."
    )
  snapshot = client.snapshot(actor=actor)
  return client, health, snapshot


def _build_target_status(
  target: EnvironmentPromotionTarget,
  *,
  actor: str,
  client_factory=EnvironmentPromotionTargetClient,
) -> dict[str, Any]:
  """Build explicit read-only connectivity and current-state evidence."""
  client, health, snapshot = _fetch_target_state(
    target,
    actor=actor,
    client_factory=client_factory,
  )
  history = client.history()
  records = history["records"]
  last_record = records[0] if records else None
  return {
    "environment_label": target.environment_label,
    "base_url": target.base_url,
    "reachable": True,
    "runtime_mode": health["runtime_mode"],
    "elevata_version": health["elevata_version"],
    "metadata_database": health["metadata_database"],
    "metadata_fingerprint": snapshot.metadata_fingerprint,
    "snapshot_fingerprint": snapshot.snapshot_fingerprint,
    "last_promotion": last_record,
    "history_count": len(records),
    "history_loaded": True,
  }


def _build_remote_history_review(
  target: EnvironmentPromotionTarget,
  *,
  release_store: ArchitectureReleaseStore | None = None,
  client_factory=EnvironmentPromotionTargetClient,
) -> dict[str, Any]:
  """Fetch and validate authoritative Promotion History from one target runner."""
  client = client_factory(target)
  health = client.health()
  target_version = str(health.get("elevata_version") or "").strip()
  if target_version != settings.ELEVATA_VERSION:
    raise EnvironmentPromotionTargetClientError(
      "Promotion target elevata version mismatch: expected "
      f"{settings.ELEVATA_VERSION}, received {target_version or '<empty>'}."
    )
  history = client.history()

  release_store = release_store or ArchitectureReleaseStore()
  bundles = tuple(release_store.load_all())
  release_by_id = {bundle.release_id: bundle for bundle in bundles}
  rows = tuple(
    sorted(
      (
        _remote_history_row(
          item,
          release_by_id,
          expected_target_environment=target.environment_label,
        )
        for item in history["records"]
      ),
      key=lambda item: (item["applied_at"], item["record_id"]),
      reverse=True,
    )
  )
  return {
    "environment_label": target.environment_label,
    "base_url": target.base_url,
    "elevata_version": health["elevata_version"],
    "records": rows,
    "record_count": len(rows),
  }


def _build_exact_plan(
  *,
  release_id: str,
  target_environment_label: str,
  actor: str,
  release_store: ArchitectureReleaseStore | None = None,
  targets: tuple[EnvironmentPromotionTarget, ...] | None = None,
  client_factory=EnvironmentPromotionTargetClient,
):
  """Build one exact plan from the stored release and live remote snapshot."""
  release_store = release_store or ArchitectureReleaseStore()
  target = get_environment_promotion_target(
    target_environment_label,
    targets=targets,
  )
  bundle = release_store.load(str(release_id or "").strip())
  if bundle is None:
    raise ArchitectureReleaseStoreError(
      f"Architecture release is not stored: {release_id or '<empty>'}."
    )

  client, health, snapshot = _fetch_target_state(
    target,
    actor=actor,
    client_factory=client_factory,
  )
  plan = build_environment_promotion_plan(
    bundle=bundle,
    target_snapshot=snapshot,
  )
  return target, client, health, snapshot, bundle, plan


def _target_status_from_snapshot(
  target: EnvironmentPromotionTarget,
  health: dict[str, Any],
  snapshot,
) -> dict[str, Any]:
  """Build compact target-state evidence without loading promotion history."""
  return {
    "environment_label": target.environment_label,
    "base_url": target.base_url,
    "reachable": True,
    "runtime_mode": health["runtime_mode"],
    "elevata_version": health["elevata_version"],
    "metadata_database": health["metadata_database"],
    "metadata_fingerprint": snapshot.metadata_fingerprint,
    "snapshot_fingerprint": snapshot.snapshot_fingerprint,
    "last_promotion": None,
    "history_count": None,
    "history_loaded": False,
  }


def _build_plan_review(
  *,
  release_id: str,
  target_environment_label: str,
  actor: str,
  release_store: ArchitectureReleaseStore | None = None,
  targets: tuple[EnvironmentPromotionTarget, ...] | None = None,
  client_factory=EnvironmentPromotionTargetClient,
) -> tuple[dict[str, Any], dict[str, Any]]:
  """Fetch one target snapshot and build the deterministic read-only plan."""
  target, _, health, snapshot, bundle, plan = _build_exact_plan(
    release_id=release_id,
    target_environment_label=target_environment_label,
    actor=actor,
    release_store=release_store,
    targets=targets,
    client_factory=client_factory,
  )
  return (
    _target_status_from_snapshot(target, health, snapshot),
    _promotion_plan_row(plan, bundle),
  )


def _promotion_plan_row(plan, bundle) -> dict[str, Any]:
  """Build a summary-first UI representation of one exact promotion plan."""
  summary = plan.summary
  by_model = Counter(action.subject_name for action in plan.actions)
  by_phase = Counter(action.dependency_phase for action in plan.actions)
  by_change_class = dict(summary["by_change_class"])
  attention_count = (
    int(by_change_class.get("destructive", 0))
    + int(by_change_class.get("lifecycle", 0))
  )
  return {
    "plan_id": plan.plan_id,
    "plan_fingerprint": plan.plan_fingerprint,
    "release_id": plan.release_id,
    "release_coordinate": f"{bundle.release_name} {bundle.release_version}",
    "bundle_fingerprint": plan.bundle_fingerprint,
    "source_environment_label": plan.source_environment_label,
    "source_metadata_fingerprint": plan.source_metadata_fingerprint,
    "target_environment_label": plan.target_environment_label,
    "target_snapshot_fingerprint": plan.target_snapshot_fingerprint,
    "target_metadata_fingerprint": plan.target_metadata_fingerprint,
    "readiness_status": plan.readiness.status.value,
    "can_apply": plan.readiness.can_apply,
    "error_count": plan.readiness.error_count,
    "warning_count": plan.readiness.warning_count,
    "issues": tuple(issue.to_dict() for issue in plan.readiness.issues),
    "action_count": summary["action_count"],
    "mutating_action_count": summary["mutating_action_count"],
    "blocked_action_count": summary["blocked_action_count"],
    "attention_action_count": attention_count,
    "by_action_type": tuple(sorted(summary["by_action_type"].items())),
    "by_change_class": tuple(sorted(by_change_class.items())),
    "by_model": tuple(sorted(
      by_model.items(),
      key=lambda item: (-item[1], item[0]),
    )),
    "by_dependency_phase": tuple(sorted(by_phase.items())),
  }


def _approval_row(approval) -> dict[str, Any]:
  """Build a compact exact-plan approval presentation row."""
  return {
    "approval_id": approval.approval_id,
    "artifact_fingerprint": approval.artifact_fingerprint,
    "plan_id": approval.plan.plan_id,
    "plan_fingerprint": approval.plan.plan_fingerprint,
    "target_environment_label": approval.plan.target_environment_label,
    "decision": approval.review.decision.value,
    "decided_by": approval.review.decided_by,
    "decided_at": approval.review.decided_at,
    "note": approval.review.note,
  }


def _package_row(package) -> dict[str, Any]:
  """Build a compact immutable deployment-package presentation row."""
  return {
    "package_id": package.package_id,
    "package_fingerprint": package.package_fingerprint,
    "release_id": package.bundle.release_id,
    "plan_id": package.plan.plan_id,
    "approval_id": package.approval.approval_id,
    "target_environment_label": package.target_environment_label,
    "created_at": package.created_at,
    "created_by": package.created_by,
  }


def _drift_check_row(payload: dict[str, Any]) -> dict[str, Any]:
  """Build compact live-target drift evidence returned by the runner."""
  return {
    "status": payload["status"],
    "is_unchanged": payload["is_unchanged"],
    "package_id": payload["package_id"],
    "expected_snapshot_fingerprint": payload["expected_snapshot_fingerprint"],
    "expected_metadata_fingerprint": payload["expected_metadata_fingerprint"],
    "live_snapshot_fingerprint": payload["live_snapshot_fingerprint"],
    "live_metadata_fingerprint": payload["live_metadata_fingerprint"],
  }


def _promotion_apply_result_row(
  record,
  package,
  *,
  remote_history_confirmed: bool,
  remote_history_count: int | None,
  remote_history_error: str = "",
) -> dict[str, Any]:
  """Build compact immutable apply/convergence evidence for the UI."""
  summary = record.summary
  return {
    "record_id": record.record_id,
    "record_fingerprint": record.record_fingerprint,
    "release_id": record.release_id,
    "release_coordinate": (
      f"{package.bundle.release_name} {package.bundle.release_version}"
    ),
    "plan_id": record.plan_id,
    "approval_id": record.approval_id,
    "target_environment_label": record.target_environment_label,
    "applied_at": record.applied_at,
    "applied_by": record.applied_by,
    "action_count": summary["action_count"],
    "applied_action_count": summary["applied_action_count"],
    "verified_action_count": summary["verified_action_count"],
    "pre_target_metadata_fingerprint": (
      record.pre_target_metadata_fingerprint
    ),
    "post_target_metadata_fingerprint": (
      record.post_target_metadata_fingerprint
    ),
    "post_target_snapshot_fingerprint": (
      record.post_target_snapshot_fingerprint
    ),
    "post_validation_plan_id": record.post_validation_plan_id,
    "post_validation_plan_fingerprint": (
      record.post_validation_plan_fingerprint
    ),
    "release_metadata_match": (
      record.post_target_metadata_fingerprint
      == package.bundle.metadata_fingerprint
    ),
    "remote_history_confirmed": remote_history_confirmed,
    "remote_history_count": remote_history_count,
    "remote_history_error": remote_history_error,
  }


def _apply_stored_package(
  *,
  target_environment_label: str,
  approval_id: str,
  package_id: str,
  confirm_package_id: str,
  actor: str,
  package_store: EnvironmentPromotionDeploymentPackageStore | None = None,
  targets: tuple[EnvironmentPromotionTarget, ...] | None = None,
  client_factory=EnvironmentPromotionTargetClient,
):
  """Apply one exact stored package through its configured target runner."""
  target = get_environment_promotion_target(
    target_environment_label,
    targets=targets,
  )
  package_store = package_store or EnvironmentPromotionDeploymentPackageStore()
  package = package_store.load_for_approval(
    target_environment_label=target.environment_label,
    approval_id=approval_id,
  )
  if package is None:
    raise EnvironmentPromotionDeploymentPackageStoreError(
      "No stored deployment package exists for the selected approval."
    )

  expected_package_id = str(package_id or "").strip()
  if package.package_id != expected_package_id:
    raise EnvironmentPromotionUIError(
      "Deployment package does not match the stored immutable package."
    )
  confirmation = str(confirm_package_id or "").strip()
  if confirmation != package.package_id:
    raise EnvironmentPromotionUIError(
      "Final package confirmation must exactly match the deployment package ID."
    )

  client = client_factory(target)
  health = client.health()
  target_version = str(health.get("elevata_version") or "").strip()
  if target_version != settings.ELEVATA_VERSION:
    raise EnvironmentPromotionTargetClientError(
      "Promotion target elevata version mismatch: expected "
      f"{settings.ELEVATA_VERSION}, received {target_version or '<empty>'}."
    )

  record = client.apply(
    package=package,
    confirm_package_id=confirmation,
    actor=actor,
  )

  history_error = ""
  history_records: list[dict[str, Any]] = []
  try:
    history = client.history()
    history_records = list(history["records"])
  except EnvironmentPromotionTargetClientError as exc:
    history_error = str(exc)

  matching_history = next(
    (
      item
      for item in history_records
      if item.get("record_id") == record.record_id
    ),
    None,
  )
  history_confirmed = bool(
    matching_history
    and matching_history.get("record_fingerprint") == record.record_fingerprint
    and matching_history.get("approval_id") == record.approval_id
    and matching_history.get("plan_id") == record.plan_id
    and matching_history.get("release_id") == record.release_id
    and matching_history.get("post_target_metadata_fingerprint")
      == record.post_target_metadata_fingerprint
  )

  last_record = history_records[0] if history_records else {
    "record_id": record.record_id,
    "release_id": record.release_id,
    "applied_at": record.applied_at.isoformat(),
  }
  target_status = {
    "environment_label": target.environment_label,
    "base_url": target.base_url,
    "reachable": True,
    "runtime_mode": health["runtime_mode"],
    "elevata_version": health["elevata_version"],
    "metadata_database": health["metadata_database"],
    "metadata_fingerprint": record.post_target_metadata_fingerprint,
    "snapshot_fingerprint": record.post_target_snapshot_fingerprint,
    "last_promotion": last_record,
    "history_count": len(history_records) if not history_error else None,
    "history_loaded": not history_error,
  }
  plan_row = _promotion_plan_row(package.plan, package.bundle)
  plan_row["applied"] = True
  return (
    target_status,
    plan_row,
    _approval_row(package.approval),
    _package_row(package),
    _promotion_apply_result_row(
      record,
      package,
      remote_history_confirmed=history_confirmed,
      remote_history_count=(len(history_records) if not history_error else None),
      remote_history_error=history_error,
    ),
  )


def _approve_exact_plan_and_build_package(
  *,
  release_id: str,
  target_environment_label: str,
  reviewed_plan_id: str,
  actor: str,
  note: str = "",
  release_store: ArchitectureReleaseStore | None = None,
  approval_store: EnvironmentPromotionApprovalStore | None = None,
  package_store: EnvironmentPromotionDeploymentPackageStore | None = None,
  targets: tuple[EnvironmentPromotionTarget, ...] | None = None,
  client_factory=EnvironmentPromotionTargetClient,
):
  """Revalidate one reviewed plan, approve it and persist one exact package."""
  target, _, health, snapshot, bundle, plan = _build_exact_plan(
    release_id=release_id,
    target_environment_label=target_environment_label,
    actor=actor,
    release_store=release_store,
    targets=targets,
    client_factory=client_factory,
  )
  expected_plan_id = str(reviewed_plan_id or "").strip()
  if plan.plan_id != expected_plan_id:
    raise EnvironmentPromotionUIError(
      "Promotion target state changed after review. The rebuilt plan no longer "
      "matches the reviewed plan; review the new plan before approval."
    )

  approval_store = approval_store or EnvironmentPromotionApprovalStore()
  approval = approval_store.load_for_plan(
    target_environment_label=target.environment_label,
    plan_fingerprint=plan.plan_fingerprint,
  )
  if approval is None:
    approval = build_environment_promotion_approval(
      plan=plan,
      decided_by=actor,
      note=str(note or ""),
    )
    approval_store.save(approval)

  package_store = package_store or EnvironmentPromotionDeploymentPackageStore()
  package = package_store.load_for_approval(
    target_environment_label=target.environment_label,
    approval_id=approval.approval_id,
  )
  if package is None:
    package = build_environment_promotion_deployment_package(
      bundle=bundle,
      plan=plan,
      approval=approval,
      created_by=actor,
    )
    package_store.save(package)
  elif package.plan.plan_id != plan.plan_id:
    raise EnvironmentPromotionUIError(
      "Stored deployment package does not match the exact reviewed plan."
    )

  return (
    _target_status_from_snapshot(target, health, snapshot),
    _promotion_plan_row(plan, bundle),
    _approval_row(approval),
    _package_row(package),
  )


def _check_stored_package(
  *,
  target_environment_label: str,
  approval_id: str,
  package_id: str,
  actor: str,
  package_store: EnvironmentPromotionDeploymentPackageStore | None = None,
  targets: tuple[EnvironmentPromotionTarget, ...] | None = None,
  client_factory=EnvironmentPromotionTargetClient,
):
  """Run live drift validation for one exact stored immutable package."""
  target = get_environment_promotion_target(
    target_environment_label,
    targets=targets,
  )
  package_store = package_store or EnvironmentPromotionDeploymentPackageStore()
  package = package_store.load_for_approval(
    target_environment_label=target.environment_label,
    approval_id=approval_id,
  )
  if package is None:
    raise EnvironmentPromotionDeploymentPackageStoreError(
      "No stored deployment package exists for the selected approval."
    )
  expected_package_id = str(package_id or "").strip()
  if package.package_id != expected_package_id:
    raise EnvironmentPromotionUIError(
      "Deployment package confirmation does not match the stored package ID."
    )

  client = client_factory(target)
  health = client.health()
  target_version = str(health.get("elevata_version") or "").strip()
  if target_version != settings.ELEVATA_VERSION:
    raise EnvironmentPromotionTargetClientError(
      "Promotion target elevata version mismatch: expected "
      f"{settings.ELEVATA_VERSION}, received {target_version or '<empty>'}."
    )
  payload = client.check(package=package, actor=actor)
  target_status = {
    "environment_label": target.environment_label,
    "base_url": target.base_url,
    "reachable": True,
    "runtime_mode": health["runtime_mode"],
    "elevata_version": health["elevata_version"],
    "metadata_database": health["metadata_database"],
    "metadata_fingerprint": payload["live_metadata_fingerprint"],
    "snapshot_fingerprint": payload["live_snapshot_fingerprint"],
    "last_promotion": None,
    "history_count": None,
    "history_loaded": False,
  }
  return (
    target_status,
    _promotion_plan_row(package.plan, package.bundle),
    _approval_row(package.approval),
    _package_row(package),
    _drift_check_row(payload),
  )


def _request_actor(request: HttpRequest) -> str:
  """Return one stable audit actor for read-only remote runner requests."""
  user = request.user
  return str(
    getattr(user, "email", "")
    or getattr(user, "username", "")
    or "environment-promotion-ui"
  ).strip()


def _render_environment_promotion(
  request: HttpRequest,
  **extra_context,
) -> HttpResponse:
  context = _build_environment_promotion_context()
  context.update(extra_context)
  return render(
    request,
    "metadata/promotion/environment_promotion.html",
    context,
  )


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
def environment_promotion(request):
  """Render the Environment Promotion workspace without remote side effects."""
  return _render_environment_promotion(request)


@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_POST
def environment_promotion_release_create(request):
  """Create one immutable Architecture Release from current authoring metadata."""
  release_name = str(request.POST.get("release_name") or "").strip()
  release_version = str(request.POST.get("release_version") or "").strip()
  description = str(request.POST.get("release_description") or "")
  try:
    bundle = _create_authoring_release(
      release_name=release_name,
      release_version=release_version,
      description=description,
      actor=_request_actor(request),
    )
  except (
    ArchitectureReleaseStoreError,
    ArchitectureReleaseValidationError,
    EnvironmentMetadataSnapshotBuildError,
    EnvironmentPromotionUIError,
    ValueError,
  ) as exc:
    return _render_environment_promotion(
      request,
      release_form={
        "release_name": release_name,
        "release_version": release_version,
        "release_description": description,
      },
      release_creation_error=str(exc),
    )
  return _render_environment_promotion(
    request,
    created_release=_release_row(bundle),
  )


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
@require_GET
def environment_promotion_release_download(request, release_id: str):
  """Download one exact stored immutable Architecture Release Bundle."""
  try:
    bundle = ArchitectureReleaseStore().require(release_id)
  except ArchitectureReleaseStoreError as exc:
    return HttpResponse(
      str(exc),
      status=404,
      content_type="text/plain; charset=utf-8",
    )
  return _architecture_release_download_response(bundle)


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
@require_POST
def environment_promotion_target_check(request):
  """Explicitly check one configured target runner and current metadata state."""
  target_label = str(request.POST.get("target_environment") or "").strip()
  try:
    target = get_environment_promotion_target(target_label)
    target_status = _build_target_status(
      target,
      actor=_request_actor(request),
    )
  except (
    EnvironmentPromotionTargetRegistryError,
    EnvironmentPromotionTargetClientError,
  ) as exc:
    return _render_environment_promotion(
      request,
      selected_target_environment=target_label,
      promotion_error=str(exc),
    )
  return _render_environment_promotion(
    request,
    selected_target_environment=target_label,
    target_status=target_status,
  )


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
@require_POST
def environment_promotion_history_refresh(request):
  """Explicitly load authoritative Promotion History from one target runner."""
  target_label = str(request.POST.get("target_environment") or "").strip()
  try:
    target = get_environment_promotion_target(target_label)
    remote_history = _build_remote_history_review(target)
  except (
    ArchitectureReleaseStoreError,
    EnvironmentPromotionTargetRegistryError,
    EnvironmentPromotionTargetClientError,
    EnvironmentPromotionUIError,
  ) as exc:
    return _render_environment_promotion(
      request,
      selected_history_target_environment=target_label,
      remote_history_error=str(exc),
    )
  return _render_environment_promotion(
    request,
    selected_history_target_environment=target_label,
    remote_history=remote_history,
  )


@login_required
@permission_required("metadata.view_targetdataset", raise_exception=True)
@require_POST
def environment_promotion_plan(request):
  """Build one deterministic promotion plan from release + remote target state."""
  release_id = str(request.POST.get("release_id") or "").strip()
  target_label = str(request.POST.get("target_environment") or "").strip()
  try:
    target_status, promotion_plan = _build_plan_review(
      release_id=release_id,
      target_environment_label=target_label,
      actor=_request_actor(request),
    )
  except (
    ArchitectureReleaseStoreError,
    EnvironmentPromotionTargetRegistryError,
    EnvironmentPromotionTargetClientError,
    EnvironmentPromotionPlannerError,
  ) as exc:
    return _render_environment_promotion(
      request,
      selected_release_id=release_id,
      selected_target_environment=target_label,
      promotion_error=str(exc),
    )
  return _render_environment_promotion(
    request,
    selected_release_id=release_id,
    selected_target_environment=target_label,
    target_status=target_status,
    promotion_plan=promotion_plan,
  )

@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_POST
def environment_promotion_approve_package(request):
  """Approve one exact reviewed plan and persist its immutable package."""
  release_id = str(request.POST.get("release_id") or "").strip()
  target_label = str(request.POST.get("target_environment") or "").strip()
  reviewed_plan_id = str(request.POST.get("reviewed_plan_id") or "").strip()
  note = str(request.POST.get("approval_note") or "")
  try:
    target_status, promotion_plan, approval, package = (
      _approve_exact_plan_and_build_package(
        release_id=release_id,
        target_environment_label=target_label,
        reviewed_plan_id=reviewed_plan_id,
        actor=_request_actor(request),
        note=note,
      )
    )
  except (
    ArchitectureReleaseStoreError,
    EnvironmentPromotionApprovalError,
    EnvironmentPromotionApprovalStoreError,
    EnvironmentPromotionDeploymentPackageError,
    EnvironmentPromotionDeploymentPackageStoreError,
    EnvironmentPromotionPlannerError,
    EnvironmentPromotionTargetRegistryError,
    EnvironmentPromotionTargetClientError,
    EnvironmentPromotionUIError,
  ) as exc:
    return _render_environment_promotion(
      request,
      selected_release_id=release_id,
      selected_target_environment=target_label,
      promotion_error=str(exc),
    )
  return _render_environment_promotion(
    request,
    selected_release_id=release_id,
    selected_target_environment=target_label,
    target_status=target_status,
    promotion_plan=promotion_plan,
    promotion_approval=approval,
    deployment_package=package,
  )


@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_POST
def environment_promotion_package_check(request):
  """Run the target runner's live drift check for one exact stored package."""
  target_label = str(request.POST.get("target_environment") or "").strip()
  approval_id = str(request.POST.get("approval_id") or "").strip()
  package_id = str(request.POST.get("package_id") or "").strip()
  try:
    (
      target_status,
      promotion_plan,
      approval,
      package,
      drift_check,
    ) = _check_stored_package(
      target_environment_label=target_label,
      approval_id=approval_id,
      package_id=package_id,
      actor=_request_actor(request),
    )
  except (
    EnvironmentPromotionDeploymentPackageStoreError,
    EnvironmentPromotionTargetRegistryError,
    EnvironmentPromotionTargetClientError,
    EnvironmentPromotionUIError,
  ) as exc:
    return _render_environment_promotion(
      request,
      selected_target_environment=target_label,
      promotion_error=str(exc),
    )
  return _render_environment_promotion(
    request,
    selected_release_id=package["release_id"],
    selected_target_environment=target_label,
    target_status=target_status,
    promotion_plan=promotion_plan,
    promotion_approval=approval,
    deployment_package=package,
    promotion_drift_check=drift_check,
  )


@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_POST
def environment_promotion_package_apply(request):
  """Guardedly apply one exact approved package through its target runner."""
  target_label = str(request.POST.get("target_environment") or "").strip()
  approval_id = str(request.POST.get("approval_id") or "").strip()
  package_id = str(request.POST.get("package_id") or "").strip()
  confirmation = str(request.POST.get("confirm_package_id") or "").strip()
  try:
    (
      target_status,
      promotion_plan,
      approval,
      package,
      apply_result,
    ) = _apply_stored_package(
      target_environment_label=target_label,
      approval_id=approval_id,
      package_id=package_id,
      confirm_package_id=confirmation,
      actor=_request_actor(request),
    )
  except (
    EnvironmentPromotionDeploymentPackageStoreError,
    EnvironmentPromotionTargetRegistryError,
    EnvironmentPromotionTargetClientError,
    EnvironmentPromotionUIError,
  ) as exc:
    return _render_environment_promotion(
      request,
      selected_target_environment=target_label,
      promotion_error=str(exc),
    )
  return _render_environment_promotion(
    request,
    selected_release_id=package["release_id"],
    selected_target_environment=target_label,
    target_status=target_status,
    promotion_plan=promotion_plan,
    promotion_approval=approval,
    deployment_package=package,
    promotion_apply_result=apply_result,
  )


@login_required
@permission_required("metadata.change_targetdataset", raise_exception=True)
@require_GET
def environment_promotion_package_download(
  request,
  target_environment: str,
  approval_id: str,
):
  """Download one exact stored immutable deployment package."""
  try:
    package = EnvironmentPromotionDeploymentPackageStore().load_for_approval(
      target_environment_label=target_environment,
      approval_id=approval_id,
    )
  except EnvironmentPromotionDeploymentPackageStoreError as exc:
    return HttpResponse(str(exc), status=400, content_type="text/plain; charset=utf-8")
  if package is None:
    return HttpResponse(
      "Environment Promotion Deployment Package not found.",
      status=404,
      content_type="text/plain; charset=utf-8",
    )
  payload = serialize_environment_promotion_deployment_package(package)
  response = HttpResponse(
    payload,
    content_type="application/json; charset=utf-8",
  )
  response["Cache-Control"] = "no-store"
  response["Content-Disposition"] = (
    f'attachment; filename="{package.package_id}.deployment.json"'
  )
  return response
