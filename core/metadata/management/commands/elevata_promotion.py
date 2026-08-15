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
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from metadata.promotion.approval import (
  EnvironmentPromotionApprovalDecision,
  EnvironmentPromotionApprovalError,
  build_environment_promotion_approval,
  check_environment_promotion_approval,
  deserialize_environment_promotion_approval,
  serialize_environment_promotion_approval,
)
from metadata.promotion.approval_store import (
  EnvironmentPromotionApprovalStore,
  EnvironmentPromotionApprovalStoreError,
)
from metadata.promotion.apply import (
  EnvironmentPromotionApplyError,
  EnvironmentPromotionDriftError,
  apply_environment_promotion_plan,
)
from metadata.promotion.deployment import (
  EnvironmentPromotionDeploymentPackageError,
  build_environment_promotion_deployment_package,
  deserialize_environment_promotion_deployment_package,
  require_valid_environment_promotion_deployment_package,
  serialize_environment_promotion_deployment_package,
)
from metadata.promotion.plan import (
  EnvironmentPromotionChangeClass,
  EnvironmentPromotionPlanError,
  EnvironmentPromotionReadinessStatus,
  deserialize_environment_promotion_plan,
  render_environment_promotion_plan_text,
  serialize_environment_promotion_plan,
)
from metadata.promotion.planner import (
  EnvironmentPromotionPlannerError,
  build_environment_promotion_plan,
)
from metadata.promotion.record import serialize_environment_promotion_record
from metadata.promotion.record_store import (
  EnvironmentPromotionRecordStore,
  EnvironmentPromotionRecordStoreError,
)
from metadata.promotion.release import (
  ArchitectureReleaseBundleError,
  deserialize_architecture_release_bundle,
)
from metadata.promotion.release_store import (
  ArchitectureReleaseStore,
  ArchitectureReleaseStoreError,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataSnapshotError,
  deserialize_environment_metadata_snapshot,
  serialize_environment_metadata_snapshot,
)
from metadata.promotion.snapshot_builder import (
  EnvironmentMetadataSnapshotBuildError,
  build_environment_metadata_snapshot,
)


EXIT_CHANGES = 1
EXIT_BLOCKED = 2
EXIT_DESTRUCTIVE = 3
EXIT_DRIFT = 4
EXIT_INVALID_PACKAGE = 5
EXIT_OPERATIONAL_ERROR = 6


class Command(BaseCommand):
  help = (
    "Create and validate Environment Promotion artifacts or apply one "
    "approved deployment package."
  )

  def add_arguments(self, parser):
    parser.add_argument(
      "action",
      choices=("snapshot", "plan", "approve", "package", "check", "apply"),
    )
    parser.add_argument("--bundle", dest="bundle_path")
    parser.add_argument("--release-id", dest="release_id")
    parser.add_argument("--release-dir", dest="release_dir")
    parser.add_argument("--target-snapshot", dest="target_snapshot_path")
    parser.add_argument("--target-environment", dest="target_environment")
    parser.add_argument("--plan", dest="plan_path")
    parser.add_argument("--approval", dest="approval_path")
    parser.add_argument("--deployment-package", dest="package_path")
    parser.add_argument("--created-by", dest="created_by")
    parser.add_argument("--decided-by", dest="decided_by")
    parser.add_argument("--applied-by", dest="applied_by")
    parser.add_argument(
      "--decision",
      choices=("approved", "rejected"),
      default="approved",
    )
    parser.add_argument("--note", default="")
    parser.add_argument("--output", dest="output_path")
    parser.add_argument("--approval-dir", dest="approval_dir")
    parser.add_argument("--history-dir", dest="history_dir")
    parser.add_argument(
      "--store",
      action="store_true",
      dest="store_artifact",
      help="Store an approval in the immutable approval store.",
    )
    parser.add_argument(
      "--format",
      choices=("json", "text"),
      default="json",
      dest="output_format",
    )
    parser.add_argument(
      "--compact",
      action="store_true",
      help="Render compact canonical JSON.",
    )
    parser.add_argument(
      "--json",
      action="store_true",
      dest="json_output",
      help="Render check output as JSON.",
    )
    parser.add_argument(
      "--live-target",
      action="store_true",
      dest="live_target",
      help="Check the deployment package against the current target metadata.",
    )
    parser.add_argument(
      "--confirm-package-id",
      dest="confirm_package_id",
      help="Required exact deployment package ID confirmation for apply.",
    )
    parser.add_argument(
      "--fail-on-changes",
      action="store_true",
      dest="fail_on_changes",
    )
    parser.add_argument(
      "--fail-on-destructive",
      action="store_true",
      dest="fail_on_destructive",
    )

  def handle(self, *args, **options):
    action = options["action"]
    try:
      if action == "snapshot":
        self._snapshot(options)
      elif action == "plan":
        self._plan(options)
      elif action == "approve":
        self._approve(options)
      elif action == "package":
        self._package(options)
      elif action == "check":
        self._check(options)
      else:
        self._apply(options)
    except EnvironmentPromotionDriftError as exc:
      raise CommandError(str(exc), returncode=EXIT_DRIFT) from exc
    except EnvironmentPromotionDeploymentPackageError as exc:
      raise CommandError(str(exc), returncode=EXIT_INVALID_PACKAGE) from exc
    except (
      ArchitectureReleaseBundleError,
      ArchitectureReleaseStoreError,
      EnvironmentMetadataSnapshotBuildError,
      EnvironmentMetadataSnapshotError,
      EnvironmentPromotionApprovalError,
      EnvironmentPromotionApprovalStoreError,
      EnvironmentPromotionApplyError,
      EnvironmentPromotionPlanError,
      EnvironmentPromotionPlannerError,
      EnvironmentPromotionRecordStoreError,
      OSError,
      ValueError,
    ) as exc:
      raise CommandError(
        str(exc),
        returncode=EXIT_OPERATIONAL_ERROR,
      ) from exc

  def _snapshot(self, options) -> None:
    environment = _required_option(
      options,
      "target_environment",
      "--target-environment",
      action="snapshot",
    )
    snapshot = build_environment_metadata_snapshot(
      environment_label=environment,
      created_by=options.get("created_by"),
    )
    rendered = serialize_environment_metadata_snapshot(
      snapshot,
      pretty=not bool(options.get("compact")),
    )
    _write_or_stdout(
      command=self,
      output_path=options.get("output_path"),
      content=rendered,
      compact=bool(options.get("compact")),
    )
    self.stderr.write(
      f"Target snapshot fingerprint: {snapshot.snapshot_fingerprint}"
    )
    self.stderr.write(
      f"Target metadata fingerprint: {snapshot.metadata_fingerprint}"
    )

  def _plan(self, options) -> None:
    bundle = _load_bundle(options)
    target_snapshot = _load_or_build_target_snapshot(options)
    plan = build_environment_promotion_plan(
      bundle=bundle,
      target_snapshot=target_snapshot,
    )
    if options.get("output_format") == "text":
      rendered = render_environment_promotion_plan_text(plan)
    else:
      rendered = serialize_environment_promotion_plan(
        plan,
        pretty=not bool(options.get("compact")),
      )
    _write_or_stdout(
      command=self,
      output_path=options.get("output_path"),
      content=rendered,
      compact=bool(options.get("compact")),
    )
    self.stderr.write(f"Plan ID: {plan.plan_id}")
    self.stderr.write(f"Plan fingerprint: {plan.plan_fingerprint}")
    self.stderr.write(f"Readiness: {plan.readiness.status.value}")
    _apply_plan_exit_policy(
      plan=plan,
      fail_on_changes=bool(options.get("fail_on_changes")),
      fail_on_destructive=bool(options.get("fail_on_destructive")),
    )

  def _approve(self, options) -> None:
    plan = _load_plan(_required_option(
      options,
      "plan_path",
      "--plan",
      action="approve",
    ))
    decided_by = _required_option(
      options,
      "decided_by",
      "--decided-by",
      action="approve",
    )
    approval = build_environment_promotion_approval(
      plan=plan,
      decided_by=decided_by,
      note=options.get("note") or "",
      decision=EnvironmentPromotionApprovalDecision(options["decision"]),
    )
    stored_path = None
    if options.get("store_artifact"):
      stored_path = EnvironmentPromotionApprovalStore(
        options.get("approval_dir") or None
      ).save(approval)
    rendered = serialize_environment_promotion_approval(
      approval,
      pretty=not bool(options.get("compact")),
    )
    if options.get("output_path"):
      _write_new_text(
        Path(options["output_path"]),
        _ensure_trailing_newline(rendered),
      )
    elif not stored_path:
      self.stdout.write(rendered, ending="")
    if stored_path:
      self.stdout.write(self.style.SUCCESS(
        f"Stored Environment Promotion Approval: {stored_path}"
      ))
    self.stdout.write(f"Approval ID: {approval.approval_id}")
    self.stdout.write(
      f"Approval fingerprint: {approval.artifact_fingerprint}"
    )

  def _package(self, options) -> None:
    bundle = _load_bundle(options)
    plan = _load_plan(_required_option(
      options,
      "plan_path",
      "--plan",
      action="package",
    ))
    approval = _load_approval(_required_option(
      options,
      "approval_path",
      "--approval",
      action="package",
    ))
    created_by = _required_option(
      options,
      "created_by",
      "--created-by",
      action="package",
    )
    output_path = _required_option(
      options,
      "output_path",
      "--output",
      action="package",
    )
    package = build_environment_promotion_deployment_package(
      bundle=bundle,
      plan=plan,
      approval=approval,
      created_by=created_by,
    )
    rendered = serialize_environment_promotion_deployment_package(
      package,
      pretty=not bool(options.get("compact")),
    )
    _write_new_text(Path(output_path), _ensure_trailing_newline(rendered))
    self.stdout.write(self.style.SUCCESS(
      f"Environment Promotion Deployment Package written to {output_path}"
    ))
    self.stdout.write(f"Package ID: {package.package_id}")
    self.stdout.write(
      f"Package fingerprint: {package.package_fingerprint}"
    )

  def _check(self, options) -> None:
    if not options.get("package_path"):
      self._check_approval(options)
      return

    package = _load_package(options["package_path"])
    require_valid_environment_promotion_deployment_package(package)
    runtime_environment = options.get("target_environment")
    if runtime_environment and runtime_environment != (
      package.target_environment_label
    ):
      raise EnvironmentPromotionDeploymentPackageError(
        "Deployment package target does not match --target-environment."
      )

    live_status = "not_checked"
    live_snapshot_fingerprint = None
    live_metadata_fingerprint = None
    if options.get("live_target"):
      runtime_environment = _required_option(
        options,
        "target_environment",
        "--target-environment",
        action="check --live-target",
      )
      snapshot = build_environment_metadata_snapshot(
        environment_label=runtime_environment,
        created_by="promotion-check",
      )
      live_snapshot_fingerprint = snapshot.snapshot_fingerprint
      live_metadata_fingerprint = snapshot.metadata_fingerprint
      if (
        snapshot.snapshot_fingerprint
        != package.plan.target_snapshot_fingerprint
        or snapshot.metadata_fingerprint
        != package.plan.target_metadata_fingerprint
      ):
        raise EnvironmentPromotionDriftError(
          "Target metadata drift detected since promotion planning."
        )
      live_status = "unchanged"

    result = {
      "is_valid": True,
      "status": "approved_package",
      "package_id": package.package_id,
      "package_fingerprint": package.package_fingerprint,
      "release_id": package.bundle.release_id,
      "plan_id": package.plan.plan_id,
      "approval_id": package.approval.approval_id,
      "target_environment_label": package.target_environment_label,
      "live_target_status": live_status,
      "live_target_snapshot_fingerprint": live_snapshot_fingerprint,
      "live_target_metadata_fingerprint": live_metadata_fingerprint,
    }
    if options.get("json_output"):
      self.stdout.write(json.dumps(
        result,
        sort_keys=True,
        ensure_ascii=False,
        indent=2,
      ))
    else:
      self.stdout.write(self.style.SUCCESS(
        f"Approved deployment package is valid: {package.package_id}"
      ))
      self.stdout.write(
        f"Target environment: {package.target_environment_label}"
      )
      if live_status == "unchanged":
        self.stdout.write("Live target metadata drift: none")


  def _check_approval(self, options) -> None:
    plan = _load_plan(_required_option(
      options,
      "plan_path",
      "--plan",
      action="check",
    ))
    approval = _load_approval(_required_option(
      options,
      "approval_path",
      "--approval",
      action="check",
    ))
    result = check_environment_promotion_approval(
      plan=plan,
      approval=approval,
    )
    if options.get("json_output"):
      self.stdout.write(json.dumps(
        result.to_dict(),
        sort_keys=True,
        ensure_ascii=False,
        indent=2,
      ))
    elif result.is_valid:
      self.stdout.write(self.style.SUCCESS(result.message))
    if not result.is_valid:
      raise EnvironmentPromotionDeploymentPackageError(result.message)

  def _apply(self, options) -> None:
    package = _load_package(_required_option(
      options,
      "package_path",
      "--deployment-package",
      action="apply",
    ))
    confirmation = _required_option(
      options,
      "confirm_package_id",
      "--confirm-package-id",
      action="apply",
    )
    if confirmation != package.package_id:
      raise EnvironmentPromotionDeploymentPackageError(
        "--confirm-package-id does not match the deployment package."
      )
    runtime_environment = _required_option(
      options,
      "target_environment",
      "--target-environment",
      action="apply",
    )
    applied_by = _required_option(
      options,
      "applied_by",
      "--applied-by",
      action="apply",
    )
    record_output_path = (
      Path(options["output_path"])
      if options.get("output_path")
      else None
    )
    if record_output_path is not None and record_output_path.exists():
      raise CommandError(
        f"Immutable artifact output already exists: {record_output_path}",
        returncode=EXIT_OPERATIONAL_ERROR,
      )
    store = EnvironmentPromotionRecordStore(
      options.get("history_dir") or None
    )
    record = apply_environment_promotion_plan(
      plan=package.plan,
      bundle=package.bundle,
      approval=package.approval,
      applied_by=applied_by,
      runtime_environment_label=runtime_environment,
      record_store=store,
    )
    record_path = store.record_file(record)
    if record_output_path is not None:
      _write_new_text(
        record_output_path,
        serialize_environment_promotion_record(record),
      )
    self.stdout.write(self.style.SUCCESS(
      f"Environment Promotion applied successfully: {record.record_id}"
    ))
    self.stdout.write(f"Promotion history: {record_path}")
    self.stdout.write(
      f"Post-apply metadata fingerprint: "
      f"{record.post_target_metadata_fingerprint}"
    )


def _load_bundle(options):
  bundle_path = options.get("bundle_path")
  release_id = options.get("release_id")
  if bool(bundle_path) == bool(release_id):
    raise CommandError(
      "Use exactly one of --bundle or --release-id.",
      returncode=EXIT_OPERATIONAL_ERROR,
    )
  if bundle_path:
    return deserialize_architecture_release_bundle(
      Path(bundle_path).read_text(encoding="utf-8")
    )
  return ArchitectureReleaseStore(
    options.get("release_dir") or None
  ).require(release_id)


def _load_or_build_target_snapshot(options):
  path = options.get("target_snapshot_path")
  environment = options.get("target_environment")
  if path:
    snapshot = deserialize_environment_metadata_snapshot(
      Path(path).read_text(encoding="utf-8")
    )
    if environment and environment != snapshot.environment_label:
      raise CommandError(
        "--target-environment does not match --target-snapshot.",
        returncode=EXIT_OPERATIONAL_ERROR,
      )
    return snapshot
  environment = _required_option(
    options,
    "target_environment",
    "--target-environment",
    action="plan",
  )
  return build_environment_metadata_snapshot(
    environment_label=environment,
    created_by="promotion-plan",
  )


def _load_plan(path: str):
  return deserialize_environment_promotion_plan(
    Path(path).read_text(encoding="utf-8")
  )


def _load_approval(path: str):
  return deserialize_environment_promotion_approval(
    Path(path).read_text(encoding="utf-8")
  )


def _load_package(path: str):
  return deserialize_environment_promotion_deployment_package(
    Path(path).read_text(encoding="utf-8")
  )


def _apply_plan_exit_policy(
  *,
  plan,
  fail_on_changes: bool,
  fail_on_destructive: bool,
) -> None:
  if plan.readiness.status == EnvironmentPromotionReadinessStatus.BLOCKED:
    raise CommandError(
      "Environment Promotion Plan is blocked.",
      returncode=EXIT_BLOCKED,
    )
  if fail_on_destructive and any(
    item.change_class == EnvironmentPromotionChangeClass.DESTRUCTIVE
    for item in plan.actions
  ):
    raise CommandError(
      "Environment Promotion Plan contains destructive actions.",
      returncode=EXIT_DESTRUCTIVE,
    )
  if fail_on_changes and plan.mutating_actions:
    raise CommandError(
      "Environment Promotion Plan contains metadata changes.",
      returncode=EXIT_CHANGES,
    )


def _required_option(
  options,
  key: str,
  flag: str,
  *,
  action: str,
) -> str:
  value = options.get(key)
  if not isinstance(value, str) or not value.strip():
    raise CommandError(
      f"{action} requires {flag}.",
      returncode=EXIT_OPERATIONAL_ERROR,
    )
  return value.strip()


def _write_or_stdout(
  *,
  command: BaseCommand,
  output_path: str | None,
  content: str,
  compact: bool,
) -> None:
  rendered = _ensure_trailing_newline(content) if compact else content
  if output_path:
    _write_new_text(Path(output_path), rendered)
  else:
    command.stdout.write(rendered, ending="")


def _write_new_text(path: Path, content: str) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  try:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
      handle.write(content)
  except FileExistsError as exc:
    raise CommandError(
      f"Immutable artifact output already exists: {path}",
      returncode=EXIT_OPERATIONAL_ERROR,
    ) from exc


def _ensure_trailing_newline(value: str) -> str:
  return value if value.endswith("\n") else value + "\n"
