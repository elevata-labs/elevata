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

from metadata.promotion.contracts import (
  MetadataDependencyPhase,
  MetadataIdentityContract,
  MetadataLifecycleStrategy,
  MetadataManagedApplyMode,
  MetadataModelContract,
  MetadataRelationshipContract,
  MetadataTransportRegistry,
)
from metadata.promotion.model_contracts import (
  METADATA_TRANSPORT_REGISTRY,
  REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS,
)
from metadata.promotion.approval import (
  ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_TYPE,
  ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_VERSION,
  EnvironmentPromotionApprovalArtifact,
  EnvironmentPromotionApprovalCheckResult,
  EnvironmentPromotionApprovalDecision,
  EnvironmentPromotionApprovalError,
  EnvironmentPromotionApprovalReview,
  EnvironmentPromotionPlanReference,
  build_environment_promotion_approval,
  check_environment_promotion_approval,
  deserialize_environment_promotion_approval,
  require_environment_promotion_approval,
  serialize_environment_promotion_approval,
)
from metadata.promotion.approval_store import (
  DEFAULT_ENVIRONMENT_PROMOTION_APPROVAL_DIR,
  ENVIRONMENT_PROMOTION_APPROVAL_DIR_ENV,
  EnvironmentPromotionApprovalStore,
  EnvironmentPromotionApprovalStoreError,
  resolve_environment_promotion_approval_dir,
)
from metadata.promotion.apply import (
  EnvironmentPromotionActionApplyError,
  EnvironmentPromotionApplyError,
  EnvironmentPromotionDriftError,
  EnvironmentPromotionExecutor,
  EnvironmentPromotionPostApplyValidationError,
  apply_environment_promotion_plan,
)
from metadata.promotion.deployment import (
  ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_TYPE,
  ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_VERSION,
  EnvironmentPromotionDeploymentPackage,
  EnvironmentPromotionDeploymentPackageError,
  build_environment_promotion_deployment_package,
  deserialize_environment_promotion_deployment_package,
  require_valid_environment_promotion_deployment_package,
  serialize_environment_promotion_deployment_package,
)
from metadata.promotion.plan import (
  ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_TYPE,
  ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_VERSION,
  EnvironmentPromotionAction,
  EnvironmentPromotionActionType,
  EnvironmentPromotionChangeClass,
  EnvironmentPromotionIssue,
  EnvironmentPromotionIssueSeverity,
  EnvironmentPromotionPlan,
  EnvironmentPromotionPlanError,
  EnvironmentPromotionReadiness,
  EnvironmentPromotionReadinessStatus,
  EnvironmentPromotionSubjectType,
  derive_environment_promotion_readiness,
  deserialize_environment_promotion_plan,
  render_environment_promotion_plan_text,
  serialize_environment_promotion_plan,
)
from metadata.promotion.planner import (
  EnvironmentPromotionPlanner,
  EnvironmentPromotionPlannerError,
  build_environment_promotion_plan,
)
from metadata.promotion.record import (
  ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_TYPE,
  ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_VERSION,
  EnvironmentPromotionActionResult,
  EnvironmentPromotionActionResultType,
  EnvironmentPromotionRecord,
  EnvironmentPromotionRecordError,
  deserialize_environment_promotion_record,
  serialize_environment_promotion_record,
)
from metadata.promotion.record_store import (
  DEFAULT_ENVIRONMENT_PROMOTION_HISTORY_DIR,
  ENVIRONMENT_PROMOTION_HISTORY_DIR_ENV,
  EnvironmentPromotionRecordStore,
  EnvironmentPromotionRecordStoreError,
  resolve_environment_promotion_history_dir,
)
from metadata.promotion.release import (
  ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_TYPE,
  ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_VERSION,
  ArchitectureReleaseBundle,
  ArchitectureReleaseBundleError,
  deserialize_architecture_release_bundle,
  serialize_architecture_release_bundle,
)
from metadata.promotion.release_service import (
  create_architecture_release_bundle,
  download_architecture_release_bundle,
  list_architecture_release_bundles,
  store_architecture_release_bundle,
)
from metadata.promotion.release_store import (
  ARCHITECTURE_RELEASE_DIR_ENV,
  DEFAULT_ARCHITECTURE_RELEASE_DIR,
  ArchitectureReleaseStore,
  ArchitectureReleaseStoreError,
  resolve_architecture_release_dir,
)
from metadata.promotion.release_validation import (
  ArchitectureReleaseValidationError,
  ArchitectureReleaseValidationIssue,
  ArchitectureReleaseValidationResult,
  require_valid_architecture_release_bundle,
  validate_architecture_release_bundle,
)
from metadata.promotion.snapshot import (
  ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_TYPE,
  ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_VERSION,
  EnvironmentMetadataObject,
  EnvironmentMetadataPayload,
  EnvironmentMetadataRelationship,
  EnvironmentMetadataSnapshot,
  EnvironmentMetadataSnapshotError,
  deserialize_environment_metadata_snapshot,
  serialize_environment_metadata_snapshot,
)
from metadata.promotion.snapshot_builder import (
  EnvironmentMetadataSnapshotBuildError,
  EnvironmentMetadataSnapshotBuilder,
  build_environment_metadata_snapshot,
)

__all__ = [
  "ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_TYPE",
  "ENVIRONMENT_PROMOTION_APPROVAL_ARTIFACT_VERSION",
  "ENVIRONMENT_PROMOTION_APPROVAL_DIR_ENV",
  "DEFAULT_ENVIRONMENT_PROMOTION_APPROVAL_DIR",
  "EnvironmentPromotionApprovalArtifact",
  "EnvironmentPromotionApprovalCheckResult",
  "EnvironmentPromotionApprovalDecision",
  "EnvironmentPromotionApprovalError",
  "EnvironmentPromotionApprovalReview",
  "EnvironmentPromotionApprovalStore",
  "EnvironmentPromotionApprovalStoreError",
  "EnvironmentPromotionPlanReference",
  "build_environment_promotion_approval",
  "check_environment_promotion_approval",
  "deserialize_environment_promotion_approval",
  "require_environment_promotion_approval",
  "resolve_environment_promotion_approval_dir",
  "serialize_environment_promotion_approval",
  "EnvironmentPromotionActionApplyError",
  "EnvironmentPromotionApplyError",
  "EnvironmentPromotionDriftError",
  "EnvironmentPromotionExecutor",
  "EnvironmentPromotionPostApplyValidationError",
  "apply_environment_promotion_plan",
  "ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_TYPE",
  "ENVIRONMENT_PROMOTION_DEPLOYMENT_PACKAGE_ARTIFACT_VERSION",
  "EnvironmentPromotionDeploymentPackage",
  "EnvironmentPromotionDeploymentPackageError",
  "build_environment_promotion_deployment_package",
  "deserialize_environment_promotion_deployment_package",
  "require_valid_environment_promotion_deployment_package",
  "serialize_environment_promotion_deployment_package",
  "ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_TYPE",
  "ENVIRONMENT_PROMOTION_RECORD_ARTIFACT_VERSION",
  "ENVIRONMENT_PROMOTION_HISTORY_DIR_ENV",
  "DEFAULT_ENVIRONMENT_PROMOTION_HISTORY_DIR",
  "EnvironmentPromotionActionResult",
  "EnvironmentPromotionActionResultType",
  "EnvironmentPromotionRecord",
  "EnvironmentPromotionRecordError",
  "EnvironmentPromotionRecordStore",
  "EnvironmentPromotionRecordStoreError",
  "deserialize_environment_promotion_record",
  "resolve_environment_promotion_history_dir",
  "serialize_environment_promotion_record",
  "ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_TYPE",
  "ENVIRONMENT_PROMOTION_PLAN_ARTIFACT_VERSION",
  "EnvironmentPromotionAction",
  "EnvironmentPromotionActionType",
  "EnvironmentPromotionChangeClass",
  "EnvironmentPromotionIssue",
  "EnvironmentPromotionIssueSeverity",
  "EnvironmentPromotionPlan",
  "EnvironmentPromotionPlanError",
  "EnvironmentPromotionPlanner",
  "EnvironmentPromotionPlannerError",
  "EnvironmentPromotionReadiness",
  "EnvironmentPromotionReadinessStatus",
  "EnvironmentPromotionSubjectType",
  "build_environment_promotion_plan",
  "derive_environment_promotion_readiness",
  "deserialize_environment_promotion_plan",
  "render_environment_promotion_plan_text",
  "serialize_environment_promotion_plan",
  "ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_TYPE",
  "ARCHITECTURE_RELEASE_BUNDLE_ARTIFACT_VERSION",
  "ARCHITECTURE_RELEASE_DIR_ENV",
  "DEFAULT_ARCHITECTURE_RELEASE_DIR",
  "ArchitectureReleaseBundle",
  "ArchitectureReleaseBundleError",
  "ArchitectureReleaseStore",
  "ArchitectureReleaseStoreError",
  "ArchitectureReleaseValidationError",
  "ArchitectureReleaseValidationIssue",
  "ArchitectureReleaseValidationResult",
  "create_architecture_release_bundle",
  "deserialize_architecture_release_bundle",
  "download_architecture_release_bundle",
  "list_architecture_release_bundles",
  "require_valid_architecture_release_bundle",
  "resolve_architecture_release_dir",
  "serialize_architecture_release_bundle",
  "store_architecture_release_bundle",
  "validate_architecture_release_bundle",
  "ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_TYPE",
  "ENVIRONMENT_METADATA_SNAPSHOT_ARTIFACT_VERSION",
  "EnvironmentMetadataObject",
  "EnvironmentMetadataPayload",
  "EnvironmentMetadataRelationship",
  "EnvironmentMetadataSnapshot",
  "EnvironmentMetadataSnapshotBuildError",
  "EnvironmentMetadataSnapshotBuilder",
  "EnvironmentMetadataSnapshotError",
  "build_environment_metadata_snapshot",
  "deserialize_environment_metadata_snapshot",
  "serialize_environment_metadata_snapshot",
  "METADATA_TRANSPORT_REGISTRY",
  "MetadataDependencyPhase",
  "MetadataIdentityContract",
  "MetadataLifecycleStrategy",
  "MetadataManagedApplyMode",
  "MetadataModelContract",
  "MetadataRelationshipContract",
  "MetadataTransportRegistry",
  "REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS",
]
