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

from datetime import datetime, timezone
from types import SimpleNamespace

from django.conf import settings
from django.urls import reverse

import pytest

import metadata.views_promotion as views_promotion
from metadata.promotion.release_store import ArchitectureReleaseStoreError


class FakeStore:
  """
  Minimal immutable-store test double.
  """

  def __init__(self, items=(), *, base_path=".test-store", error=None):
    self.items = tuple(items)
    self.base_path = base_path
    self.error = error

  def load_all(self):
    """
    Return configured items or raise the configured store error.
    """
    if self.error is not None:
      raise self.error
    return self.items


def _release(
  *,
  release_id: str,
  version: str,
  created_at: datetime,
  metadata_fingerprint: str,
):
  """
  Build a release-shaped test value.
  """
  return SimpleNamespace(
    release_id=release_id,
    release_name="customer-platform",
    release_version=version,
    description="Release description",
    source_environment_label="dev",
    created_at=created_at,
    created_by="release@example.com",
    bundle_fingerprint=("b" * 64),
    metadata_fingerprint=metadata_fingerprint,
    snapshot=SimpleNamespace(
      metadata=SimpleNamespace(
        objects=(1, 2, 3),
        relationships=(1,),
      )
    ),
  )


def test_environment_promotion_context_is_summary_first_and_newest_first():
  """Verify stored releases are compact and ordered newest first without remote I/O."""
  older = datetime(2026, 8, 13, 8, 0, tzinfo=timezone.utc)
  newer = datetime(2026, 8, 14, 8, 0, tzinfo=timezone.utc)

  release_old = _release(
    release_id="rel-0000000000000001",
    version="1.0.1",
    created_at=older,
    metadata_fingerprint="1" * 64,
  )
  release_new = _release(
    release_id="rel-0000000000000002",
    version="1.0.2",
    created_at=newer,
    metadata_fingerprint="2" * 64,
  )

  context = views_promotion._build_environment_promotion_context(
    release_store=FakeStore((release_old, release_new)),
    targets=(),
  )

  assert context["release_count"] == 2
  assert context["releases"][0]["release_version"] == "1.0.2"
  assert context["releases"][0]["object_count"] == 3
  assert context["releases"][0]["relationship_count"] == 1
  assert context["remote_history"] is None
  assert context["remote_history_error"] == ""


def test_environment_promotion_context_reports_invalid_release_store():
  """Verify invalid immutable release artifacts remain visible fail-closed findings."""
  context = views_promotion._build_environment_promotion_context(
    release_store=FakeStore(
      error=ArchitectureReleaseStoreError("invalid release"),
    ),
    targets=(),
  )

  assert context["releases"] == ()
  assert context["release_error"] == "invalid release"


def test_environment_promotion_url_is_registered():
  """
  Verify the dedicated Environment Promotion workspace route is available.
  """
  assert reverse("environment_promotion").endswith("/environment-promotion/")


def test_environment_promotion_is_a_distinct_main_menu_workspace():
  """
  Verify Environment Promotion is positioned after Architecture Control.
  """
  menu_items = settings.ELEVATA_CRUD["metadata"]["menu_items"]
  item = next(
    value
    for value in menu_items
    if value.get("url_name") == "environment_promotion"
  )

  assert item["label"] == "Environment Promotion"
  assert item["position"] == "after:architecture_control"


def test_environment_promotion_context_lists_targets_without_secrets():
  from metadata.promotion.target_registry import EnvironmentPromotionTarget

  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001",
    bearer_token="secret-" + ("x" * 32),
    timeout_seconds=30,
  )

  context = views_promotion._build_environment_promotion_context(
    release_store=FakeStore(),
    targets=(target,),
  )

  assert context["target_count"] == 1
  assert context["promotion_targets"] == ({
    "environment_label": "test",
    "base_url": "http://127.0.0.1:8001",
    "timeout_seconds": 30,
  },)
  assert "bearer_token" not in context["promotion_targets"][0]


def test_environment_promotion_remote_action_urls_are_registered():
  assert reverse("environment_promotion_target_check").endswith(
    "/environment-promotion/target/check/"
  )
  assert reverse("environment_promotion_history_refresh").endswith(
    "/environment-promotion/history/refresh/"
  )
  assert reverse("environment_promotion_plan").endswith(
    "/environment-promotion/plan/"
  )
  assert reverse("environment_promotion_approve_package").endswith(
    "/environment-promotion/approve-package/"
  )
  assert reverse("environment_promotion_package_check").endswith(
    "/environment-promotion/package/check/"
  )
  assert reverse("environment_promotion_package_apply").endswith(
    "/environment-promotion/package/apply/"
  )
  assert reverse(
    "environment_promotion_package_download",
    kwargs={
      "target_environment": "test",
      "approval_id": "papr-1234567890abcdef",
    },
  ).endswith(
    "/environment-promotion/package/test/papr-1234567890abcdef/download/"
  )


def test_build_target_status_uses_runner_health_snapshot_and_history(monkeypatch):
  from metadata.promotion.target_registry import EnvironmentPromotionTarget

  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001",
    bearer_token="t" * 32,
  )
  snapshot = SimpleNamespace(
    environment_label="test",
    metadata_fingerprint="m" * 64,
    snapshot_fingerprint="s" * 64,
  )

  class FakeClient:
    def __init__(self, configured_target):
      assert configured_target is target

    def health(self):
      return {
        "status": "ok",
        "runtime_mode": "promotion_target",
        "environment_label": "test",
        "elevata_version": settings.ELEVATA_VERSION,
        "metadata_database": "reachable",
      }

    def snapshot(self, *, actor):
      assert actor == "operator@example.com"
      return snapshot

    def history(self):
      return {
        "target_environment_label": "test",
        "records": [{
          "record_id": "prom-1234567890abcdef",
          "release_id": "rel-1234567890abcdef",
          "applied_at": "2026-08-15T04:00:00+00:00",
        }],
      }

  status = views_promotion._build_target_status(
    target,
    actor="operator@example.com",
    client_factory=FakeClient,
  )

  assert status["reachable"] is True
  assert status["metadata_fingerprint"] == "m" * 64
  assert status["snapshot_fingerprint"] == "s" * 64
  assert status["history_count"] == 1
  assert status["history_loaded"] is True
  assert status["last_promotion"]["record_id"] == "prom-1234567890abcdef"


def test_build_remote_history_review_uses_authoritative_runner_history():
  from metadata.promotion.target_registry import EnvironmentPromotionTarget

  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001/metadata",
    bearer_token="t" * 32,
  )
  release = _release(
    release_id="rel-1234567890abcdef",
    version="1.0.3",
    created_at=datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc),
    metadata_fingerprint="m" * 64,
  )

  class FakeClient:
    def __init__(self, configured_target):
      assert configured_target is target

    def health(self):
      return {
        "runtime_mode": "promotion_target",
        "elevata_version": settings.ELEVATA_VERSION,
        "metadata_database": "reachable",
      }

    def history(self):
      return {
        "target_environment_label": "test",
        "records": [{
          "record_id": "prom-1234567890abcdef",
          "record_fingerprint": "f" * 64,
          "release_id": release.release_id,
          "plan_id": "plan-1234567890abcdef",
          "approval_id": "papr-1234567890abcdef",
          "source_environment_label": "dev",
          "target_environment_label": "test",
          "applied_at": "2026-08-15T12:05:00+00:00",
          "applied_by": "operator@example.com",
          "post_target_metadata_fingerprint": release.metadata_fingerprint,
          "summary": {
            "action_count": 8,
            "applied_action_count": 3,
            "verified_action_count": 5,
          },
        }],
      }

  review = views_promotion._build_remote_history_review(
    target,
    release_store=FakeStore((release,)),
    client_factory=FakeClient,
  )

  assert review["environment_label"] == "test"
  assert review["record_count"] == 1
  row = review["records"][0]
  assert row["record_id"] == "prom-1234567890abcdef"
  assert row["release_coordinate"] == "customer-platform 1.0.3"
  assert row["release_metadata_match"] is True
  assert row["plan_id"] == "plan-1234567890abcdef"
  assert row["approval_id"] == "papr-1234567890abcdef"
  assert row["action_count"] == 8


def test_remote_history_rejects_record_for_different_target():
  release = _release(
    release_id="rel-1234567890abcdef",
    version="1.0.3",
    created_at=datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc),
    metadata_fingerprint="m" * 64,
  )
  record = {
    "record_id": "prom-1234567890abcdef",
    "record_fingerprint": "f" * 64,
    "release_id": release.release_id,
    "plan_id": "plan-1234567890abcdef",
    "approval_id": "papr-1234567890abcdef",
    "source_environment_label": "dev",
    "target_environment_label": "prod",
    "applied_at": "2026-08-15T12:05:00+00:00",
    "applied_by": "operator@example.com",
    "post_target_metadata_fingerprint": release.metadata_fingerprint,
    "summary": {
      "action_count": 8,
      "applied_action_count": 3,
      "verified_action_count": 5,
    },
  }

  with pytest.raises(
    views_promotion.EnvironmentPromotionUIError,
    match="environment mismatch",
  ):
    views_promotion._remote_history_row(
      record,
      {release.release_id: release},
      expected_target_environment="test",
    )


def test_build_plan_review_uses_remote_snapshot_without_target_mutation(monkeypatch):
  from metadata.promotion.target_registry import EnvironmentPromotionTarget

  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001",
    bearer_token="t" * 32,
  )
  bundle = SimpleNamespace(
    release_id="rel-1234567890abcdef",
    release_name="customer-platform",
    release_version="1.0.2",
  )
  snapshot = SimpleNamespace(
    environment_label="test",
    metadata_fingerprint="m" * 64,
    snapshot_fingerprint="s" * 64,
  )

  class FakeReleaseStore:
    def load(self, release_id):
      assert release_id == bundle.release_id
      return bundle

  class FakeClient:
    def __init__(self, configured_target):
      assert configured_target is target

    def health(self):
      return {
        "status": "ok",
        "runtime_mode": "promotion_target",
        "environment_label": "test",
        "elevata_version": settings.ELEVATA_VERSION,
        "metadata_database": "reachable",
      }

    def snapshot(self, *, actor):
      assert actor == "operator@example.com"
      return snapshot

  plan = SimpleNamespace(
    plan_id="plan-1234567890abcdef",
    plan_fingerprint="p" * 64,
    release_id=bundle.release_id,
    bundle_fingerprint="b" * 64,
    source_environment_label="dev",
    source_metadata_fingerprint="r" * 64,
    target_environment_label="test",
    target_snapshot_fingerprint=snapshot.snapshot_fingerprint,
    target_metadata_fingerprint=snapshot.metadata_fingerprint,
    readiness=SimpleNamespace(
      status=SimpleNamespace(value="ready"),
      can_apply=True,
      error_count=0,
      warning_count=0,
      issues=(),
    ),
    actions=(
      SimpleNamespace(subject_name="TargetDataset", dependency_phase=50),
      SimpleNamespace(subject_name="TargetColumn", dependency_phase=110),
    ),
    summary={
      "action_count": 2,
      "mutating_action_count": 2,
      "blocked_action_count": 0,
      "by_action_type": {"create": 2},
      "by_change_class": {"additive": 2},
    },
  )

  monkeypatch.setattr(
    views_promotion,
    "build_environment_promotion_plan",
    lambda **kwargs: plan,
  )

  target_status, plan_row = views_promotion._build_plan_review(
    release_id=bundle.release_id,
    target_environment_label="test",
    actor="operator@example.com",
    release_store=FakeReleaseStore(),
    targets=(target,),
    client_factory=FakeClient,
  )

  assert target_status["metadata_fingerprint"] == "m" * 64
  assert target_status["history_count"] is None
  assert target_status["history_loaded"] is False
  assert plan_row["plan_id"] == plan.plan_id
  assert plan_row["readiness_status"] == "ready"
  assert plan_row["mutating_action_count"] == 2
  assert plan_row["by_model"] == (("TargetColumn", 1), ("TargetDataset", 1))


def test_approval_rebuild_fails_closed_when_reviewed_plan_changed(monkeypatch):
  target = SimpleNamespace(environment_label="test")
  reviewed_plan_id = "plan-1111111111111111"
  changed_plan = SimpleNamespace(plan_id="plan-2222222222222222")

  monkeypatch.setattr(
    views_promotion,
    "_build_exact_plan",
    lambda **kwargs: (
      target,
      object(),
      {},
      object(),
      object(),
      changed_plan,
    ),
  )

  with pytest.raises(
    views_promotion.EnvironmentPromotionUIError,
    match="changed after review",
  ):
    views_promotion._approve_exact_plan_and_build_package(
      release_id="rel-1234567890abcdef",
      target_environment_label="test",
      reviewed_plan_id=reviewed_plan_id,
      actor="operator@example.com",
    )


def test_approval_reuses_exact_immutable_approval_and_package(monkeypatch):
  target = SimpleNamespace(
    environment_label="test",
    base_url="http://127.0.0.1:8001/metadata",
  )
  snapshot = SimpleNamespace(
    metadata_fingerprint="m" * 64,
    snapshot_fingerprint="s" * 64,
  )
  health = {
    "runtime_mode": "promotion_target",
    "elevata_version": settings.ELEVATA_VERSION,
    "metadata_database": "reachable",
  }
  bundle = SimpleNamespace(
    release_id="rel-1234567890abcdef",
    release_name="customer-platform",
    release_version="1.0.3",
  )
  plan = SimpleNamespace(
    plan_id="plan-1234567890abcdef",
    plan_fingerprint="p" * 64,
    release_id=bundle.release_id,
    bundle_fingerprint="b" * 64,
    source_environment_label="dev",
    source_metadata_fingerprint="r" * 64,
    target_environment_label="test",
    target_snapshot_fingerprint=snapshot.snapshot_fingerprint,
    target_metadata_fingerprint=snapshot.metadata_fingerprint,
    readiness=SimpleNamespace(
      status=SimpleNamespace(value="ready"),
      can_apply=True,
      error_count=0,
      warning_count=0,
      issues=(),
    ),
    actions=(SimpleNamespace(subject_name="TargetDataset", dependency_phase=50),),
    summary={
      "action_count": 1,
      "mutating_action_count": 1,
      "blocked_action_count": 0,
      "by_action_type": {"create": 1},
      "by_change_class": {"additive": 1},
    },
  )
  approval = SimpleNamespace(
    approval_id="papr-1234567890abcdef",
    artifact_fingerprint="a" * 64,
    plan=SimpleNamespace(
      plan_id=plan.plan_id,
      plan_fingerprint=plan.plan_fingerprint,
      target_environment_label="test",
    ),
    review=SimpleNamespace(
      decision=SimpleNamespace(value="approved"),
      decided_by="operator@example.com",
      decided_at=datetime(2026, 8, 15, 6, 0, tzinfo=timezone.utc),
      note="approved",
    ),
  )
  package = SimpleNamespace(
    package_id="dpkg-1234567890abcdef",
    package_fingerprint="d" * 64,
    bundle=bundle,
    plan=plan,
    approval=approval,
    target_environment_label="test",
    created_at=datetime(2026, 8, 15, 6, 1, tzinfo=timezone.utc),
    created_by="operator@example.com",
  )

  monkeypatch.setattr(
    views_promotion,
    "_build_exact_plan",
    lambda **kwargs: (target, object(), health, snapshot, bundle, plan),
  )

  class FakeApprovalStore:
    def load_for_plan(self, **kwargs):
      return approval

    def save(self, value):
      raise AssertionError("existing approval must be reused")

  class FakePackageStore:
    def load_for_approval(self, **kwargs):
      return package

    def save(self, value):
      raise AssertionError("existing package must be reused")

  _, plan_row, approval_row, package_row = (
    views_promotion._approve_exact_plan_and_build_package(
      release_id=bundle.release_id,
      target_environment_label="test",
      reviewed_plan_id=plan.plan_id,
      actor="operator@example.com",
      approval_store=FakeApprovalStore(),
      package_store=FakePackageStore(),
    )
  )

  assert plan_row["plan_id"] == plan.plan_id
  assert approval_row["approval_id"] == approval.approval_id
  assert package_row["package_id"] == package.package_id


def test_apply_stored_package_requires_exact_typed_confirmation():
  package = SimpleNamespace(package_id="dpkg-1234567890abcdef")

  class FakePackageStore:
    def load_for_approval(self, **kwargs):
      return package

  target = SimpleNamespace(environment_label="test")

  with pytest.raises(
    views_promotion.EnvironmentPromotionUIError,
    match="Final package confirmation",
  ):
    views_promotion._apply_stored_package(
      target_environment_label="test",
      approval_id="papr-1234567890abcdef",
      package_id=package.package_id,
      confirm_package_id="dpkg-ffffffffffffffff",
      actor="operator@example.com",
      package_store=FakePackageStore(),
      targets=(target,),
    )


def test_apply_stored_package_returns_convergence_and_remote_history(monkeypatch):
  target = SimpleNamespace(
    environment_label="test",
    base_url="http://127.0.0.1:8001/metadata",
  )
  bundle = SimpleNamespace(
    release_id="rel-1234567890abcdef",
    release_name="customer-platform",
    release_version="1.0.3",
    bundle_fingerprint="b" * 64,
    metadata_fingerprint="m" * 64,
  )
  plan = SimpleNamespace(
    plan_id="plan-1234567890abcdef",
    plan_fingerprint="p" * 64,
    release_id=bundle.release_id,
    bundle_fingerprint=bundle.bundle_fingerprint,
    source_environment_label="dev",
    source_metadata_fingerprint="r" * 64,
    target_environment_label="test",
    target_snapshot_fingerprint="s" * 64,
    target_metadata_fingerprint="t" * 64,
    readiness=SimpleNamespace(
      status=SimpleNamespace(value="ready"),
      can_apply=True,
      error_count=0,
      warning_count=0,
      issues=(),
    ),
    actions=(
      SimpleNamespace(subject_name="Team", dependency_phase=40),
      SimpleNamespace(subject_name="TargetSchema", dependency_phase=110),
    ),
    summary={
      "action_count": 2,
      "mutating_action_count": 1,
      "blocked_action_count": 0,
      "by_action_type": {"update": 1, "verify": 1},
      "by_change_class": {"mutating": 1, "verification": 1},
    },
  )
  approval = SimpleNamespace(
    approval_id="papr-1234567890abcdef",
    artifact_fingerprint="a" * 64,
    plan=plan,
    review=SimpleNamespace(
      decision=SimpleNamespace(value="approved"),
      decided_by="operator@example.com",
      decided_at=datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc),
      note="approved",
    ),
  )
  package = SimpleNamespace(
    package_id="dpkg-1234567890abcdef",
    package_fingerprint="d" * 64,
    bundle=bundle,
    plan=plan,
    approval=approval,
    target_environment_label="test",
    created_at=datetime(2026, 8, 15, 12, 1, tzinfo=timezone.utc),
    created_by="operator@example.com",
  )
  record = SimpleNamespace(
    record_id="prom-1234567890abcdef",
    record_fingerprint="f" * 64,
    release_id=bundle.release_id,
    plan_id=plan.plan_id,
    approval_id=approval.approval_id,
    target_environment_label="test",
    applied_at=datetime(2026, 8, 15, 12, 2, tzinfo=timezone.utc),
    applied_by="operator@example.com",
    pre_target_metadata_fingerprint="t" * 64,
    post_target_metadata_fingerprint=bundle.metadata_fingerprint,
    post_target_snapshot_fingerprint="z" * 64,
    post_validation_plan_id="plan-ffffffffffffffff",
    post_validation_plan_fingerprint="q" * 64,
    summary={
      "action_count": 2,
      "applied_action_count": 1,
      "verified_action_count": 1,
    },
  )

  class FakePackageStore:
    def load_for_approval(self, **kwargs):
      return package

  class FakeClient:
    def __init__(self, configured_target):
      assert configured_target is target

    def health(self):
      return {
        "runtime_mode": "promotion_target",
        "elevata_version": settings.ELEVATA_VERSION,
        "metadata_database": "reachable",
      }

    def apply(self, *, package, confirm_package_id, actor):
      assert confirm_package_id == package.package_id
      assert actor == "operator@example.com"
      return record

    def history(self):
      return {
        "target_environment_label": "test",
        "records": [{
          "record_id": record.record_id,
          "record_fingerprint": record.record_fingerprint,
          "approval_id": record.approval_id,
          "plan_id": record.plan_id,
          "release_id": record.release_id,
          "post_target_metadata_fingerprint": (
            record.post_target_metadata_fingerprint
          ),
          "applied_at": record.applied_at.isoformat(),
        }],
      }

  target_status, _, _, _, apply_result = views_promotion._apply_stored_package(
    target_environment_label="test",
    approval_id=approval.approval_id,
    package_id=package.package_id,
    confirm_package_id=package.package_id,
    actor="operator@example.com",
    package_store=FakePackageStore(),
    targets=(target,),
    client_factory=FakeClient,
  )

  assert target_status["metadata_fingerprint"] == bundle.metadata_fingerprint
  assert target_status["history_count"] == 1
  assert apply_result["record_id"] == record.record_id
  assert apply_result["release_metadata_match"] is True
  assert apply_result["remote_history_confirmed"] is True
