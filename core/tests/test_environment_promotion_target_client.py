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

from io import BytesIO
import json
from types import SimpleNamespace

import pytest
from urllib.error import HTTPError

from metadata.promotion import target_client
from metadata.promotion.target_client import (
  EnvironmentPromotionTargetClient,
  EnvironmentPromotionTargetClientError,
)
from metadata.promotion.target_registry import (
  EnvironmentPromotionTarget,
  EnvironmentPromotionTargetRegistryError,
  load_environment_promotion_targets,
)


TOKEN = "t" * 32


class FakeResponse:
  def __init__(self, payload: str):
    self.payload = payload.encode("utf-8")

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc, tb):
    return False

  def read(self, limit):
    return self.payload[:limit]


class FakeOpener:
  def __init__(self, payload: dict):
    self.payload = payload
    self.requests = []

  def open(self, request, timeout):
    self.requests.append((request, timeout))
    return FakeResponse(json.dumps(self.payload))


def test_target_registry_loads_explicit_targets_and_keeps_tokens_out_of_repr(monkeypatch):
  monkeypatch.setenv("ELEVATA_PROMOTION_TARGETS", "test,prod-eu")
  monkeypatch.setenv("ELEVATA_PROMOTION_TARGET_TEST_URL", "http://127.0.0.1:8001/")
  monkeypatch.setenv("ELEVATA_PROMOTION_TARGET_TEST_TOKEN", TOKEN)
  monkeypatch.setenv("ELEVATA_PROMOTION_TARGET_PROD_EU_URL", "https://prod.example/elevata/")
  monkeypatch.setenv("ELEVATA_PROMOTION_TARGET_PROD_EU_TOKEN", "p" * 32)
  monkeypatch.setenv("ELEVATA_PROMOTION_TARGET_TIMEOUT_SECONDS", "45")

  targets = load_environment_promotion_targets()

  assert [target.environment_label for target in targets] == ["test", "prod-eu"]
  assert targets[0].base_url == "http://127.0.0.1:8001"
  assert targets[1].base_url == "https://prod.example/elevata"
  assert targets[0].timeout_seconds == 45
  assert TOKEN not in repr(targets[0])


def test_target_registry_fails_closed_when_declared_target_has_no_token(monkeypatch):
  monkeypatch.setenv("ELEVATA_PROMOTION_TARGETS", "test")
  monkeypatch.setenv("ELEVATA_PROMOTION_TARGET_TEST_URL", "http://127.0.0.1:8001")
  monkeypatch.delenv("ELEVATA_PROMOTION_TARGET_TEST_TOKEN", raising=False)

  with pytest.raises(EnvironmentPromotionTargetRegistryError, match="32 characters"):
    load_environment_promotion_targets()


def test_target_client_health_uses_bearer_token_and_validates_identity():
  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001",
    bearer_token=TOKEN,
    timeout_seconds=12,
  )
  opener = FakeOpener({
    "status": "ok",
    "runtime_mode": "promotion_target",
    "environment_label": "test",
    "elevata_version": "2.18.0",
    "metadata_database": "reachable",
  })

  result = EnvironmentPromotionTargetClient(target, opener=opener).health()

  assert result["status"] == "ok"
  request, timeout = opener.requests[0]
  assert request.full_url == "http://127.0.0.1:8001/promotion-runner/health/"
  assert request.get_header("Authorization") == f"Bearer {TOKEN}"
  assert timeout == 12


def test_target_client_rejects_runner_environment_mismatch():
  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001",
    bearer_token=TOKEN,
  )
  opener = FakeOpener({
    "status": "ok",
    "runtime_mode": "promotion_target",
    "environment_label": "prod",
    "elevata_version": "2.18.0",
    "metadata_database": "reachable",
  })

  with pytest.raises(EnvironmentPromotionTargetClientError, match="identity mismatch"):
    EnvironmentPromotionTargetClient(target, opener=opener).health()


def test_target_client_snapshot_sends_actor_and_checks_snapshot_environment(monkeypatch):
  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001",
    bearer_token=TOKEN,
  )
  opener = FakeOpener({"artifact_type": "environment_metadata_snapshot"})
  snapshot = SimpleNamespace(environment_label="test")
  monkeypatch.setattr(
    target_client,
    "deserialize_environment_metadata_snapshot",
    lambda payload: snapshot,
  )

  result = EnvironmentPromotionTargetClient(target, opener=opener).snapshot(
    actor="operator@example.com"
  )

  assert result is snapshot
  request, _ = opener.requests[0]
  assert request.get_header("X-elevata-actor") == "operator@example.com"


def test_target_client_check_posts_exact_package_and_accepts_drift_409(monkeypatch):
  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001/metadata",
    bearer_token=TOKEN,
    timeout_seconds=12,
  )
  package = SimpleNamespace(package_id="dpkg-1234567890abcdef")
  response_payload = {
    "status": "drift",
    "is_unchanged": False,
    "package_id": package.package_id,
    "package_fingerprint": "p" * 64,
    "release_id": "rel-1234567890abcdef",
    "plan_id": "plan-1234567890abcdef",
    "approval_id": "papr-1234567890abcdef",
    "target_environment_label": "test",
    "expected_snapshot_fingerprint": "a" * 64,
    "expected_metadata_fingerprint": "b" * 64,
    "live_snapshot_fingerprint": "c" * 64,
    "live_metadata_fingerprint": "d" * 64,
  }

  class DriftOpener:
    def __init__(self):
      self.requests = []

    def open(self, request, timeout):
      self.requests.append((request, timeout))
      raise HTTPError(
        request.full_url,
        409,
        "Conflict",
        {},
        BytesIO(json.dumps(response_payload).encode("utf-8")),
      )

  opener = DriftOpener()
  monkeypatch.setattr(
    target_client,
    "serialize_environment_promotion_deployment_package",
    lambda value: '{"package":"exact"}\n',
  )

  result = EnvironmentPromotionTargetClient(target, opener=opener).check(
    package=package,
    actor="operator@example.com",
  )

  assert result["status"] == "drift"
  request, timeout = opener.requests[0]
  assert request.full_url == (
    "http://127.0.0.1:8001/metadata/promotion-runner/check/"
  )
  assert request.method == "POST"
  assert request.data == b'{"package":"exact"}\n'
  assert request.get_header("Authorization") == f"Bearer {TOKEN}"
  assert request.get_header("X-elevata-actor") == "operator@example.com"
  assert timeout == 12


def test_target_client_apply_posts_exact_confirmation_and_validates_record(monkeypatch):
  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001/metadata",
    bearer_token=TOKEN,
    timeout_seconds=12,
  )
  package = SimpleNamespace(
    package_id="dpkg-1234567890abcdef",
    bundle=SimpleNamespace(
      release_id="rel-1234567890abcdef",
      bundle_fingerprint="b" * 64,
      metadata_fingerprint="m" * 64,
    ),
    plan=SimpleNamespace(
      plan_id="plan-1234567890abcdef",
      plan_fingerprint="p" * 64,
      actions=(1, 2),
    ),
    approval=SimpleNamespace(
      approval_id="papr-1234567890abcdef",
      artifact_fingerprint="a" * 64,
    ),
  )
  record = SimpleNamespace(
    plan_id=package.plan.plan_id,
    plan_fingerprint=package.plan.plan_fingerprint,
    approval_id=package.approval.approval_id,
    approval_fingerprint=package.approval.artifact_fingerprint,
    release_id=package.bundle.release_id,
    bundle_fingerprint=package.bundle.bundle_fingerprint,
    target_environment_label="test",
    post_target_metadata_fingerprint=package.bundle.metadata_fingerprint,
    summary={"action_count": 2},
  )
  opener = FakeOpener({"record": "exact"})
  monkeypatch.setattr(
    target_client,
    "serialize_environment_promotion_deployment_package",
    lambda value: '{"package":"exact"}\n',
  )
  monkeypatch.setattr(
    target_client,
    "deserialize_environment_promotion_record",
    lambda payload: record,
  )

  result = EnvironmentPromotionTargetClient(target, opener=opener).apply(
    package=package,
    confirm_package_id=package.package_id,
    actor="operator@example.com",
  )

  assert result is record
  request, timeout = opener.requests[0]
  assert request.full_url == (
    "http://127.0.0.1:8001/metadata/promotion-runner/apply/"
  )
  assert request.method == "POST"
  assert request.data == b'{"package":"exact"}\n'
  assert request.get_header("X-elevata-confirm-package-id") == package.package_id
  assert request.get_header("X-elevata-applied-by") == "operator@example.com"
  assert timeout == 12


def test_target_client_apply_rejects_confirmation_mismatch():
  target = EnvironmentPromotionTarget(
    environment_label="test",
    base_url="http://127.0.0.1:8001/metadata",
    bearer_token=TOKEN,
  )
  package = SimpleNamespace(package_id="dpkg-1234567890abcdef")

  with pytest.raises(
    EnvironmentPromotionTargetClientError,
    match="confirmation does not match",
  ):
    EnvironmentPromotionTargetClient(target, opener=FakeOpener({})).apply(
      package=package,
      confirm_package_id="dpkg-ffffffffffffffff",
      actor="operator@example.com",
    )
