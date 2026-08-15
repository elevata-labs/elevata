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

from types import SimpleNamespace

import pytest
from django.http import HttpResponse
from django.test import RequestFactory, override_settings

from elevata_site.promotion_target_middleware import PromotionTargetOnlyMiddleware
from metadata import views_promotion_runner
from metadata.promotion import runner as promotion_runner
from metadata.promotion.runner import (
  EnvironmentPromotionRunnerError,
  apply_environment_promotion_package,
)


TOKEN = "t" * 32


@pytest.mark.django_db
@override_settings(
  ELEVATA_RUNTIME_MODE="promotion_target",
  ELEVATA_ENVIRONMENT="test",
  ELEVATA_PROMOTION_RUNNER_TOKEN=TOKEN,
)
def test_runner_health_requires_bearer_token():
  factory = RequestFactory()
  unauthorized = views_promotion_runner.promotion_runner_health(
    factory.get("/promotion-runner/health/")
  )
  assert unauthorized.status_code == 401

  request = factory.get(
    "/promotion-runner/health/",
    HTTP_AUTHORIZATION=f"Bearer {TOKEN}",
  )
  response = views_promotion_runner.promotion_runner_health(request)
  assert response.status_code == 200
  assert b'"environment_label": "test"' in response.content
  assert response["Cache-Control"] == "no-store"


@override_settings(
  ELEVATA_RUNTIME_MODE="authoring",
  ELEVATA_PROMOTION_RUNNER_TOKEN=TOKEN,
)
def test_runner_is_not_exposed_in_authoring_mode():
  request = RequestFactory().get(
    "/promotion-runner/health/",
    HTTP_AUTHORIZATION=f"Bearer {TOKEN}",
  )
  response = views_promotion_runner.promotion_runner_health(request)
  assert response.status_code == 404


def test_apply_package_requires_exact_package_confirmation(monkeypatch):
  package = SimpleNamespace(
    package_id="dpkg-1234567890abcdef",
    target_environment_label="test",
  )
  monkeypatch.setattr(
    promotion_runner,
    "_require_package_target",
    lambda **kwargs: "test",
  )

  with pytest.raises(EnvironmentPromotionRunnerError, match="confirmation"):
    apply_environment_promotion_package(
      package=package,
      runtime_environment_label="test",
      confirm_package_id="dpkg-0000000000000000",
      applied_by="operator@example.com",
    )


def test_apply_package_delegates_to_guarded_apply(monkeypatch):
  package = SimpleNamespace(
    package_id="dpkg-1234567890abcdef",
    target_environment_label="test",
    plan=object(),
    bundle=object(),
    approval=object(),
  )
  calls = {}
  expected_record = object()

  monkeypatch.setattr(
    promotion_runner,
    "_require_package_target",
    lambda **kwargs: "test",
  )

  def fake_apply(**kwargs):
    calls.update(kwargs)
    return expected_record

  monkeypatch.setattr(
    promotion_runner,
    "apply_environment_promotion_plan",
    fake_apply,
  )

  result = apply_environment_promotion_package(
    package=package,
    runtime_environment_label="test",
    confirm_package_id=package.package_id,
    applied_by="operator@example.com",
  )

  assert result is expected_record
  assert calls["plan"] is package.plan
  assert calls["bundle"] is package.bundle
  assert calls["approval"] is package.approval
  assert calls["runtime_environment_label"] == "test"
  assert calls["applied_by"] == "operator@example.com"


@override_settings(ELEVATA_RUNTIME_MODE="promotion_target")
def test_promotion_target_middleware_blocks_non_runner_routes(monkeypatch):
  factory = RequestFactory()
  middleware = PromotionTargetOnlyMiddleware(lambda request: HttpResponse("ok"))

  monkeypatch.setattr(
    "elevata_site.promotion_target_middleware.resolve",
    lambda path: SimpleNamespace(url_name="architecture_control"),
  )
  blocked = middleware(factory.get("/architecture-control/"))
  assert blocked.status_code == 404

  monkeypatch.setattr(
    "elevata_site.promotion_target_middleware.resolve",
    lambda path: SimpleNamespace(url_name="promotion_runner_health"),
  )
  allowed = middleware(factory.get("/promotion-runner/health/"))
  assert allowed.status_code == 200
