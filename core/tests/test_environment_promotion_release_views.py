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

from django.urls import reverse

import metadata.views_promotion as views_promotion


def test_create_authoring_release_uses_explicit_runtime_environment(monkeypatch):
  bundle = SimpleNamespace(release_id="rel-1234567890abcdef")
  captured = {}

  monkeypatch.setattr(views_promotion.settings, "ELEVATA_RUNTIME_MODE", "authoring")
  monkeypatch.setattr(views_promotion.settings, "ELEVATA_ENVIRONMENT", "dev")

  def fake_create(**kwargs):
    captured.update(kwargs)
    return bundle

  class FakeStore:
    pass

  store = FakeStore()
  monkeypatch.setattr(
    views_promotion,
    "create_architecture_release_bundle",
    fake_create,
  )
  monkeypatch.setattr(
    views_promotion,
    "store_architecture_release_bundle",
    lambda value, *, store: captured.update(stored=value, store=store),
  )

  result = views_promotion._create_authoring_release(
    release_name="customer-platform",
    release_version="1.0.3",
    description="Promotion UI release",
    actor="operator@example.com",
    release_store=store,
  )

  assert result is bundle
  assert captured["release_name"] == "customer-platform"
  assert captured["release_version"] == "1.0.3"
  assert captured["description"] == "Promotion UI release"
  assert captured["created_by"] == "operator@example.com"
  assert captured["environment_label"] == "dev"
  assert captured["stored"] is bundle
  assert captured["store"] is store


def test_create_authoring_release_fails_closed_without_environment(monkeypatch):
  monkeypatch.setattr(views_promotion.settings, "ELEVATA_RUNTIME_MODE", "authoring")
  monkeypatch.setattr(views_promotion.settings, "ELEVATA_ENVIRONMENT", "")

  try:
    views_promotion._create_authoring_release(
      release_name="customer-platform",
      release_version="1.0.3",
      description="",
      actor="operator@example.com",
    )
  except views_promotion.EnvironmentPromotionUIError as exc:
    assert "ELEVATA_ENVIRONMENT" in str(exc)
  else:
    raise AssertionError("release creation must require an explicit environment")


def test_release_download_response_is_no_store_attachment(monkeypatch):
  bundle = SimpleNamespace(release_id="rel-1234567890abcdef")
  monkeypatch.setattr(
    views_promotion,
    "serialize_architecture_release_bundle",
    lambda value: '{"artifact_type":"architecture_release_bundle"}\n',
  )

  response = views_promotion._architecture_release_download_response(bundle)

  assert response.status_code == 200
  assert response["Cache-Control"] == "no-store"
  assert response["Content-Disposition"] == (
    'attachment; filename="rel-1234567890abcdef.release.json"'
  )
  assert b"architecture_release_bundle" in response.content


def test_release_create_and_download_urls_are_registered():
  assert reverse("environment_promotion_release_create").endswith(
    "/environment-promotion/release/create/"
  )
  assert reverse(
    "environment_promotion_release_download",
    args=["rel-1234567890abcdef"],
  ).endswith(
    "/environment-promotion/release/rel-1234567890abcdef/download/"
  )


def test_next_release_version_suggestion_increments_last_numeric_segment():
  bundles = (SimpleNamespace(release_version="1.0.3"),)
  assert views_promotion._next_release_version_suggestion(bundles) == "1.0.4"


def test_next_release_version_suggestion_preserves_numeric_segment_width():
  bundles = (SimpleNamespace(release_version="2026.08.03"),)
  assert views_promotion._next_release_version_suggestion(bundles) == "2026.08.04"


def test_next_release_version_suggestion_is_blank_for_non_numeric_version():
  bundles = (SimpleNamespace(release_version="2026.08-beta"),)
  assert views_promotion._next_release_version_suggestion(bundles) == ""
