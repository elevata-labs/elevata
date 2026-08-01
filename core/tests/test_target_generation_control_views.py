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

from django.http import HttpResponse
from django.template.loader import get_template
from django.test import RequestFactory

import metadata.views as views
from metadata.generation.target_generation_operations import (
  TargetGenerationOperationsError,
)


class FakeQuerySet:
  """Minimal QuerySet-shaped object for Architecture Control rendering."""

  def select_related(self, *args):
    return self

  def filter(self, **kwargs):
    return self

  def order_by(self, *args):
    return []


class FakeSession(dict):
  """Dictionary session test double with Django's modified flag."""

  modified = False


def _unwrap_view(view_func):
  current = view_func
  while hasattr(current, "__wrapped__"):
    current = current.__wrapped__
  return current


def _patch_messages(monkeypatch):
  emitted = []
  for level in ("success", "warning", "error"):
    monkeypatch.setattr(
      views.messages,
      level,
      lambda request, message, level=level: emitted.append((level, message)),
    )
  return emitted


def _patch_redirect(monkeypatch):
  redirects = []

  def fake_redirect(target_url):
    redirects.append(target_url)
    return HttpResponse("redirect", status=302)

  monkeypatch.setattr(views, "redirect", fake_redirect)
  monkeypatch.setattr(views, "reverse", lambda name, *args: "/architecture-control/")
  return redirects


def _generation_context():
  return SimpleNamespace(
    plan=SimpleNamespace(plan_fingerprint="1" * 64),
    review=SimpleNamespace(review_fingerprint="2" * 64),
  )


def test_architecture_control_builds_generation_preview_for_generated_schema(
  monkeypatch,
) -> None:
  rendered = {}
  generation_context = _generation_context()
  review_status = SimpleNamespace(status="no_changes")
  calls = {}

  monkeypatch.setattr(
    views,
    "TargetSchema",
    SimpleNamespace(objects=FakeQuerySet()),
  )
  monkeypatch.setattr(
    views,
    "TargetDataset",
    SimpleNamespace(objects=FakeQuerySet()),
  )
  monkeypatch.setattr(
    views,
    "build_architecture_control_context",
    lambda scope: SimpleNamespace(
      scope=scope,
      review_status=review_status,
    ),
  )
  monkeypatch.setattr(
    views,
    "build_architecture_execution_preview",
    lambda scope, *, control_context=None, no_deps=False: SimpleNamespace(
      gate=SimpleNamespace(status="ready_no_changes"),
    ),
  )
  monkeypatch.setattr(
    views,
    "build_target_generation_operations_context",
    lambda schema_short_name, *, actor=None: (
      calls.update({"schema": schema_short_name, "actor": actor})
      or generation_context
    ),
  )
  generation_sequence = SimpleNamespace(all_up_to_date=False)
  monkeypatch.setattr(
    views,
    "build_target_generation_sequence_context",
    lambda **kwargs: (
      calls.update({"sequence": kwargs})
      or generation_sequence
    ),
  )
  monkeypatch.setattr(
    views,
    "render",
    lambda request, template_name, context: (
      rendered.update({"template": template_name, "context": context})
      or HttpResponse("ok")
    ),
  )

  request = RequestFactory().get(
    "/architecture-control/?scope_mode=schema&schema_short=raw",
  )
  request.user = SimpleNamespace(username="Ilona")
  request.session = FakeSession()

  response = _unwrap_view(views.architecture_control)(request)

  assert response.status_code == 200
  assert calls["schema"] == "raw"
  assert calls["actor"] is request.user
  assert calls["sequence"] == {
    "selected_schema_short_name": "raw",
    "actor": request.user,
    "selected_context": generation_context,
  }
  assert rendered["context"]["generation_context"] is generation_context
  assert rendered["context"]["generation_sequence"] is generation_sequence
  assert rendered["context"]["generation_error"] is None
  assert rendered["context"]["generation_querystring"] == (
    "scope_mode=schema&schema_short=raw&generation_review_fingerprint="
    + "2" * 64
  )


def test_generation_plan_download_uses_exact_visible_review(monkeypatch) -> None:
  context = _generation_context()
  monkeypatch.setattr(
    views,
    "_build_target_generation_request_context",
    lambda request: (
      {"scope_mode": "schema", "schema_short": "raw"},
      "raw",
      context,
    ),
  )
  monkeypatch.setattr(
    views,
    "render_target_generation_plan_json",
    lambda plan: '{"artifact_type":"target_generation_plan"}\n',
  )

  request = RequestFactory().get(
    "/architecture-control/generation/plan/download/",
  )
  response = _unwrap_view(
    views.architecture_control_generation_plan_download,
  )(request)

  assert response.status_code == 200
  assert response["Content-Type"] == "application/json; charset=utf-8"
  assert response.content == b'{"artifact_type":"target_generation_plan"}\n'
  assert response["Content-Disposition"] == (
    'attachment; filename="target_generation_raw_111111111111.plan.json"'
  )


def test_generation_review_download_uses_exact_visible_review(monkeypatch) -> None:
  context = _generation_context()
  monkeypatch.setattr(
    views,
    "_build_target_generation_request_context",
    lambda request: (
      {"scope_mode": "schema", "schema_short": "raw"},
      "raw",
      context,
    ),
  )
  monkeypatch.setattr(
    views,
    "render_target_generation_review_json",
    lambda review: '{"artifact_type":"target_generation_review"}\n',
  )

  request = RequestFactory().get(
    "/architecture-control/generation/review/download/",
  )
  response = _unwrap_view(
    views.architecture_control_generation_review_download,
  )(request)

  assert response.status_code == 200
  assert response["Content-Type"] == "application/json; charset=utf-8"
  assert response.content == b'{"artifact_type":"target_generation_review"}\n'
  assert response["Content-Disposition"] == (
    'attachment; filename="target_generation_raw_222222222222.review.json"'
  )


def test_generation_approval_view_binds_submitted_review(monkeypatch) -> None:
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)
  calls = {}
  expected_review = "2" * 64

  def fake_create(schema_short_name, **kwargs):
    calls["schema"] = schema_short_name
    calls.update(kwargs)
    return SimpleNamespace(
      artifact=SimpleNamespace(approval_id="gpa_123"),
      context=SimpleNamespace(
        review=SimpleNamespace(review_fingerprint=expected_review),
      ),
    )

  monkeypatch.setattr(
    views,
    "create_target_generation_operations_approval",
    fake_create,
  )

  request = RequestFactory().post(
    "/architecture-control/generation/approve/",
    data={
      "scope_mode": "schema",
      "schema_short": "raw",
      "generation_review_fingerprint": expected_review,
      "note": "Reviewed in the UI.",
    },
  )
  request.user = SimpleNamespace(email="", username="Ilona")

  response = _unwrap_view(views.architecture_control_generation_approve)(request)

  assert response.status_code == 302
  assert calls["schema"] == "raw"
  assert calls["expected_review_fingerprint"] == expected_review
  assert calls["approved_by"] == "Ilona"
  assert calls["note"] == "Reviewed in the UI."
  assert calls["actor"] is request.user
  assert emitted == [
    (
      "success",
      "Generation Approval created: gpa_123 for review 222222222222.",
    ),
  ]
  assert redirects == [
    "/architecture-control/?scope_mode=schema&schema_short=raw#target-generation",
  ]


def test_generation_approval_check_view_reports_valid_match(monkeypatch) -> None:
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)
  expected_review = "2" * 64
  calls = {}

  def fake_check(schema_short_name, **kwargs):
    calls["schema"] = schema_short_name
    calls.update(kwargs)
    return SimpleNamespace(
      is_valid=True,
      message="Generation Approval matches the current Target Generation Review.",
    )

  monkeypatch.setattr(
    views,
    "check_target_generation_operations_approval",
    fake_check,
  )

  request = RequestFactory().post(
    "/architecture-control/generation/check/",
    data={
      "scope_mode": "schema",
      "schema_short": "raw",
      "generation_review_fingerprint": expected_review,
    },
  )
  request.user = SimpleNamespace(username="Ilona")

  response = _unwrap_view(
    views.architecture_control_generation_approval_check,
  )(request)

  assert response.status_code == 302
  assert calls == {
    "schema": "raw",
    "expected_review_fingerprint": expected_review,
    "actor": request.user,
  }
  assert emitted == [
    (
      "success",
      "Generation Approval matches the current Target Generation Review.",
    ),
  ]
  assert redirects == [
    "/architecture-control/?scope_mode=schema&schema_short=raw#target-generation",
  ]


def test_generation_apply_view_surfaces_result_and_residual_plan(monkeypatch) -> None:
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)
  expected_review = "2" * 64
  operation_result = SimpleNamespace(
    apply_result=SimpleNamespace(
      converged=False,
      summary_text=(
        "Applied Target Generation Plan 111111111111: "
        "1 planned actions consumed; 1 residual actions."
      ),
    ),
  )
  monkeypatch.setattr(
    views,
    "apply_target_generation_operations_plan",
    lambda schema_short_name, **kwargs: operation_result,
  )
  monkeypatch.setattr(
    views,
    "_store_target_generation_result",
    lambda request, result: "result-1",
  )

  request = RequestFactory().post(
    "/architecture-control/generation/apply/",
    data={
      "scope_mode": "schema",
      "schema_short": "raw",
      "generation_review_fingerprint": expected_review,
    },
  )
  request.user = SimpleNamespace(username="Ilona")

  response = _unwrap_view(views.architecture_control_generation_apply)(request)

  assert response.status_code == 302
  assert emitted == [
    (
      "warning",
      "Applied Target Generation Plan 111111111111: 1 planned actions "
      "consumed; 1 residual actions. Review and approve the residual plan "
      "before the next guarded apply.",
    ),
  ]
  assert redirects == [
    "/architecture-control/?scope_mode=schema&schema_short=raw&"
    "generation_result_id=result-1#target-generation",
  ]


def test_generation_apply_view_rejects_stale_preview(monkeypatch) -> None:
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)
  monkeypatch.setattr(
    views,
    "apply_target_generation_operations_plan",
    lambda *args, **kwargs: (_ for _ in ()).throw(
      TargetGenerationOperationsError(
        "Target Generation preview changed. Review the refreshed plan before applying it."
      )
    ),
  )

  request = RequestFactory().post(
    "/architecture-control/generation/apply/",
    data={
      "scope_mode": "schema",
      "schema_short": "raw",
      "generation_review_fingerprint": "2" * 64,
    },
  )
  request.user = SimpleNamespace(username="Ilona")

  response = _unwrap_view(views.architecture_control_generation_apply)(request)

  assert response.status_code == 302
  assert emitted == [
    (
      "error",
      "Target Generation preview changed. Review the refreshed plan before applying it.",
    ),
  ]
  assert redirects == [
    "/architecture-control/?scope_mode=schema&schema_short=raw#target-generation",
  ]


def test_architecture_control_template_compiles_generation_workflow() -> None:
  template = get_template("metadata/architecture/architecture_control.html")
  assert template is not None


def test_architecture_control_all_scope_builds_generation_sequence(
  monkeypatch,
) -> None:
  rendered = {}
  generation_sequence = SimpleNamespace(all_up_to_date=False)
  review_status = SimpleNamespace(status="no_changes")
  calls = {}

  monkeypatch.setattr(
    views,
    "TargetSchema",
    SimpleNamespace(objects=FakeQuerySet()),
  )
  monkeypatch.setattr(
    views,
    "TargetDataset",
    SimpleNamespace(objects=FakeQuerySet()),
  )
  monkeypatch.setattr(
    views,
    "build_architecture_control_context",
    lambda scope: SimpleNamespace(
      scope=scope,
      review_status=review_status,
    ),
  )
  monkeypatch.setattr(
    views,
    "build_architecture_execution_preview",
    lambda scope, *, control_context=None, no_deps=False: SimpleNamespace(
      gate=SimpleNamespace(status="ready_no_changes"),
    ),
  )
  monkeypatch.setattr(
    views,
    "build_target_generation_sequence_context",
    lambda **kwargs: (
      calls.update(kwargs)
      or generation_sequence
    ),
  )
  monkeypatch.setattr(
    views,
    "render",
    lambda request, template_name, context: (
      rendered.update({"template": template_name, "context": context})
      or HttpResponse("ok")
    ),
  )

  request = RequestFactory().get(
    "/architecture-control/?scope_mode=all",
  )
  request.user = SimpleNamespace(username="Ilona")
  request.session = FakeSession()

  response = _unwrap_view(views.architecture_control)(request)

  assert response.status_code == 200
  assert calls == {
    "selected_schema_short_name": "",
    "actor": request.user,
    "selected_context": None,
  }
  assert rendered["context"]["generation_context"] is None
  assert rendered["context"]["generation_sequence"] is generation_sequence
  assert rendered["context"]["generation_sequence_error"] is None
