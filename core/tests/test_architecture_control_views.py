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

from types import SimpleNamespace
from typing import Any

from django.http import HttpResponse
from django.test import RequestFactory

from metadata.architecture.approval import ArchitectureApprovalCheckResult
import metadata.views as views


class FakeQuerySet:
  """
  QuerySet-shaped test double for Architecture Control view tests.
  """

  def select_related(self, *args):
    """
    Return the same query object after select-related configuration.
    """
    return self

  def filter(self, **kwargs):
    """
    Return the same query object after filtering.
    """
    return self

  def order_by(self, *args):
    """
    Return an empty selectable list.
    """
    return []


def _unwrap_view(view_func):
  """
  Return the undecorated view function.
  """
  current = view_func
  while hasattr(current, "__wrapped__"):
    current = current.__wrapped__
  return current


def _patch_messages(monkeypatch) -> list[tuple[str, str]]:
  """
  Patch Django messages and capture emitted messages.
  """
  emitted: list[tuple[str, str]] = []

  def capture(level: str):
    """
    Build a message capture function for one message level.
    """
    def inner(request, message: str) -> None:
      """
      Store the emitted message.
      """
      emitted.append((level, message))

    return inner

  monkeypatch.setattr(views.messages, "success", capture("success"))
  monkeypatch.setattr(views.messages, "warning", capture("warning"))
  monkeypatch.setattr(views.messages, "error", capture("error"))
  return emitted


def _patch_render(monkeypatch) -> dict[str, Any]:
  """
  Patch template rendering and capture context.
  """
  rendered: dict[str, Any] = {}

  def fake_render(request, template_name: str, context: dict[str, Any]):
    """
    Store render arguments and return a simple response.
    """
    rendered["template_name"] = template_name
    rendered["context"] = context
    return HttpResponse("ok")

  monkeypatch.setattr(views, "render", fake_render)
  return rendered


def _patch_redirect(monkeypatch) -> list[str]:
  """
  Patch redirect and capture target URLs.
  """
  redirects: list[str] = []

  def fake_redirect(target_url: str):
    """
    Return a response containing the redirect target.
    """
    redirects.append(target_url)
    return HttpResponse("redirect", status=302)

  monkeypatch.setattr(views, "redirect", fake_redirect)
  return redirects


def _patch_scope_lists(monkeypatch) -> None:
  """
  Patch selectable schema and dataset query objects.
  """
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


def _review_status() -> SimpleNamespace:
  """
  Return a review-status-shaped object for view tests.
  """
  return SimpleNamespace(
    status="pending",
    label="Pending",
    message="Architecture changes are present and have no matching approval.",
    badge_class="text-bg-warning",
    icon="bi-hourglass-split",
    report_fingerprint="report-1",
    approval_id=None,
    artifact_fingerprint=None,
    approval_directory=".elevata/approvals",
    has_changes=True,
    is_blocked=False,
    scope={
      "dataset_keys": ["serving.Customer"],
    },
    state={
      "previous_fingerprint": "previous-state",
      "current_fingerprint": "state-1",
    },
    summary={
      "dataset_change_count": 0,
      "column_change_count": 1,
      "migration_action_count": 1,
      "policy_decision_count": 1,
      "blocking_policy_decision_count": 0,
    },
  )


def test_architecture_control_view_renders_all_scope(
  monkeypatch,
) -> None:
  """
  Verify Architecture Control page rendering for all datasets.
  """
  _patch_scope_lists(monkeypatch)
  rendered = _patch_render(monkeypatch)
  status = _review_status()

  monkeypatch.setattr(
    views,
    "build_architecture_control_context",
    lambda scope: SimpleNamespace(
      scope=scope,
      review_status=status,
    ),
  )
  monkeypatch.setattr(
    views,
    "build_architecture_execution_preview",
    lambda scope, *, control_context=None, no_deps=False: SimpleNamespace(
      gate=SimpleNamespace(status="pending_approval"),
    ),
  )

  request = RequestFactory().get("/architecture-control/?scope_mode=all")
  request.session = {}
  response = _unwrap_view(views.architecture_control)(request)

  assert response.status_code == 200
  assert rendered["template_name"] == "metadata/architecture/architecture_control.html"
  assert rendered["context"]["scope"].mode == "all"
  assert rendered["context"]["review_status"] is status
  assert rendered["context"]["execution_preview"].gate.status == "pending_approval"
  assert rendered["context"]["execution_preview_error"] is None
  assert rendered["context"]["scope_querystring"] == "scope_mode=all"


def test_architecture_control_report_view_returns_text_response(
  monkeypatch,
) -> None:
  """
  Verify text report rendering from the Architecture Control endpoint.
  """
  monkeypatch.setattr(
    views,
    "render_architecture_control_report_text",
    lambda scope: f"{scope.mode}\n",
  )

  request = RequestFactory().get("/architecture-control/report/?scope_mode=all")
  response = _unwrap_view(views.architecture_control_report)(request)

  assert response.status_code == 200
  assert response["Content-Type"] == "text/plain; charset=utf-8"
  assert response.content == b"all\n"


def test_architecture_control_report_download_returns_json_attachment(
  monkeypatch,
) -> None:
  """
  Verify report JSON download response.
  """
  monkeypatch.setattr(
    views,
    "render_architecture_control_report_json",
    lambda scope: '{"report_fingerprint":"report-1"}\n',
  )

  request = RequestFactory().get(
    "/architecture-control/report/download/?scope_mode=all",
  )
  response = _unwrap_view(views.architecture_control_report_download)(request)

  assert response.status_code == 200
  assert response["Content-Type"] == "application/json; charset=utf-8"
  assert response.content == b'{"report_fingerprint":"report-1"}\n'
  assert response["Content-Disposition"] == (
    'attachment; filename="all_architecture_report.json"'
  )


def test_architecture_control_approve_creates_artifact_and_message(
  monkeypatch,
) -> None:
  """
  Verify approval creation from the Architecture Control endpoint.
  """
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)
  calls: dict[str, Any] = {}

  def fake_create_architecture_control_approval(
    scope,
    *,
    approved_by: str,
    note: str,
  ) -> SimpleNamespace:
    """
    Capture approval creation input and return an approval result.
    """
    calls["scope"] = scope
    calls["approved_by"] = approved_by
    calls["note"] = note
    return SimpleNamespace(
      artifact=SimpleNamespace(approval_id="apr_123"),
    )

  monkeypatch.setattr(
    views,
    "create_architecture_control_approval",
    fake_create_architecture_control_approval,
  )
  monkeypatch.setattr(views, "reverse", lambda name: "/architecture-control/")

  request = RequestFactory().post(
    "/architecture-control/approve/",
    data={
      "scope_mode": "all",
      "note": "Reviewed.",
    },
  )
  request.user = SimpleNamespace(email="", username="Ilona")

  response = _unwrap_view(views.architecture_control_approve)(request)

  assert response.status_code == 302
  assert calls["scope"].mode == "all"
  assert calls["approved_by"] == "Ilona"
  assert calls["note"] == "Reviewed."
  assert emitted == [
    ("success", "Architecture approval artifact created: apr_123"),
  ]
  assert redirects == [
    "/architecture-control/?scope_mode=all",
  ]


def test_architecture_control_approval_check_emits_warning_message(
  monkeypatch,
) -> None:
  """
  Verify warning messaging for non-matching approval checks.
  """
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)

  monkeypatch.setattr(
    views,
    "check_architecture_control_approval",
    lambda scope: ArchitectureApprovalCheckResult(
      is_valid=False,
      status="missing",
      message="No approval artifact exists for the report fingerprint.",
      report_fingerprint="report-1",
    ),
  )
  monkeypatch.setattr(views, "reverse", lambda name: "/architecture-control/")

  request = RequestFactory().post(
    "/architecture-control/check/",
    data={
      "scope_mode": "all",
    },
  )

  response = _unwrap_view(views.architecture_control_approval_check)(request)

  assert response.status_code == 302
  assert emitted == [
    ("warning", "No approval artifact exists for the report fingerprint."),
  ]
  assert redirects == [
    "/architecture-control/?scope_mode=all",
  ]


def test_architecture_control_execute_emits_success_message(
  monkeypatch,
) -> None:
  """
  Verify success messaging for controlled Architecture Control execution.
  """
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)

  calls: dict[str, Any] = {}

  def fake_execute_architecture_control_scope(
    scope,
    *,
    actor: str,
    no_deps: bool = False,
  ):
    """
    Capture execution input and return a success result.
    """
    calls["scope"] = scope
    calls["actor"] = actor
    calls["no_deps"] = no_deps
    return SimpleNamespace(
      succeeded=True,
      message="Architecture execution completed for All datasets by Ilona.",
    )

  monkeypatch.setattr(
    views,
    "execute_architecture_control_scope",
    fake_execute_architecture_control_scope,
  )
  monkeypatch.setattr(views, "reverse", lambda name: "/architecture-control/")

  request = RequestFactory().post(
    "/architecture-control/execute/",
    data={
      "scope_mode": "all",
    },
  )
  request.user = SimpleNamespace(email="", username="Ilona")
  request.session = {}

  response = _unwrap_view(views.architecture_control_execute)(request)

  assert response.status_code == 302
  assert calls["scope"].mode == "all"
  assert calls["actor"] == "Ilona"
  assert calls["no_deps"] is False
  assert emitted == [
    ("success", "Architecture execution completed for All datasets by Ilona."),
  ]
  assert redirects[0].startswith(
    "/architecture-control/?scope_mode=all&execution_result_id="
  )
  assert request.session["architecture_control_last_execution"]["status"] == "success"
  result_id = request.session["architecture_control_last_execution"]["result_id"]
  assert result_id
  assert (
    request.session["architecture_control_execution_results"][result_id]["status"]
    == "success"
  )


def test_architecture_control_execute_emits_error_message(
  monkeypatch,
) -> None:
  """
  Verify error messaging for failed controlled Architecture Control execution.
  """
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)

  monkeypatch.setattr(
    views,
    "execute_architecture_control_scope",
    lambda scope, *, actor, no_deps=False: SimpleNamespace(
      succeeded=False,
      message="Execution failed.",
    ),
  )
  monkeypatch.setattr(views, "reverse", lambda name: "/architecture-control/")

  request = RequestFactory().post(
    "/architecture-control/execute/",
    data={
      "scope_mode": "all",
    },
  )
  request.user = SimpleNamespace(email="", username="Ilona")
  request.session = {}

  response = _unwrap_view(views.architecture_control_execute)(request)

  assert response.status_code == 302
  assert emitted == [
    (
      "error",
      "Controlled execution failed. See Last controlled execution for details.",
    ),
  ]
  assert redirects[0].startswith(
    "/architecture-control/?scope_mode=all&execution_result_id="
  )
  assert request.session["architecture_control_last_execution"]["status"] == "failed"
  result_id = request.session["architecture_control_last_execution"]["result_id"]
  assert result_id
  assert (
    request.session["architecture_control_execution_results"][result_id]["status"]
    == "failed"
  )