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

import pytest
from django.http import HttpResponse
from django.test import RequestFactory

from metadata.architecture.approval import ArchitectureApprovalCheckResult
import metadata.views as views

"""
Tests for Architecture Operations UI views.
"""

def _target_dataset() -> SimpleNamespace:
  """
  Return a TargetDataset-shaped object for view tests.
  """
  return SimpleNamespace(
    pk=42,
    target_schema=SimpleNamespace(short_name="serving"),
    target_dataset_name="Customer",
  )


def _unwrap_view(view_func):
  """
  Return the undecorated view function.
  """
  current = view_func
  while hasattr(current, "__wrapped__"):
    current = current.__wrapped__
  return current


def _patch_target_lookup(
  monkeypatch: pytest.MonkeyPatch,
  target_dataset: SimpleNamespace,
) -> None:
  """
  Patch TargetDataset lookup for direct view tests.
  """
  monkeypatch.setattr(
    views,
    "get_object_or_404",
    lambda *args, **kwargs: target_dataset,
  )


def _patch_redirect(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
  """
  Patch redirect and capture redirect targets.
  """
  redirects: list[tuple[str, dict[str, Any]]] = []

  def fake_redirect(name: str, *args, **kwargs) -> HttpResponse:
    """
    Return a response containing the redirect target.
    """
    redirects.append((name, kwargs))
    return HttpResponse("redirect", status=302)

  monkeypatch.setattr(views, "redirect", fake_redirect)
  return redirects


def _patch_messages(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
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


def test_architecture_report_view_returns_text_response(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify text report rendering from the UI endpoint.
  """
  target_dataset = _target_dataset()
  _patch_target_lookup(monkeypatch, target_dataset)
  monkeypatch.setattr(
    views,
    "render_target_dataset_architecture_report_text",
    lambda target: "Architecture Change Report\n",
  )

  request = RequestFactory().get("/target-datasets/42/architecture-review/report/")
  response = _unwrap_view(views.targetdataset_architecture_report)(
    request,
    pk=42,
  )

  assert response.status_code == 200
  assert response["Content-Type"] == "text/plain; charset=utf-8"
  assert response.content == b"Architecture Change Report\n"


def test_architecture_report_view_returns_operation_errors(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify report rendering error responses.
  """
  target_dataset = _target_dataset()
  _patch_target_lookup(monkeypatch, target_dataset)

  def raise_error(target):
    """
    Raise an architecture operation error.
    """
    raise views.ArchitectureOperationsError("Scope could not be resolved.")

  monkeypatch.setattr(
    views,
    "render_target_dataset_architecture_report_text",
    raise_error,
  )

  request = RequestFactory().get("/target-datasets/42/architecture-review/report/")
  response = _unwrap_view(views.targetdataset_architecture_report)(
    request,
    pk=42,
  )

  assert response.status_code == 400
  assert response["Content-Type"] == "text/plain; charset=utf-8"
  assert response.content == b"Scope could not be resolved."


def test_architecture_report_download_returns_json_attachment(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify report JSON download response.
  """
  target_dataset = _target_dataset()
  _patch_target_lookup(monkeypatch, target_dataset)
  monkeypatch.setattr(
    views,
    "render_target_dataset_architecture_report_json",
    lambda target: '{"report_fingerprint":"report-1"}\n',
  )

  request = RequestFactory().get(
    "/target-datasets/42/architecture-review/report/download/",
  )
  response = _unwrap_view(views.targetdataset_architecture_report_download)(
    request,
    pk=42,
  )

  assert response.status_code == 200
  assert response["Content-Type"] == "application/json; charset=utf-8"
  assert response.content == b'{"report_fingerprint":"report-1"}\n'
  assert response["Content-Disposition"] == (
    'attachment; filename="serving_Customer_architecture_report.json"'
  )


def test_architecture_approve_view_creates_artifact_and_message(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify approval creation from the UI endpoint.
  """
  target_dataset = _target_dataset()
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)
  calls: dict[str, Any] = {}

  _patch_target_lookup(monkeypatch, target_dataset)

  def fake_create_target_dataset_architecture_approval(
    target,
    *,
    approved_by: str,
    note: str,
  ) -> SimpleNamespace:
    """
    Capture approval creation input and return an approval result.
    """
    calls["target"] = target
    calls["approved_by"] = approved_by
    calls["note"] = note
    return SimpleNamespace(
      artifact=SimpleNamespace(approval_id="apr_123"),
    )

  monkeypatch.setattr(
    views,
    "create_target_dataset_architecture_approval",
    fake_create_target_dataset_architecture_approval,
  )

  request = RequestFactory().post(
    "/target-datasets/42/architecture-review/approve/",
    data={"note": "Reviewed."},
  )
  request.user = SimpleNamespace(email="", username="Ilona")

  response = _unwrap_view(views.targetdataset_architecture_approve)(
    request,
    pk=42,
  )

  assert response.status_code == 302
  assert calls == {
    "target": target_dataset,
    "approved_by": "Ilona",
    "note": "Reviewed.",
  }
  assert emitted == [
    ("success", "Architecture approval artifact created: apr_123"),
  ]
  assert redirects == [
    ("targetdataset_architecture_review", {"pk": 42}),
  ]


def test_architecture_approve_view_reports_operation_errors(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify approval creation error handling.
  """
  target_dataset = _target_dataset()
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)
  _patch_target_lookup(monkeypatch, target_dataset)

  def raise_error(target, *, approved_by: str, note: str) -> None:
    """
    Raise an architecture operation error.
    """
    raise views.ArchitectureOperationsError("Report is blocked by policy.")

  monkeypatch.setattr(
    views,
    "create_target_dataset_architecture_approval",
    raise_error,
  )

  request = RequestFactory().post(
    "/target-datasets/42/architecture-review/approve/",
    data={"note": "Reviewed."},
  )
  request.user = SimpleNamespace(email="", username="Ilona")

  response = _unwrap_view(views.targetdataset_architecture_approve)(
    request,
    pk=42,
  )

  assert response.status_code == 302
  assert emitted == [("error", "Report is blocked by policy.")]
  assert redirects == [
    ("targetdataset_architecture_review", {"pk": 42}),
  ]


def test_architecture_approval_check_view_emits_success_message(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify success messaging for valid approval checks.
  """
  target_dataset = _target_dataset()
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)
  _patch_target_lookup(monkeypatch, target_dataset)
  monkeypatch.setattr(
    views,
    "check_target_dataset_architecture_approval",
    lambda target: ArchitectureApprovalCheckResult(
      is_valid=True,
      status="approved",
      message="Approval artifact matches the architecture change report.",
      report_fingerprint="report-1",
      approval_id="apr_123",
      artifact_fingerprint="artifact-1",
    ),
  )

  request = RequestFactory().post(
    "/target-datasets/42/architecture-review/check/",
  )
  response = _unwrap_view(views.targetdataset_architecture_approval_check)(
    request,
    pk=42,
  )

  assert response.status_code == 302
  assert emitted == [
    ("success", "Approval artifact matches the architecture change report."),
  ]
  assert redirects == [
    ("targetdataset_architecture_review", {"pk": 42}),
  ]


def test_architecture_approval_check_view_emits_warning_message(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """
  Verify warning messaging for non-matching approval checks.
  """
  target_dataset = _target_dataset()
  emitted = _patch_messages(monkeypatch)
  redirects = _patch_redirect(monkeypatch)
  _patch_target_lookup(monkeypatch, target_dataset)
  monkeypatch.setattr(
    views,
    "check_target_dataset_architecture_approval",
    lambda target: ArchitectureApprovalCheckResult(
      is_valid=False,
      status="drift",
      message="An approval exists for this scope, but it is bound to a different architecture report fingerprint.",
      report_fingerprint="report-1",
      approval_id="apr_old",
      artifact_fingerprint="artifact-old",
    ),
  )

  request = RequestFactory().post(
    "/target-datasets/42/architecture-review/check/",
  )
  response = _unwrap_view(views.targetdataset_architecture_approval_check)(
    request,
    pk=42,
  )

  assert response.status_code == 302
  assert emitted == [
    (
      "warning",
      "An approval exists for this scope, but it is bound to a different architecture report fingerprint.",
    ),
  ]
  assert redirects == [
    ("targetdataset_architecture_review", {"pk": 42}),
  ]