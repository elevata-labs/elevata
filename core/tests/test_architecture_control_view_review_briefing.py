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

from metadata import views
from metadata.architecture.control import (
  ArchitectureControlContext,
  ArchitectureControlScope,
)


class FakeQuerySet:
  """
  Minimal chainable QuerySet test double for Architecture Control view tests.
  """

  def __init__(self, values: tuple[Any, ...] = ()):
    self.values = values

  def order_by(self, *fields: str) -> "FakeQuerySet":
    """
    Return the same fake queryset for ordered access.
    """
    return self

  def select_related(self, *fields: str) -> "FakeQuerySet":
    """
    Return the same fake queryset for related-object access.
    """
    return self

  def filter(self, **kwargs: Any) -> "FakeQuerySet":
    """
    Return the same fake queryset for filtered access.
    """
    return self

  def __iter__(self):
    """
    Return an iterator over configured values.
    """
    return iter(self.values)


class FakeExecutionRecordStore:
  """
  Execution record store test double for Architecture Control view tests.
  """

  def list_records(self, *args: Any, **kwargs: Any) -> tuple[Any, ...]:
    """
    Return no execution history records.
    """
    return ()


def _undecorated_architecture_control_view():
  """
  Return the undecorated Architecture Control view for direct context testing.
  """
  return views.architecture_control.__wrapped__.__wrapped__


def test_architecture_control_view_adds_review_briefing_context(
  monkeypatch,
) -> None:
  """
  Verify Architecture Control passes the Review Briefing into the template context.
  """
  request = RequestFactory().get("/architecture/control/", {"scope_mode": "all"})
  scope = ArchitectureControlScope.for_all()
  context = ArchitectureControlContext(
    scope=scope,
    report=SimpleNamespace(report_fingerprint="report-1"),
    review_status=SimpleNamespace(status="approved"),
    approval_store=SimpleNamespace(),
  )
  preview = SimpleNamespace(gate=SimpleNamespace(can_execute=True))
  briefing = SimpleNamespace(scope_key="all")
  captured: dict[str, Any] = {}
  briefing_call: dict[str, Any] = {}

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
    "ArchitectureExecutionRecordStore",
    FakeExecutionRecordStore,
  )
  monkeypatch.setattr(
    views,
    "_architecture_control_scope_from_params",
    lambda params: scope,
  )
  monkeypatch.setattr(
    views,
    "build_architecture_control_context",
    lambda value: context,
  )
  monkeypatch.setattr(
    views,
    "build_architecture_execution_preview",
    lambda *args, **kwargs: preview,
  )

  def fake_build_architecture_review_briefing(**kwargs: Any) -> Any:
    """
    Capture Review Briefing inputs and return the configured briefing.
    """
    briefing_call.update(kwargs)
    return briefing

  def fake_render(request, template_name: str, context: dict[str, Any]) -> HttpResponse:
    """
    Capture the template context passed by the view.
    """
    captured["template_name"] = template_name
    captured["context"] = context
    return HttpResponse("ok")

  monkeypatch.setattr(
    views,
    "build_architecture_review_briefing",
    fake_build_architecture_review_briefing,
  )
  monkeypatch.setattr(views, "render", fake_render)

  response = _undecorated_architecture_control_view()(request)

  assert response.status_code == 200
  assert captured["template_name"] == "metadata/architecture/architecture_control.html"
  assert captured["context"]["review_briefing"] is briefing
  assert briefing_call == {
    "control_context": context,
    "scope": scope,
    "execution_preview": preview,
    "execution_preview_error": None,
  }


def test_architecture_control_view_skips_review_briefing_without_report(
  monkeypatch,
) -> None:
  """
  Verify incomplete view-level test doubles do not break Architecture Control.
  """
  request = RequestFactory().get("/architecture/control/", {"scope_mode": "all"})
  scope = ArchitectureControlScope.for_all()
  context = SimpleNamespace(
    scope=scope,
    review_status=SimpleNamespace(status="pending"),
  )
  captured: dict[str, Any] = {}

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
    "ArchitectureExecutionRecordStore",
    FakeExecutionRecordStore,
  )
  monkeypatch.setattr(
    views,
    "_architecture_control_scope_from_params",
    lambda params: scope,
  )
  monkeypatch.setattr(
    views,
    "build_architecture_control_context",
    lambda value: context,
  )
  monkeypatch.setattr(
    views,
    "build_architecture_execution_preview",
    lambda *args, **kwargs: SimpleNamespace(gate=SimpleNamespace(can_execute=False)),
  )

  def fail_build_architecture_review_briefing(**kwargs: Any) -> Any:
    """
    Fail when the optional briefing builder is called for incomplete input.
    """
    raise AssertionError("Review Briefing must be skipped without a report.")

  def fake_render(request, template_name: str, context: dict[str, Any]) -> HttpResponse:
    """
    Capture the template context passed by the view.
    """
    captured["template_name"] = template_name
    captured["context"] = context
    return HttpResponse("ok")

  monkeypatch.setattr(
    views,
    "build_architecture_review_briefing",
    fail_build_architecture_review_briefing,
  )
  monkeypatch.setattr(views, "render", fake_render)

  response = _undecorated_architecture_control_view()(request)

  assert response.status_code == 200
  assert captured["template_name"] == "metadata/architecture/architecture_control.html"
  assert captured["context"]["scope"] is scope
  assert captured["context"]["review_briefing"] is None
  assert captured["context"]["error_message"] is None
