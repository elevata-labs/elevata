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

from django.http import HttpResponse
from django.template.loader import render_to_string
from django.test import RequestFactory

import generic
import metadata.urls as metadata_urls
import metadata.views as views
from generic import GenericCRUDView
from metadata.models import SourceDataset, System


def test_source_dataset_detail_context_builds_readiness(monkeypatch):
  """Verify SourceDataset detail context delegates to the readiness service."""
  source_dataset = object()
  readiness = object()
  calls = {}

  def fake_build(instance):
    calls["instance"] = instance
    return readiness

  monkeypatch.setattr(views, "build_source_ingestion_readiness", fake_build)

  context = views.source_dataset_detail_context(
    request=RequestFactory().get("/metadata/source-datasets/1/detail/"),
    instance=source_dataset,
  )

  assert calls["instance"] is source_dataset
  assert context == {"source_ingestion_readiness": readiness}


def test_dynamic_source_dataset_view_registers_detail_context_provider():
  """Verify dynamic CRUD generation only registers the SourceDataset provider."""
  source_view = metadata_urls.make_view(SourceDataset)
  system_view = metadata_urls.make_view(System)

  assert source_view.detail_context_provider is views.source_dataset_detail_context
  assert system_view.detail_context_provider is None


def test_generic_detail_view_merges_registered_extra_context(monkeypatch):
  """Verify the shared detail view consumes a dynamic context provider."""
  captured = {}
  obj = SimpleNamespace()

  class FakeMeta:
    model_name = "fake"
    verbose_name = "fake"
    fields = ()
    many_to_many = ()

  class FakeModel:
    _meta = FakeMeta()

  def provider(*, request, instance):
    assert instance is obj
    return {"readiness_marker": "present"}

  class FakeView(GenericCRUDView):
    model = FakeModel
    detail_context_provider = staticmethod(provider)

    def get_related_objects(self, instance):
      return []

  def fake_render(request, template_name, context):
    captured["template_name"] = template_name
    captured["context"] = context
    return HttpResponse("ok")

  monkeypatch.setattr(generic, "get_object_or_404", lambda model, pk: obj)
  monkeypatch.setattr(generic, "render", fake_render)

  response = FakeView().detail(
    RequestFactory().get("/metadata/fake/1/detail/"),
    pk=1,
  )

  assert response.status_code == 200
  assert captured["template_name"] == "generic/detail.html"
  assert captured["context"]["readiness_marker"] == "present"


def test_source_ingestion_readiness_partial_renders_attention_signals_directly():
  """Verify the shared panel renders summary and diagnostic evidence."""
  readiness = SimpleNamespace(
    status="attention",
    source_type="csv",
    metadata_import_mode="automatic",
    landing_required=True,
    ingest_mode="native",
    raw_target_keys=("raw.raw_csv_customer",),
    integrated_column_count=3,
    blocking_signal_count=1,
    warning_signal_count=0,
    signals=(
      SimpleNamespace(
        severity="blocking",
        label="Canonical file URI missing",
        message="Native file execution requires ingestion_config.uri.",
      ),
    ),
  )

  html = render_to_string(
    "metadata/partials/_source_ingestion_readiness_panel.html",
    {"readiness": readiness},
  )

  assert "Source Ingestion Readiness" in html
  assert "Attention" in html
  assert "raw.raw_csv_customer" in html
  assert "Canonical file URI missing" in html
  assert "1 blocking" in html
  assert "Show all diagnostic signals" not in html
  assert "<details" not in html


def test_source_ingestion_readiness_partial_shows_not_applicable_signals_without_toggle():
  """Verify short not-applicable diagnostics are visible without interaction."""
  readiness = SimpleNamespace(
    status="not_applicable",
    source_type="excel",
    metadata_import_mode="automatic",
    landing_required=False,
    ingest_mode="none",
    raw_target_keys=(),
    integrated_column_count=0,
    blocking_signal_count=0,
    warning_signal_count=0,
    signals=(
      SimpleNamespace(
        severity="ok",
        label="Automated metadata import available",
        message="Source type 'excel' supports automated metadata import.",
      ),
      SimpleNamespace(
        severity="info",
        label="Dataset outside integration scope",
        message="The SourceDataset is documented but not selected for integration.",
      ),
    ),
  )

  html = render_to_string(
    "metadata/partials/_source_ingestion_readiness_panel.html",
    {"readiness": readiness},
  )

  assert "Not applicable" in html
  assert "Automated metadata import available" in html
  assert "Dataset outside integration scope" in html
  assert "Show all diagnostic signals" not in html
  assert "<details" not in html


def test_source_ingestion_readiness_partial_uses_explicit_overflow_control():
  """Verify larger diagnostic sets use the established explicit details pattern."""
  readiness = SimpleNamespace(
    status="ready",
    source_type="csv",
    metadata_import_mode="automatic",
    landing_required=True,
    ingest_mode="native",
    raw_target_keys=("raw.raw_csv_customer",),
    integrated_column_count=3,
    blocking_signal_count=0,
    warning_signal_count=0,
    signals=tuple(
      SimpleNamespace(
        severity="ok",
        label=f"Signal {index}",
        message=f"Diagnostic message {index}.",
      )
      for index in range(1, 6)
    ),
  )

  html = render_to_string(
    "metadata/partials/_source_ingestion_readiness_panel.html",
    {"readiness": readiness},
  )

  assert "Showing first 4 of 5 diagnostic signals." in html
  assert "Show remaining diagnostic signals (1)" in html
  assert "Hide remaining diagnostic signals" in html
  assert "Show all diagnostic signals" not in html
  assert "<details" in html
  assert html.count("Signal 1") == 1
  assert html.count("Signal 4") == 1
  assert "Signal 5" in html
