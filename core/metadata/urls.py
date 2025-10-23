"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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

from django.urls import path
from django.apps import apps
from django.conf import settings
from django.views.generic import RedirectView
from django.utils.text import slugify
from generic import GenericCRUDView
# HTMX views for the import
from . import views
from metadata.models import SourceDataset, SourceSystem

# app_name = "metadata"

# ------------------------------------------------------------
# Configuration (optional, read from settings.ELEVATA_CRUD)
# ------------------------------------------------------------
CFG = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})
ORDER = CFG.get("order", [])
EXCLUDE = set(CFG.get("exclude", []))
PATHS = CFG.get("paths", {})

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------
def make_view(model):
  """Dynamically create a CRUD view class for the given model."""
  attrs = {
    "model": model,
    "template_list": "generic/list.html",
    "template_form": "generic/form.html",
    "template_confirm_delete": "generic/confirm_delete.html",
  }
  return type(f"{model.__name__}CRUDView", (GenericCRUDView,), attrs)

def path_segment_for(model):
  """Derive URL segment from config or model verbose_name_plural."""
  custom = PATHS.get(model.__name__)
  return custom.strip("/") if custom else slugify(model._meta.verbose_name_plural)

def sort_models(models):
  """Return models sorted by ORDER, then alphabetically, excluding EXCLUDE."""
  by_name = {m.__name__: m for m in models if m.__name__ not in EXCLUDE}
  ordered = [by_name[n] for n in ORDER if n in by_name]
  remaining = sorted(
    [m for n, m in by_name.items() if n not in set(ORDER)],
    key=lambda m: m._meta.verbose_name_plural.lower(),
  )
  return ordered + remaining

# ------------------------------------------------------------
# Build urlpatterns dynamically
# ------------------------------------------------------------
app_config = apps.get_app_config("metadata")
models_sorted = sort_models(list(app_config.get_models()))

urlpatterns = []

# Optional redirect: /metadata → first model
if models_sorted:
  first_model_name = models_sorted[0]._meta.model_name
  urlpatterns.append(
    path(
      "",
      RedirectView.as_view(pattern_name=f"{first_model_name}_list", permanent=False),
      name="metadata_index",
    )
  )

# CRUD + HTMX routes per model
for model in models_sorted:
  model_name = model._meta.model_name
  seg = path_segment_for(model)
  view_cls = make_view(model)

  urlpatterns += [
    # Standard CRUD
    path(f"{seg}/", view_cls.as_view(action="list"), name=f"{model_name}_list"),
    path(f"{seg}/new/", view_cls.as_view(action="edit"), name=f"{model_name}_create"),
    path(f"{seg}/<int:pk>/edit/", view_cls.as_view(action="edit"), name=f"{model_name}_edit"),
    path(f"{seg}/<int:pk>/delete/", view_cls.as_view(action="delete"), name=f"{model_name}_delete"),

    # Inline (HTMX)
    path(f"{seg}/<int:pk>/row/", view_cls.as_view(action="row"), name=f"{model_name}_row"),
    path(f"{seg}/<int:pk>/row-edit/", view_cls.as_view(action="row_edit"), name=f"{model_name}_row_edit"),
    path(f"{seg}/row-new/", view_cls.as_view(action="row_new"), name=f"{model_name}_row_new"),
    path(f"{seg}/row-create/", view_cls.as_view(action="row_create"), name=f"{model_name}_row_create"),
    path(f"{seg}/<int:pk>/row-delete/", view_cls.as_view(action="row_delete"), name=f"{model_name}_row_delete"),

    # Detail
    path(f"{seg}/<int:pk>/detail/", view_cls.as_view(action="detail"), name=f"{model_name}_detail"),
    path("source-type-hint/", views.source_type_hint, name="source_type_hint"),
  ]

# Custom import endpoints (UI-Buttons → HTMX POST)
ds_seg = path_segment_for(SourceDataset)
sys_seg = path_segment_for(SourceSystem)

urlpatterns += [
  path(f"{ds_seg}/<int:pk>/import/", views.import_dataset_metadata, name="sourcedataset_import_metadata"),
  path(f"{sys_seg}/<int:pk>/import/", views.import_system_metadata, name="sourcesystem_import_metadata"),
]

# ------------------------------------------------------------
# Debug: log models on startup
# ------------------------------------------------------------
print(
  f"[elevata] Registered CRUD routes for: "
  + ", ".join([m.__name__ for m in models_sorted])
)
