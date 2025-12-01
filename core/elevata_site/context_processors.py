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

from django.apps import apps
from django.conf import settings
from django.urls import reverse, NoReverseMatch
from metadata.constants import (
  SUPPORTED_SQLALCHEMY, BETA_SQLALCHEMY, MANUAL_TYPES,
  TYPE_SUPPORT_LABEL, TYPE_BADGE_CLASS,
)

from metadata.rendering.dialects.dialect_factory import (
  get_available_dialect_names,
  get_active_dialect_name,
)


def type_support(request):
  """
  Inject support info into templates.
  """
  return {
    "SUPPORTED_SQLALCHEMY": sorted(SUPPORTED_SQLALCHEMY),
    "BETA_SQLALCHEMY": sorted(BETA_SQLALCHEMY),
    "MANUAL_TYPES": sorted(MANUAL_TYPES),
    "TYPE_SUPPORT_LABEL": TYPE_SUPPORT_LABEL,
    "TYPE_BADGE_CLASS": TYPE_BADGE_CLASS,
  }

def _safe_reverse(name: str) -> str:
  for candidate in (name, f"metadata:{name}"):
    try:
      return reverse(candidate)
    except NoReverseMatch:
      continue
  return ""

def app_menu(request):
  """
  Dynamically generates the main menu from the models of the 'metadata' app.
  Sort order, exclusions, and label prefix can be configured in settings.ELEVATA_CRUD['metadata'].
  Expected URL-Names: <model_name>_list.
  """
  cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})
  order = cfg.get("order", [])
  exclude = set(cfg.get("exclude", []))
  prefix = cfg.get("prefix", "")
  descriptions = cfg.get("descriptions", {})
  icons = cfg.get("icons", {})

  items = []
  try:
    app_config = apps.get_app_config("metadata")
  except LookupError:
    return {"MAIN_MENU": items}

  models = list(app_config.get_models())
  # Apply exclusions
  models = [m for m in models if m.__name__ not in exclude]

  # Sort: First 'order' then rest by alphabet with verbose_name_plural
  by_name = {m.__name__: m for m in models}
  ordered = [by_name[n] for n in order if n in by_name]
  remaining = sorted(
    [m for n, m in by_name.items() if n not in set(order)],
    key=lambda m: m._meta.verbose_name_plural.lower()
  )
  models_sorted = ordered + remaining

  # Generate menu items
  for model in models_sorted:
    model_name = model._meta.model_name
    label = f"{prefix}{model._meta.verbose_name_plural.title()}"
    url_name = f"{model_name}_list"
    href = _safe_reverse(url_name)
    if not href:
      continue
    desc = descriptions.get(model.__name__, f"Manage {label.lower()}")
    icon = icons.get(model.__name__, "folder")
    items.append({
      "label": label,
      "url_name": url_name,
      "href": href,
      "card_text": desc,
      "icon": icon,
    })

  return {"MAIN_MENU": items}

def crud_ui_config(request):
  """
  UI customization for generic CRUD tables.

  We expose:
  - LIST_TOGGLE_FIELDS: {"SourceColumn": "integrate", ...}
    -> which boolean field can be toggled inline per model

  - BADGE_CLASSES: {"SourceDataset": {"increment_interval": {...}}, ...}
    -> optional mapping field->value->css class for pretty badges
  """
  cfg = getattr(settings, "ELEVATA_CRUD", {}).get("metadata", {})

  list_toggle_fields = cfg.get("list_toggle_fields", {})
  badge_classes = cfg.get("badges", {})

  return {
    "LIST_TOGGLE_FIELDS": list_toggle_fields,
    "BADGE_CLASSES": badge_classes,
  }


def elevata_version(request):
  """Expose current elevata framework version globally."""
  from django.conf import settings
  return {"ELEVATA_VERSION": getattr(settings, "ELEVATA_VERSION", "dev")}


def sql_dialect_context(request):
  """
  Expose available SQL dialects + current active default to all templates.
  """
  choices = get_available_dialect_names()
  try:
    active = get_active_dialect_name()
  except Exception:
    active = choices[0] if choices else None

  return {
    "SQL_DIALECT_CHOICES": choices,
    "SQL_DIALECT_ACTIVE": active,
  }