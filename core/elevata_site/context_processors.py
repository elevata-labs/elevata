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
    model_name = model._meta.model_name # lowercase
    label = f"{prefix}{model._meta.verbose_name_plural.title()}"
    url_name = f"{model_name}_list"
    items.append({"label": label, "url_name": url_name})

  return {"MAIN_MENU": items}