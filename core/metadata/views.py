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
from core.generic import GenericCRUDView

def make_crud_view(model):
  """Dynamically create a CRUD view for a given model."""
  return type(
    f"{model.__name__}CRUDView",
    (GenericCRUDView,),
    {
      "model": model,
      "template_list": "generic/list.html",
      "template_form": "generic/form.html",
      "template_confirm_delete": "generic/confirm_delete.html",
    },
  )

# Dynamisch alle Models im metadata-App zu CRUD-Views machen
metadata_models = apps.get_app_config("metadata").get_models()
globals().update({
  f"{model.__name__}CRUDView": make_crud_view(model)
  for model in metadata_models
})