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

from django.urls import reverse_lazy
from generic import GenericCRUDView
from .models import Team, Person, PartialLoad, SourceSystem, SourceDataset, SourceDatasetOwnership, SourceColumn, TargetDataset, TargetDatasetOwnership, TargetColumn, TargetDatasetReference

# ------------------------------------------------------------
# CRUD views for the models
# ------------------------------------------------------------
class TeamCRUDView(GenericCRUDView):
  model = Team
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class PersonCRUDView(GenericCRUDView):
  model = Person
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class PartialLoadCRUDView(GenericCRUDView):
  model = PartialLoad
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class SourceSystemCRUDView(GenericCRUDView):
  model = SourceSystem
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class SourceDatasetCRUDView(GenericCRUDView):
  model = SourceDataset
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class SourceDatasetOwnershipCRUDView(GenericCRUDView):
  model = SourceDatasetOwnership
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class SourceColumnCRUDView(GenericCRUDView):
  model = SourceColumn
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class TargetDatasetCRUDView(GenericCRUDView):
  model = TargetDataset
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class TargetDatasetOwnershipCRUDView(GenericCRUDView):
  model = TargetDatasetOwnership
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class TargetColumnCRUDView(GenericCRUDView):
  model = TargetColumn
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"

class TargetDatasetReferenceCRUDView(GenericCRUDView):
  model = TargetDatasetReference
  template_list = "generic/list.html"
  template_form = "generic/form.html"
  template_confirm_delete = "generic/confirm_delete.html"
