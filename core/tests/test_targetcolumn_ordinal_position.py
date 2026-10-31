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

import pytest
from django.core.exceptions import ValidationError

from metadata.forms import TargetColumnForm
from metadata.models import TargetColumn, TargetDataset, TargetSchema


@pytest.mark.django_db
def test_target_column_form_rejects_duplicate_ordinal_position():
  schema = TargetSchema.objects.create(
    short_name="bizcore_ordinal_validation",
    display_name="Business Core Ordinal Validation",
    schema_name="bizcore_ordinal_validation",
  )
  dataset = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="bc_ordinal_validation",
  )
  TargetColumn.objects.create(
    target_dataset=dataset,
    target_column_name="first_column",
    ordinal_position=1,
    datatype="STRING",
  )

  form = TargetColumnForm(instance=TargetColumn(target_dataset=dataset))
  form.cleaned_data = {"ordinal_position": 1}

  with pytest.raises(ValidationError, match="already used"):
    form.clean_ordinal_position()
