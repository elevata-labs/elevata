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

from metadata.services.rename_common import commit_rename, RenameSpec
from metadata.models import TargetColumn, TargetDataset, TargetSchema


@pytest.mark.django_db
def test_commit_rename_persists_former_names():
  schema, _ = TargetSchema.objects.get_or_create(
    short_name="rawcore",
    defaults={
      "is_system_managed": True,
      "surrogate_keys_enabled": False,
    },
  )

  td = TargetDataset.objects.create(
    target_schema=schema,
    target_dataset_name="rc_test_commit_rename",
    historize=False,
    is_system_managed=True,
    lineage_key="lk:rc_test_commit_rename",
  )

  col = TargetColumn.objects.create(
    target_dataset=td,
    target_column_name="old_name",
    datatype="STRING",
    nullable=True,
    is_system_managed=True,
    former_names=[],
    active=True,
  )

  spec = RenameSpec(
    name_attr="target_column_name",
    get_scope_qs=lambda: TargetColumn.objects.filter(target_dataset=td),
    validator_context="target_column_name",
    validator_kind="Column",
    collision_label="Column name",
    collision_scope_label="in this dataset",
    extra_update_fields=["former_names"],
  )

  res = commit_rename(col, "new_name", spec)
  assert res.get("ok") is True

  col.refresh_from_db()
  assert col.target_column_name == "new_name"
  assert col.former_names == ["old_name"]
