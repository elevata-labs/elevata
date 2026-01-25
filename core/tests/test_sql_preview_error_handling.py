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
from django.urls import reverse


@pytest.mark.django_db
def test_targetdataset_sql_preview_returns_alert_on_render_error(client, django_user_model, monkeypatch):
  # Create a user with permissions (or use your existing helper/fixture if you have one)
  user = django_user_model.objects.create_user(username="u", password="p")

  from django.contrib.auth.models import Permission
  perm = Permission.objects.get(codename="view_targetdataset")
  user.user_permissions.add(perm)
  user.save()

  client.force_login(user)

  # Create a minimal TargetDataset (adjust required fields to your model)
  from metadata.models import TargetDataset, TargetSchema

  schema, _ = TargetSchema.objects.get_or_create(short_name="bizcore", schema_name="bizcore")

  td = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_test")

  # Make render_preview_sql blow up (simulates missing group keys, missing mappings, etc.)
  from metadata import views as metadata_views

  def fake_render_preview_sql(dataset, dialect):
    raise ValueError("Aggregate mode is grouped but no group keys are defined.")

  monkeypatch.setattr(metadata_views, "render_preview_sql", fake_render_preview_sql)

  url = reverse("targetdataset_sql_preview", args=[td.pk])
  resp = client.get(url, {"dialect": "duckdb"})

  assert resp.status_code == 200
  body = resp.content.decode("utf-8")

  # Assert the UI error rendering path is used
  assert "SQL preview failed" in body
  assert "Aggregate mode is grouped but no group keys are defined." in body
