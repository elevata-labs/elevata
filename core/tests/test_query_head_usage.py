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
from django.contrib.auth.models import Permission
from metadata.models import TargetSchema, TargetDataset, QueryNode

pytestmark = pytest.mark.django_db

def _login_with_perm(client, user, perm_codename="view_targetdataset"):
  p = Permission.objects.get(codename=perm_codename)
  user.user_permissions.add(p)
  user.save()
  client.force_login(user)

def test_query_contract_json_uses_query_head(client, django_user_model, monkeypatch):
  user = django_user_model.objects.create_user("u", password="x")
  _login_with_perm(client, user)

  schema, _ = TargetSchema.objects.get_or_create(short_name="bizcore", schema_name="bizcore")
  td = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_test")

  root = QueryNode.objects.create(target_dataset=td, node_type="select", name="Base", active=True)
  head = QueryNode.objects.create(target_dataset=td, node_type="aggregate", name="Agg", active=True)
  td.query_root = root
  td.query_head = head
  td.save(update_fields=["query_root", "query_head"])

  called = {"node_id": None}

  def fake_infer(node):
    called["node_id"] = node.id
    class CR:
      columns = ["c1"]
      issues = []
    return CR()

  monkeypatch.setattr("metadata.views.infer_query_node_contract", fake_infer)

  url = reverse("api_targetdataset_query_contract", args=[td.pk])
  r = client.get(url)
  assert r.status_code == 200
  payload = r.json()
  assert payload["ok"] is True
  assert payload["has_query_root"] is True
  assert payload["query_root_id"] == root.id
  assert payload["query_head_id"] == head.id
  assert called["node_id"] == head.id

def test_query_tree_view_starts_from_head(client, django_user_model):
  user = django_user_model.objects.create_user("u2", password="x")
  _login_with_perm(client, user)

  schema, _ = TargetSchema.objects.get_or_create(short_name="bizcore", schema_name="bizcore")
  td = TargetDataset.objects.create(target_schema=schema, target_dataset_name="bc_test2")

  root = QueryNode.objects.create(target_dataset=td, node_type="select", name="Base", active=True)
  head = QueryNode.objects.create(target_dataset=td, node_type="window", name="Win", active=True)
  td.query_root = root
  td.query_head = head
  td.save(update_fields=["query_root", "query_head"])

  url = reverse("targetdataset_query_tree", args=[td.pk])
  r = client.get(url)
  assert r.status_code == 200
  ctx = r.context
  assert ctx["has_query_root"] is True
  assert ctx["query_root"].id == root.id
  assert ctx["query_head"].id == head.id
