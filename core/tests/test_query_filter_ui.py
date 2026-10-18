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

import json
from types import SimpleNamespace

from django.urls import reverse

import generic
import metadata.views as views
import metadata.views_scoped as views_scoped
from metadata.forms import QueryFilterAuthoringForm


def _predicate(column_name="customer_no"):
  return {
    "version": 1,
    "expression": {
      "type": "comparison",
      "operator": ">",
      "left": {"type": "column", "name": column_name},
      "right": {"type": "literal", "value": 100},
    },
  }




def test_query_filter_authoring_form_preserves_input_column_order():
  form = QueryFilterAuthoringForm(
    column_names=(
      "customer",
      "rc_aw_customer_key",
      "customer_id",
      "account_no",
      "person_id",
      "rc_aw_person_key",
      "first_name",
      "last_name",
    ),
  )

  assert form.fields["predicate"].widget.column_names == [
    "customer",
    "rc_aw_customer_key",
    "customer_id",
    "account_no",
    "person_id",
    "rc_aw_person_key",
    "first_name",
    "last_name",
  ]


def test_query_filter_authoring_form_accepts_input_contract_column():
  form = QueryFilterAuthoringForm(
    {"predicate": json.dumps(_predicate())},
    column_names=("customer_no", "title_name"),
  )

  assert form.is_valid(), form.errors
  assert form.cleaned_data["predicate"] == _predicate()


def test_query_filter_authoring_form_rejects_unknown_input_column():
  form = QueryFilterAuthoringForm(
    {"predicate": json.dumps(_predicate("missing_column"))},
    column_names=("customer_no",),
  )

  assert form.is_valid() is False
  assert "outside the input query contract" in str(form.errors["predicate"])


def test_query_filter_authoring_form_rejects_non_predicate_root():
  contract = {
    "version": 1,
    "expression": {"type": "column", "name": "customer_no"},
  }
  form = QueryFilterAuthoringForm(
    {"predicate": json.dumps(contract)},
    column_names=("customer_no",),
  )

  assert form.is_valid() is False


def test_query_filter_column_names_uses_input_node_contract(monkeypatch):
  input_node = object()
  monkeypatch.setattr(
    views,
    "infer_query_node_contract",
    lambda node: SimpleNamespace(columns=("customer_no", "title_name")),
  )

  assert views._query_filter_column_names(input_node) == ["customer_no", "title_name"]


def test_query_filter_routes_are_registered():
  assert reverse("targetdataset_add_filter_node", args=[42]).endswith(
    "/metadata/target-datasets/42/query/add-filter/"
  )
  assert reverse("queryfilter_edit", args=[7]).endswith(
    "/metadata/query-filter/7/edit/"
  )


def test_query_filter_is_classified_as_query_scoped_model():
  assert "QueryFilterNode" in views_scoped.QUERY_SCOPED_MODEL_NAMES


def test_query_node_deactivation_is_blocked_when_filter_uses_it(monkeypatch):
  class QueryNode:
    pass

  class _QuerySet:
    def __init__(self, exists):
      self._exists = exists

    def exists(self):
      return self._exists

  class _Manager:
    def __init__(self, exists):
      self._exists = exists

    def filter(self, **kwargs):
      return _QuerySet(self._exists)

  models = {
    "QueryWindowNode": SimpleNamespace(objects=_Manager(False)),
    "QueryFilterNode": SimpleNamespace(objects=_Manager(True)),
    "QueryAggregateNode": SimpleNamespace(objects=_Manager(False)),
    "QueryUnionBranch": SimpleNamespace(objects=_Manager(False)),
  }
  monkeypatch.setattr(
    generic.apps,
    "get_model",
    lambda app_label, model_name: models[model_name],
  )

  assert generic._is_deactivation_blocked(QueryNode()) is True
