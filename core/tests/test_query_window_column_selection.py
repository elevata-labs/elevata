"""Tests for contract-driven column selection in query window authoring."""

from types import SimpleNamespace

from django import forms

import metadata.generation.query_contract as query_contract
import metadata.views_scoped as views_scoped


def test_candidate_columns_use_exact_provided_node_contract(monkeypatch):
  """A downstream editor must see the output of its exact input node."""
  upstream = object()
  query_node = SimpleNamespace(aggregate=SimpleNamespace(input_node=upstream))
  seen = []

  def fake_infer(node):
    seen.append(node)
    return SimpleNamespace(output_columns=["group_key", "total_amount"])

  monkeypatch.setattr(query_contract, "infer_query_node_contract", fake_infer)

  result = views_scoped._candidate_column_names_for_targetdataset(
    SimpleNamespace(query_root=None),
    query_node=query_node,
  )

  assert seen == [query_node]
  assert result == ["group_key", "total_amount"]


def test_window_column_argument_uses_window_input_columns(monkeypatch):
  """Column arguments are selected from the Window node input contract."""
  input_node = object()
  target_dataset = object()
  parent = SimpleNamespace(window_node=SimpleNamespace(input_node=input_node))
  view = views_scoped.QueryWindowColumnArgScopedView()

  monkeypatch.setattr(view, "get_parent_object", lambda: parent)
  monkeypatch.setattr(views_scoped, "_get_query_builder_dataset", lambda _obj: target_dataset)

  def fake_candidates(td, query_node=None):
    assert td is target_dataset
    assert query_node is input_node
    return ["customer_id", "loaded_at"]

  monkeypatch.setattr(views_scoped, "_candidate_column_names_for_targetdataset", fake_candidates)

  form = forms.Form()
  form.fields["column_name"] = forms.CharField(required=False)

  enhanced = view.enhance_form(form)

  assert isinstance(enhanced.fields["column_name"].widget, forms.Select)
  assert list(enhanced.fields["column_name"].widget.choices) == [
    ("", "---------"),
    ("customer_id", "customer_id"),
    ("loaded_at", "loaded_at"),
  ]


def test_window_column_argument_resolves_owning_dataset_through_window_column():
  """Window argument mutations must resolve back to the owning TargetDataset."""
  target_dataset = object()
  node = SimpleNamespace(target_dataset=target_dataset)
  window_node = SimpleNamespace(node=node)
  window_column = SimpleNamespace(window_node=window_node)
  argument = SimpleNamespace(window_column=window_column)

  assert views_scoped._td_from_query_instance(argument) is target_dataset


def test_window_column_function_help_points_to_arguments_editor():
  """Parameterized window functions must expose the separate Arguments workflow."""
  view = views_scoped.QueryWindowColumnScopedView()
  form = forms.Form()
  form.fields["function"] = forms.CharField()

  enhanced = view.enhance_form(form)

  assert "use Arguments" in enhanced.fields["function"].help_text
  assert "LAG" in enhanced.fields["function"].help_text


class _FakeQuerySet:
  def __init__(self):
    self.calls = []

  def filter(self, **kwargs):
    self.calls.append(("filter", kwargs))
    return self

  def order_by(self, *fields):
    self.calls.append(("order_by", fields))
    return self


def test_window_column_scopes_reusable_definitions_to_owning_dataset(monkeypatch):
  """Window definition dropdowns must expose only active definitions of the owning dataset."""
  target_dataset = object()
  parent = object()
  view = views_scoped.QueryWindowColumnScopedView()

  monkeypatch.setattr(view, "get_parent_object", lambda: parent)
  monkeypatch.setattr(views_scoped, "_get_query_builder_dataset", lambda _obj: target_dataset)

  order_qs = _FakeQuerySet()
  partition_qs = _FakeQuerySet()
  monkeypatch.setattr(views_scoped.OrderByExpression, "objects", order_qs)
  monkeypatch.setattr(views_scoped.PartitionByExpression, "objects", partition_qs)

  form = forms.Form()
  form.fields["order_by"] = SimpleNamespace(queryset=None, help_text="")
  form.fields["partition_by"] = SimpleNamespace(queryset=None, help_text="")
  form.fields["function"] = forms.CharField()

  enhanced = view.enhance_form(form)

  assert order_qs.calls == [
    ("filter", {"target_dataset": target_dataset, "active": True}),
    ("order_by", ("name", "id")),
  ]
  assert partition_qs.calls == [
    ("filter", {"target_dataset": target_dataset, "active": True}),
    ("order_by", ("name", "id")),
  ]
  assert enhanced.fields["order_by"].queryset is order_qs
  assert enhanced.fields["partition_by"].queryset is partition_qs
  assert "for this dataset" in enhanced.fields["order_by"].help_text
  assert "for this dataset" in enhanced.fields["partition_by"].help_text


def test_aggregate_measure_scopes_order_by_to_owning_dataset(monkeypatch):
  """Aggregate measures must not expose reusable ORDER BY definitions from other datasets."""
  target_dataset = object()
  input_node = object()
  parent = SimpleNamespace(input_node=input_node)
  view = views_scoped.QueryAggregateMeasureScopedView()

  monkeypatch.setattr(view, "get_parent_object", lambda: parent)
  monkeypatch.setattr(views_scoped, "_get_query_builder_dataset", lambda _obj: target_dataset)
  monkeypatch.setattr(
    views_scoped,
    "_candidate_column_names_for_targetdataset",
    lambda _td, query_node=None: ["product_name"],
  )

  order_qs = _FakeQuerySet()
  monkeypatch.setattr(views_scoped.OrderByExpression, "objects", order_qs)

  form = forms.Form()
  form.fields["input_column_name"] = forms.CharField(required=False)
  form.fields["order_by"] = SimpleNamespace(queryset=None, help_text="")

  enhanced = view.enhance_form(form)

  assert order_qs.calls == [
    ("filter", {"target_dataset": target_dataset, "active": True}),
    ("order_by", ("name", "id")),
  ]
  assert enhanced.fields["order_by"].queryset is order_qs
  assert "for this dataset" in enhanced.fields["order_by"].help_text
