"""Regression tests for Window required-input pushdown."""

from types import SimpleNamespace

from metadata.rendering.builder import _build_window_plan_for_node
from metadata.rendering.expr import ColumnRef, WindowFunction
from metadata.rendering.logical_plan import LogicalSelect, SelectItem, SourceTable


class _Items(list):
  def all(self):
    return self

  def filter(self, **kwargs):
    items = self
    for name, value in kwargs.items():
      items = [item for item in items if getattr(item, name, None) == value]
    return _Items(items)

  def order_by(self, *args):
    return self


def _window_column():
  return SimpleNamespace(
    active=True,
    ordinal_position=1,
    id=1,
    function="SUM",
    output_name="window_sum_product_id",
    args=_Items([
      SimpleNamespace(
        ordinal_position=1,
        id=1,
        arg_type="column",
        column_name="product_id",
        int_value=None,
        str_value="",
      )
    ]),
    partition_by=SimpleNamespace(
      items=_Items([
        SimpleNamespace(
          ordinal_position=1,
          id=1,
          input_column_name="product_model_id",
        )
      ])
    ),
    order_by=SimpleNamespace(
      items=_Items([
        SimpleNamespace(
          ordinal_position=1,
          id=1,
          input_column_name="product_id",
          direction="ASC",
        )
      ])
    ),
    frame_type="",
    frame_start="",
    frame_start_offset=None,
    frame_end="",
    frame_end_offset=None,
  )


def test_window_pushes_hidden_partition_column_without_leaking_it(monkeypatch):
  input_node = SimpleNamespace(id=10)
  window = SimpleNamespace(
    input_node=input_node,
    columns=_Items([_window_column()]),
  )
  node = SimpleNamespace(window=window)

  monkeypatch.setattr(
    "metadata.rendering.builder.infer_query_node_contract",
    lambda _node: SimpleNamespace(
      columns=["product_id"],
      available_input_columns=["product_id", "product_model_id"],
      issues=[],
    ),
  )

  captured = {}

  def _inner(_node, required_input_columns=None):
    captured["required"] = set(required_input_columns or set())
    return LogicalSelect(
      from_=SourceTable(schema="rawcore", name="rc_aw_product", alias="s"),
      select_list=[
        SelectItem(expr=ColumnRef(table_alias="s", column_name="product_id"), alias="product_id"),
        SelectItem(
          expr=ColumnRef(table_alias="s", column_name="product_model_id"),
          alias="product_model_id",
        ),
      ],
    )

  monkeypatch.setattr("metadata.rendering.builder._build_plan_for_query_node", _inner)

  plan = _build_window_plan_for_node(node)

  assert captured["required"] == {"product_id", "product_model_id"}
  assert [item.alias for item in plan.select_list] == [
    "product_id",
    "window_sum_product_id",
  ]
  assert "product_model_id" not in [item.alias for item in plan.select_list]

  window_expr = plan.select_list[-1].expr
  assert isinstance(window_expr, WindowFunction)
  assert window_expr.window.partition_by[0].column_name == "product_model_id"


def test_window_output_required_downstream_is_not_pushed_to_input(monkeypatch):
  input_node = SimpleNamespace(id=11)
  window = SimpleNamespace(
    input_node=input_node,
    columns=_Items([_window_column()]),
  )
  node = SimpleNamespace(window=window)

  monkeypatch.setattr(
    "metadata.rendering.builder.infer_query_node_contract",
    lambda _node: SimpleNamespace(
      columns=["product_id"],
      available_input_columns=["product_id", "product_model_id"],
      issues=[],
    ),
  )

  captured = {}

  def _inner(_node, required_input_columns=None):
    captured["required"] = set(required_input_columns or set())
    return LogicalSelect(
      from_=SourceTable(schema="rawcore", name="rc_aw_product", alias="s"),
      select_list=[
        SelectItem(expr=ColumnRef(table_alias="s", column_name="product_id"), alias="product_id"),
        SelectItem(
          expr=ColumnRef(table_alias="s", column_name="product_model_id"),
          alias="product_model_id",
        ),
      ],
    )

  monkeypatch.setattr("metadata.rendering.builder._build_plan_for_query_node", _inner)

  _build_window_plan_for_node(
    node,
    required_input_columns={"window_sum_product_id"},
  )

  assert captured["required"] == {"product_id", "product_model_id"}
