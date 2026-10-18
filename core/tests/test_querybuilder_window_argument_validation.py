"""Regression tests for fail-closed Window function argument validation."""

from types import SimpleNamespace

import pytest

from metadata.rendering.builder import _build_window_select_items


class _OrderedItems:
  """Minimal queryset-like wrapper used by builder unit tests."""

  def __init__(self, items):
    self._items = list(items)

  def all(self):
    return self

  def filter(self, **kwargs):
    if kwargs.get("active") is True:
      return _OrderedItems([item for item in self._items if getattr(item, "active", True)])
    return self

  def order_by(self, *args):
    return list(self._items)


class _Args(_OrderedItems):
  pass


def _arg(*, arg_type, column_name="", int_value=None, str_value=""):
  return SimpleNamespace(
    arg_type=arg_type,
    column_name=column_name,
    int_value=int_value,
    str_value=str_value,
  )


def _window_column(function, output_name, args):
  return SimpleNamespace(
    function=function,
    output_name=output_name,
    args=_Args(args),
    partition_by=None,
    order_by=None,
    frame_type="",
    frame_start="",
    frame_start_offset=None,
    frame_end="",
    frame_end_offset=None,
    active=True,
  )


def _window_node(*columns):
  return SimpleNamespace(columns=_OrderedItems(columns))


def test_window_sum_without_argument_fails_before_sql_rendering():
  node = _window_node(_window_column("SUM", "running_sum", []))

  with pytest.raises(ValueError, match=r"Window function SUM expects 1\.\.1 args, got 0"):
    _build_window_select_items(node, "u")


def test_window_count_without_argument_remains_valid_count_star_contract():
  node = _window_node(_window_column("COUNT", "row_count", []))

  items = _build_window_select_items(node, "u")

  assert len(items) == 1
  assert items[0].alias == "row_count"
  assert items[0].expr.name == "COUNT"
  assert items[0].expr.args == []


def test_window_sum_requires_column_argument_type():
  node = _window_node(
    _window_column("SUM", "running_sum", [_arg(arg_type="int", int_value=1)])
  )

  with pytest.raises(ValueError, match=r"Window function SUM arg 1 must be one of"):
    _build_window_select_items(node, "u")


def test_window_sum_with_column_argument_is_valid():
  node = _window_node(
    _window_column("SUM", "running_sum", [_arg(arg_type="column", column_name="product_id")])
  )

  items = _build_window_select_items(node, "u")

  assert len(items) == 1
  assert items[0].alias == "running_sum"
  assert items[0].expr.name == "SUM"
  assert len(items[0].expr.args) == 1
  assert items[0].expr.args[0].column_name == "product_id"
