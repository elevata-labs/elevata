"""Regression tests for base-projection column selection."""

from types import SimpleNamespace

from metadata.rendering.builder import _target_columns_for_base_projection


class _Columns:
  def __init__(self, columns):
    self._columns = list(columns)

  def order_by(self, *args):
    return list(self._columns)


def _dataset(*columns):
  return SimpleNamespace(target_columns=_Columns(columns))


def _column(name, *, active=True, lineage_origin="direct"):
  return SimpleNamespace(
    target_column_name=name,
    active=active,
    lineage_origin=lineage_origin,
  )


def test_base_projection_keeps_required_inactive_authored_column():
  authored = _column("product", active=False, lineage_origin="direct")
  dataset = _dataset(authored)

  selected = _target_columns_for_base_projection(
    dataset,
    required_input_columns={"product"},
  )

  assert selected == [authored]


def test_base_projection_excludes_required_query_derived_column():
  query_derived = _column(
    "person_type_code",
    active=True,
    lineage_origin="query_derived",
  )
  dataset = _dataset(query_derived)

  selected = _target_columns_for_base_projection(
    dataset,
    required_input_columns={"person_type_code"},
  )

  assert selected == []
