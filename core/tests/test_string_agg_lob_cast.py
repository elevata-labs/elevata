"""Regression tests for Microsoft STRING_AGG large-value rendering."""

from metadata.rendering.dialects.fabric_warehouse import FabricWarehouseDialect
from metadata.rendering.dialects.mssql import MssqlDialect


def _render_with_stubbed_exprs(monkeypatch, dialect, *, ordered: bool) -> str:
  value = object()
  delimiter = object()
  order_by = object()
  rendered = {
    value: "s.product_name",
    delimiter: "','",
    order_by: "s.product_no ASC",
  }

  monkeypatch.setattr(dialect, "render_expr", lambda expr: rendered[expr])

  args = [value, delimiter]
  if ordered:
    args.append(order_by)
  return dialect.render_string_agg(args)


def test_mssql_string_agg_casts_value_to_nvarchar_max(monkeypatch):
  sql = _render_with_stubbed_exprs(monkeypatch, MssqlDialect(), ordered=True)

  assert sql == (
    "STRING_AGG(CAST(s.product_name AS NVARCHAR(MAX)), ',') "
    "WITHIN GROUP (ORDER BY s.product_no ASC)"
  )


def test_fabric_string_agg_casts_value_to_varchar_max(monkeypatch):
  sql = _render_with_stubbed_exprs(monkeypatch, FabricWarehouseDialect(), ordered=False)

  assert sql == "STRING_AGG(CAST(s.product_name AS VARCHAR(MAX)), ',')"
