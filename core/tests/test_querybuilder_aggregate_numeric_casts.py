"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

This file is part of elevata.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or (at your
option) any later version.
"""

from metadata.rendering.builder import _cast_to_type_contract
from metadata.rendering.dialects.bigquery import BigQueryDialect
from metadata.rendering.expr import ColumnRef, FuncCall


def _render_aggregate(function: str, *, precision: int, scale: int) -> str:
  contract = {
    "datatype": "DECIMAL",
    "max_length": None,
    "precision": precision,
    "scale": scale,
  }
  arg = _cast_to_type_contract(
    ColumnRef(table_alias="u", column_name="amount"),
    contract,
  )
  aggregate = FuncCall(name=function, args=[arg])
  return BigQueryDialect().render_expr(aggregate)


def test_bigquery_avg_decimal_38_10_uses_bignumeric_contract():
  assert _render_aggregate("AVG", precision=38, scale=10) == (
    "AVG(CAST(u.amount AS BIGNUMERIC))"
  )


def test_bigquery_sum_decimal_29_4_uses_numeric_contract():
  assert _render_aggregate("SUM", precision=29, scale=4) == (
    "SUM(CAST(u.amount AS NUMERIC))"
  )


def test_bigquery_decimal_type_family_switches_on_scale():
  dialect = BigQueryDialect()
  assert dialect.map_logical_type(
    datatype="DECIMAL",
    precision=29,
    scale=4,
  ) == "NUMERIC(29,4)"
  assert dialect.map_logical_type(
    datatype="DECIMAL",
    precision=38,
    scale=10,
  ) == "BIGNUMERIC(38,10)"


def test_bigquery_parameterized_decimal_contract_uses_unparameterized_cast():
  dialect = BigQueryDialect()
  assert dialect.cast_expression("u.amount", "NUMERIC(29,4)") == "CAST(u.amount AS NUMERIC)"
  assert dialect.cast_expression("u.amount", "BIGNUMERIC(38,10)") == "CAST(u.amount AS BIGNUMERIC)"
