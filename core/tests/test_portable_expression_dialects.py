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

from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.rendering.expr import (
  Arithmetic,
  Between,
  BooleanOp,
  CaseBranch,
  CaseWhen,
  Cast,
  ColumnRef,
  Comparison,
  FuncCall,
  InList,
  IsNull,
  Literal,
  Not,
  UnaryArithmetic,
)


DIALECT_NAMES = [
  "bigquery",
  "databricks",
  "duckdb",
  "fabric_warehouse",
  "mssql",
  "postgres",
  "snowflake",
]


def _col(name: str) -> ColumnRef:
  return ColumnRef(table_alias="src", column_name=name)


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
def test_portable_arithmetic_comparison_boolean_and_null_rendering(dialect_name: str):
  dialect = get_active_dialect(dialect_name)
  expr = BooleanOp(
    operator="AND",
    parts=[
      Comparison(
        left=Arithmetic(left=_col("amount"), operator="/", right=_col("quantity")),
        operator=">",
        right=Literal(0),
      ),
      Not(expr=IsNull(expr=_col("customer_id"))),
    ],
  )

  sql = dialect.render_expr(expr)

  assert "amount" in sql
  assert "quantity" in sql
  assert " / " in sql
  assert " > 0" in sql
  assert " AND " in sql
  assert "NOT" in sql
  assert "IS NULL" in sql


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
def test_portable_case_between_and_in_rendering(dialect_name: str):
  dialect = get_active_dialect(dialect_name)
  expr = CaseWhen(
    branches=[
      CaseBranch(
        condition=Between(
          expr=_col("score"),
          lower=Literal(80),
          upper=Literal(100),
        ),
        result=Literal("high"),
      ),
      CaseBranch(
        condition=InList(
          expr=_col("status"),
          values=[Literal("A"), Literal("B")],
        ),
        result=Literal("active"),
      ),
    ],
    else_expr=Literal("other"),
  )

  sql = dialect.render_expr(expr)

  assert sql.startswith("CASE ")
  assert " BETWEEN 80 AND 100" in sql
  assert " IN ('A', 'B')" in sql
  assert "THEN 'high'" in sql
  assert "THEN 'active'" in sql
  assert "ELSE 'other'" in sql
  assert sql.endswith(" END")


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
def test_portable_unary_and_negated_predicates_rendering(dialect_name: str):
  dialect = get_active_dialect(dialect_name)

  assert "-" in dialect.render_expr(UnaryArithmetic(operator="-", expr=_col("amount")))
  assert "IS NOT NULL" in dialect.render_expr(IsNull(expr=_col("customer_id"), negated=True))
  assert "NOT BETWEEN" in dialect.render_expr(
    Between(expr=_col("score"), lower=Literal(0), upper=Literal(10), negated=True)
  )
  assert "NOT IN" in dialect.render_expr(
    InList(expr=_col("status"), values=[Literal("X"), Literal("Y")], negated=True)
  )


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
def test_portable_scalar_function_names_are_dialect_owned(dialect_name: str):
  dialect = get_active_dialect(dialect_name)

  rendered = {
    "ABS": dialect.render_expr(FuncCall(name="ABS", args=[_col("amount")])),
    "CEIL": dialect.render_expr(FuncCall(name="CEIL", args=[_col("amount")])),
    "FLOOR": dialect.render_expr(FuncCall(name="FLOOR", args=[_col("amount")])),
    "LENGTH": dialect.render_expr(FuncCall(name="LENGTH", args=[_col("name")])),
    "LOWER": dialect.render_expr(FuncCall(name="LOWER", args=[_col("name")])),
    "NULLIF": dialect.render_expr(FuncCall(name="NULLIF", args=[_col("amount"), Literal(0)])),
    "ROUND": dialect.render_expr(FuncCall(name="ROUND", args=[_col("amount"), Literal(2)])),
    "ROUND_DEFAULT": dialect.render_expr(FuncCall(name="ROUND", args=[_col("amount")])),
    "TRIM": dialect.render_expr(FuncCall(name="TRIM", args=[_col("name")])),
    "UPPER": dialect.render_expr(FuncCall(name="UPPER", args=[_col("name")])),
  }

  expected_prefixes = {
    "ABS": "ABS(",
    "CEIL": "CEILING(" if dialect_name in {"mssql", "fabric_warehouse"} else "CEIL(",
    "FLOOR": "FLOOR(",
    "LENGTH": "(CASE WHEN" if dialect_name in {"mssql", "fabric_warehouse"} else "LENGTH(",
    "LOWER": "LOWER(",
    "NULLIF": "NULLIF(",
    "ROUND": "ROUND(",
    "ROUND_DEFAULT": "ROUND(",
    "TRIM": "TRIM(",
    "UPPER": "UPPER(",
  }

  for function_name, prefix in expected_prefixes.items():
    assert rendered[function_name].startswith(prefix)


@pytest.mark.parametrize("dialect_name", ["mssql", "fabric_warehouse"])
def test_tsql_length_preserves_trailing_spaces_and_null_semantics(dialect_name: str):
  dialect = get_active_dialect(dialect_name)
  sql = dialect.render_expr(FuncCall(name="LENGTH", args=[_col("name")]))

  assert sql.startswith("(CASE WHEN ")
  assert " IS NULL THEN NULL " in sql
  assert "LEN(" in sql
  assert " + '#') - 1" in sql


@pytest.mark.parametrize("dialect_name", ["mssql", "fabric_warehouse"])
def test_tsql_round_with_one_argument_supplies_required_scale(dialect_name: str):
  dialect = get_active_dialect(dialect_name)
  sql = dialect.render_expr(FuncCall(name="ROUND", args=[_col("amount")]))

  assert sql.endswith(", 0)")


def test_bigquery_modulo_uses_mod_function():
  dialect = get_active_dialect("bigquery")
  sql = dialect.render_expr(Arithmetic(left=_col("amount"), operator="%", right=Literal(10)))

  assert sql.startswith("MOD(")
  assert ", 10)" in sql


@pytest.mark.parametrize(
  "dialect_name",
  ["databricks", "duckdb", "fabric_warehouse", "mssql", "postgres", "snowflake"],
)
def test_non_bigquery_modulo_uses_operator(dialect_name: str):
  dialect = get_active_dialect(dialect_name)
  sql = dialect.render_expr(Arithmetic(left=_col("amount"), operator="%", right=Literal(10)))

  assert " % 10" in sql


@pytest.mark.parametrize(
  "dialect_name, physical_type",
  [
    ("bigquery", "STRING"),
    ("databricks", "STRING"),
    ("duckdb", "VARCHAR"),
    ("fabric_warehouse", "VARCHAR(MAX)"),
    ("mssql", "NVARCHAR(MAX)"),
    ("postgres", "TEXT"),
    ("snowflake", "VARCHAR"),
  ],
)
def test_portable_cast_uses_dialect_type_mapping(dialect_name: str, physical_type: str):
  dialect = get_active_dialect(dialect_name)
  sql = dialect.render_expr(Cast(expr=_col("amount"), target_type="STRING"))

  assert sql.endswith(f" AS {physical_type})")


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
def test_portable_boolean_literal_uses_dialect_literal_semantics(dialect_name: str):
  dialect = get_active_dialect(dialect_name)
  sql = dialect.render_expr(Comparison(left=_col("is_active"), operator="=", right=Literal(True)))

  if dialect_name in {"mssql", "fabric_warehouse"}:
    assert " = 1" in sql
  else:
    assert " = TRUE" in sql


@pytest.mark.parametrize("dialect_name", DIALECT_NAMES)
def test_portable_expression_renderer_fails_closed_for_invalid_ast(dialect_name: str):
  dialect = get_active_dialect(dialect_name)

  with pytest.raises(ValueError, match="Unsupported arithmetic operator"):
    dialect.render_expr(Arithmetic(left=Literal(1), operator="^", right=Literal(2)))

  with pytest.raises(ValueError, match="Boolean expression requires at least two parts"):
    dialect.render_expr(BooleanOp(operator="AND", parts=[Literal(True)]))

  with pytest.raises(ValueError, match="IN expression requires at least one value"):
    dialect.render_expr(InList(expr=_col("status"), values=[]))

  with pytest.raises(ValueError, match="CASE expression requires at least one branch"):
    dialect.render_expr(CaseWhen(branches=[]))
