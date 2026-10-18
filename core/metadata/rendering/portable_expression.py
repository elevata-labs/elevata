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

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Callable, Dict, Iterable, Mapping

from metadata.rendering.expr import (
  Arithmetic,
  Between,
  BooleanOp,
  CaseBranch,
  CaseWhen,
  Cast,
  Coalesce,
  ColumnRef,
  Comparison,
  Concat,
  Expr,
  FuncCall,
  InList,
  IsNull,
  Literal,
  Not,
  UnaryArithmetic,
)


PORTABLE_EXPRESSION_VERSION = 1

ARITHMETIC_OPERATORS = {"+", "-", "*", "/", "%"}
UNARY_ARITHMETIC_OPERATORS = {"+", "-"}
COMPARISON_OPERATORS = {"=", "!=", "<", "<=", ">", ">=", "LIKE"}
BOOLEAN_OPERATORS = {"AND", "OR"}
PORTABLE_FUNCTION_ARITY = {
  "ABS": (1, 1),
  "CEIL": (1, 1),
  "FLOOR": (1, 1),
  "LENGTH": (1, 1),
  "LOWER": (1, 1),
  "NULLIF": (2, 2),
  "ROUND": (1, 2),
  "TRIM": (1, 1),
  "UPPER": (1, 1),
}
PORTABLE_FUNCTIONS = frozenset(PORTABLE_FUNCTION_ARITY)
PORTABLE_CAST_TYPES = {
  "BIGINT",
  "BINARY",
  "BOOLEAN",
  "DATE",
  "DECIMAL",
  "FLOAT",
  "INTEGER",
  "JSON",
  "STRING",
  "TIME",
  "TIMESTAMP",
  "UUID",
}


class PortableExpressionError(ValueError):
  """Raised when a persisted portable expression violates its contract."""


def canonical_expression_json(contract: Mapping[str, Any]) -> str:
  """Return the canonical JSON representation used for fingerprints and artifacts."""
  normalized = normalize_expression_contract(contract)
  return json.dumps(
    normalized,
    ensure_ascii=False,
    separators=(",", ":"),
    sort_keys=True,
  )


def expression_fingerprint(contract: Mapping[str, Any]) -> str:
  """Return a deterministic SHA-256 fingerprint for a portable expression contract."""
  canonical = canonical_expression_json(contract)
  return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_expression_contract(
  contract: Mapping[str, Any],
  *,
  allowed_runtime_values: Iterable[str] = (),
) -> Dict[str, Any]:
  """Validate and normalize a complete versioned portable expression contract."""
  if not isinstance(contract, Mapping):
    raise PortableExpressionError("Portable expression contract must be an object.")

  _require_exact_keys(contract, {"version", "expression"}, "expression contract")

  version = contract.get("version")
  if version != PORTABLE_EXPRESSION_VERSION:
    raise PortableExpressionError(
      f"Unsupported portable expression version: {version!r}."
    )

  allowed_runtime = frozenset(
    str(name).strip().lower()
    for name in allowed_runtime_values
    if str(name).strip()
  )
  return {
    "version": PORTABLE_EXPRESSION_VERSION,
    "expression": _normalize_node(
      contract.get("expression"),
      allowed_runtime_values=allowed_runtime,
    ),
  }


def normalize_predicate_contract(
  contract: Mapping[str, Any],
  *,
  allowed_runtime_values: Iterable[str] = (),
) -> Dict[str, Any]:
  """Validate and normalize a portable contract with a predicate-valued root."""
  normalized = normalize_expression_contract(
    contract,
    allowed_runtime_values=allowed_runtime_values,
  )
  if normalized["expression"].get("type") not in {
    "comparison",
    "boolean",
    "not",
    "is_null",
    "between",
    "in",
  }:
    raise PortableExpressionError(
      "Portable predicate requires a boolean predicate root, "
      f"got {normalized['expression'].get('type')!r}."
    )
  return normalized


def expression_contract_to_ast(
  contract: Mapping[str, Any],
  column_resolver: Callable[[str], ColumnRef] | None = None,
  *,
  allowed_runtime_values: Iterable[str] = (),
  runtime_value_resolver: Callable[[str], Expr] | None = None,
) -> Expr:
  """Parse a validated portable expression contract into the logical expression AST."""
  normalized = normalize_expression_contract(
    contract,
    allowed_runtime_values=allowed_runtime_values,
  )
  resolver = column_resolver or (lambda name: ColumnRef(table_alias=None, column_name=name))
  return _node_to_ast(
    normalized["expression"],
    resolver,
    runtime_value_resolver=runtime_value_resolver,
  )


def predicate_contract_to_ast(
  contract: Mapping[str, Any],
  column_resolver: Callable[[str], ColumnRef] | None = None,
  *,
  allowed_runtime_values: Iterable[str] = (),
  runtime_value_resolver: Callable[[str], Expr] | None = None,
) -> Expr:
  """Parse a portable contract and require a predicate-valued root expression."""
  normalized = normalize_predicate_contract(
    contract,
    allowed_runtime_values=allowed_runtime_values,
  )
  resolver = column_resolver or (lambda name: ColumnRef(table_alias=None, column_name=name))
  return _node_to_ast(
    normalized["expression"],
    resolver,
    runtime_value_resolver=runtime_value_resolver,
  )


def expression_runtime_value_names(
  contract: Mapping[str, Any],
  *,
  allowed_runtime_values: Iterable[str] = (),
) -> set[str]:
  """Return runtime-value references used by a normalized portable expression."""
  normalized = normalize_expression_contract(
    contract,
    allowed_runtime_values=allowed_runtime_values,
  )
  names: set[str] = set()

  def walk(node: Any) -> None:
    if not isinstance(node, Mapping):
      return
    if node.get("type") == "runtime_value":
      name = str(node.get("name") or "").strip().lower()
      if name:
        names.add(name)
    for value in node.values():
      if isinstance(value, Mapping):
        walk(value)
      elif isinstance(value, list):
        for item in value:
          walk(item)

  walk(normalized["expression"])
  return names


def format_expression_contract(
  contract: Mapping[str, Any],
  *,
  allowed_runtime_values: Iterable[str] = (),
) -> str:
  """Return a concise, platform-neutral display string for a portable expression."""
  normalized = normalize_expression_contract(
    contract,
    allowed_runtime_values=allowed_runtime_values,
  )
  return _format_expression_node(normalized["expression"])


def _format_expression_node(node: Mapping[str, Any]) -> str:
  node_type = node["type"]

  if node_type == "column":
    return str(node["name"])
  if node_type == "literal":
    value = node["value"]
    if value is None:
      return "NULL"
    if isinstance(value, bool):
      return "TRUE" if value else "FALSE"
    if isinstance(value, str):
      return "'" + value.replace("'", "''") + "'"
    return str(value)
  if node_type == "runtime_value":
    name = str(node["name"])
    return "Delta cutoff" if name == "delta_cutoff" else name.replace("_", " ")
  if node_type in {"arithmetic", "comparison"}:
    return (
      f"({_format_expression_node(node['left'])} {node['operator']} "
      f"{_format_expression_node(node['right'])})"
    )
  if node_type == "unary_arithmetic":
    return f"({node['operator']}{_format_expression_node(node['expression'])})"
  if node_type == "boolean":
    joiner = f" {node['operator']} "
    return "(" + joiner.join(_format_expression_node(part) for part in node["parts"]) + ")"
  if node_type == "not":
    return f"NOT {_format_expression_node(node['expression'])}"
  if node_type == "is_null":
    suffix = "IS NOT NULL" if node["negated"] else "IS NULL"
    return f"{_format_expression_node(node['expression'])} {suffix}"
  if node_type == "between":
    operator = "NOT BETWEEN" if node["negated"] else "BETWEEN"
    return (
      f"{_format_expression_node(node['expression'])} {operator} "
      f"{_format_expression_node(node['lower'])} AND {_format_expression_node(node['upper'])}"
    )
  if node_type == "in":
    operator = "NOT IN" if node["negated"] else "IN"
    values = ", ".join(_format_expression_node(value) for value in node["values"])
    return f"{_format_expression_node(node['expression'])} {operator} ({values})"
  if node_type == "case":
    parts = ["CASE"]
    for branch in node["branches"]:
      parts.append(
        f"WHEN {_format_expression_node(branch['when'])} "
        f"THEN {_format_expression_node(branch['then'])}"
      )
    if node["else"] is not None:
      parts.append(f"ELSE {_format_expression_node(node['else'])}")
    parts.append("END")
    return " ".join(parts)
  if node_type == "function":
    args = ", ".join(_format_expression_node(arg) for arg in node["args"])
    return f"{node['name']}({args})"
  if node_type == "coalesce":
    args = ", ".join(_format_expression_node(part) for part in node["parts"])
    return f"COALESCE({args})"
  if node_type == "concat":
    args = ", ".join(_format_expression_node(part) for part in node["parts"])
    return f"CONCAT({args})"
  if node_type == "cast":
    return f"CAST({_format_expression_node(node['expression'])} AS {node['target_type']})"

  raise PortableExpressionError(f"Unsupported expression node type: {node_type!r}.")


def ast_to_expression_contract(expr: Expr) -> Dict[str, Any]:
  """Serialize a supported logical expression AST into a portable versioned contract."""
  return {
    "version": PORTABLE_EXPRESSION_VERSION,
    "expression": _ast_to_node(expr),
  }


def _normalize_node(
  node: Any,
  *,
  allowed_runtime_values: frozenset[str] = frozenset(),
) -> Dict[str, Any]:
  if not isinstance(node, Mapping):
    raise PortableExpressionError("Expression node must be an object.")

  node_type = _required_string(node, "type", "expression node").lower()

  if node_type == "column":
    _require_exact_keys(node, {"type", "name"}, "column expression")
    return {
      "type": "column",
      "name": _required_string(node, "name", "column expression"),
    }

  if node_type == "literal":
    _require_exact_keys(node, {"type", "value"}, "literal expression")
    value = node.get("value")
    if not _is_json_scalar(value):
      raise PortableExpressionError(
        "Literal value must be null, boolean, string, integer, or number."
      )
    return {"type": "literal", "value": value}

  if node_type == "runtime_value":
    _require_exact_keys(node, {"type", "name"}, "runtime value expression")
    name = _required_string(node, "name", "runtime value expression").lower()
    if name not in allowed_runtime_values:
      raise PortableExpressionError(
        f"Runtime value {name!r} is not allowed in this expression context."
      )
    return {"type": "runtime_value", "name": name}

  if node_type == "arithmetic":
    _require_exact_keys(node, {"type", "operator", "left", "right"}, "arithmetic expression")
    operator = _normalized_operator(node, "operator", ARITHMETIC_OPERATORS, "arithmetic")
    return {
      "type": "arithmetic",
      "operator": operator,
      "left": _normalize_node(node.get("left"), allowed_runtime_values=allowed_runtime_values),
      "right": _normalize_node(node.get("right"), allowed_runtime_values=allowed_runtime_values),
    }

  if node_type == "comparison":
    _require_exact_keys(node, {"type", "operator", "left", "right"}, "comparison expression")
    operator = _normalized_operator(node, "operator", COMPARISON_OPERATORS, "comparison")
    return {
      "type": "comparison",
      "operator": operator,
      "left": _normalize_node(node.get("left"), allowed_runtime_values=allowed_runtime_values),
      "right": _normalize_node(node.get("right"), allowed_runtime_values=allowed_runtime_values),
    }

  if node_type == "unary_arithmetic":
    _require_exact_keys(node, {"type", "operator", "expression"}, "unary arithmetic expression")
    operator = _normalized_operator(
      node,
      "operator",
      UNARY_ARITHMETIC_OPERATORS,
      "unary arithmetic",
    )
    return {
      "type": "unary_arithmetic",
      "operator": operator,
      "expression": _normalize_node(
        node.get("expression"),
        allowed_runtime_values=allowed_runtime_values,
      ),
    }

  if node_type == "boolean":
    _require_exact_keys(node, {"type", "operator", "parts"}, "boolean expression")
    operator = _normalized_operator(node, "operator", BOOLEAN_OPERATORS, "boolean")
    raw_parts = node.get("parts")
    if not isinstance(raw_parts, list) or len(raw_parts) < 2:
      raise PortableExpressionError(
        "Boolean expression requires at least 2 predicate parts."
      )
    parts = [
      _normalize_predicate_node(part, "boolean expression", allowed_runtime_values)
      for part in raw_parts
    ]
    return {"type": "boolean", "operator": operator, "parts": parts}

  if node_type == "not":
    _require_exact_keys(node, {"type", "expression"}, "not expression")
    return {
      "type": "not",
      "expression": _normalize_predicate_node(
        node.get("expression"),
        "NOT expression",
        allowed_runtime_values,
      ),
    }

  if node_type == "is_null":
    _require_exact_keys(node, {"type", "expression", "negated"}, "null predicate")
    return {
      "type": "is_null",
      "expression": _normalize_node(
        node.get("expression"),
        allowed_runtime_values=allowed_runtime_values,
      ),
      "negated": _required_bool(node, "negated", "null predicate"),
    }

  if node_type == "between":
    _require_exact_keys(node, {"type", "expression", "lower", "upper", "negated"}, "between predicate")
    return {
      "type": "between",
      "expression": _normalize_node(
        node.get("expression"),
        allowed_runtime_values=allowed_runtime_values,
      ),
      "lower": _normalize_node(
        node.get("lower"),
        allowed_runtime_values=allowed_runtime_values,
      ),
      "upper": _normalize_node(
        node.get("upper"),
        allowed_runtime_values=allowed_runtime_values,
      ),
      "negated": _required_bool(node, "negated", "between predicate"),
    }

  if node_type == "in":
    _require_exact_keys(node, {"type", "expression", "values", "negated"}, "in predicate")
    return {
      "type": "in",
      "expression": _normalize_node(
        node.get("expression"),
        allowed_runtime_values=allowed_runtime_values,
      ),
      "values": _normalize_node_list(
        node.get("values"),
        "in predicate",
        minimum=1,
        allowed_runtime_values=allowed_runtime_values,
      ),
      "negated": _required_bool(node, "negated", "in predicate"),
    }

  if node_type == "case":
    _require_exact_keys(node, {"type", "branches", "else"}, "case expression")
    raw_branches = node.get("branches")
    if not isinstance(raw_branches, list) or not raw_branches:
      raise PortableExpressionError("CASE expression requires at least one branch.")
    branches = [
      _normalize_case_branch(branch, allowed_runtime_values)
      for branch in raw_branches
    ]
    else_node = node.get("else")
    return {
      "type": "case",
      "branches": branches,
      "else": (
        _normalize_node(else_node, allowed_runtime_values=allowed_runtime_values)
        if else_node is not None
        else None
      ),
    }

  if node_type == "function":
    _require_exact_keys(node, {"type", "name", "args"}, "function expression")
    name = _required_string(node, "name", "function expression").upper()
    if name not in PORTABLE_FUNCTIONS:
      raise PortableExpressionError(f"Unsupported portable function: {name}.")
    args = _normalize_node_list(
      node.get("args"),
      "function expression",
      minimum=1,
      allowed_runtime_values=allowed_runtime_values,
    )
    _validate_function_arity(name, len(args))
    return {"type": "function", "name": name, "args": args}

  if node_type == "coalesce":
    _require_exact_keys(node, {"type", "parts"}, "coalesce expression")
    return {
      "type": "coalesce",
      "parts": _normalize_node_list(
        node.get("parts"),
        "coalesce expression",
        minimum=2,
        allowed_runtime_values=allowed_runtime_values,
      ),
    }

  if node_type == "concat":
    _require_exact_keys(node, {"type", "parts"}, "concat expression")
    return {
      "type": "concat",
      "parts": _normalize_node_list(
        node.get("parts"),
        "concat expression",
        minimum=1,
        allowed_runtime_values=allowed_runtime_values,
      ),
    }

  if node_type == "cast":
    _require_exact_keys(node, {"type", "expression", "target_type"}, "cast expression")
    target_type = _required_string(node, "target_type", "cast expression").upper()
    if target_type not in PORTABLE_CAST_TYPES:
      raise PortableExpressionError(f"Unsupported portable CAST type: {target_type}.")
    return {
      "type": "cast",
      "expression": _normalize_node(
        node.get("expression"),
        allowed_runtime_values=allowed_runtime_values,
      ),
      "target_type": target_type,
    }

  raise PortableExpressionError(f"Unsupported expression node type: {node_type!r}.")


def _normalize_case_branch(
  branch: Any,
  allowed_runtime_values: frozenset[str],
) -> Dict[str, Any]:
  if not isinstance(branch, Mapping):
    raise PortableExpressionError("CASE branch must be an object.")
  _require_exact_keys(branch, {"when", "then"}, "case branch")
  return {
    "when": _normalize_predicate_node(
      branch.get("when"),
      "CASE WHEN",
      allowed_runtime_values,
    ),
    "then": _normalize_node(
      branch.get("then"),
      allowed_runtime_values=allowed_runtime_values,
    ),
  }


def _normalize_predicate_node(
  node: Any,
  label: str,
  allowed_runtime_values: frozenset[str],
) -> Dict[str, Any]:
  """Normalize a node and require predicate-valued semantics."""
  normalized = _normalize_node(
    node,
    allowed_runtime_values=allowed_runtime_values,
  )
  if normalized.get("type") not in {
    "comparison",
    "boolean",
    "not",
    "is_null",
    "between",
    "in",
  }:
    raise PortableExpressionError(
      f"{label} requires a predicate expression, got {normalized.get('type')!r}."
    )
  return normalized


def _node_to_ast(
  node: Mapping[str, Any],
  column_resolver: Callable[[str], ColumnRef],
  *,
  runtime_value_resolver: Callable[[str], Expr] | None = None,
) -> Expr:
  node_type = str(node["type"])

  if node_type == "column":
    return column_resolver(str(node["name"]))
  if node_type == "literal":
    return Literal(node.get("value"))
  if node_type == "runtime_value":
    if runtime_value_resolver is None:
      raise PortableExpressionError(
        f"Runtime value {node.get('name')!r} requires a runtime resolver."
      )
    resolved = runtime_value_resolver(str(node["name"]))
    if not isinstance(resolved, Expr):
      raise PortableExpressionError(
        f"Runtime resolver for {node.get('name')!r} must return an Expr."
      )
    return resolved
  if node_type == "arithmetic":
    return Arithmetic(
      left=_node_to_ast(node["left"], column_resolver, runtime_value_resolver=runtime_value_resolver),
      operator=str(node["operator"]),
      right=_node_to_ast(node["right"], column_resolver, runtime_value_resolver=runtime_value_resolver),
    )
  if node_type == "comparison":
    return Comparison(
      left=_node_to_ast(node["left"], column_resolver, runtime_value_resolver=runtime_value_resolver),
      operator=str(node["operator"]),
      right=_node_to_ast(node["right"], column_resolver, runtime_value_resolver=runtime_value_resolver),
    )
  if node_type == "unary_arithmetic":
    return UnaryArithmetic(
      operator=str(node["operator"]),
      expr=_node_to_ast(node["expression"], column_resolver, runtime_value_resolver=runtime_value_resolver),
    )
  if node_type == "boolean":
    return BooleanOp(
      operator=str(node["operator"]),
      parts=[_node_to_ast(part, column_resolver, runtime_value_resolver=runtime_value_resolver) for part in node["parts"]],
    )
  if node_type == "not":
    return Not(expr=_node_to_ast(node["expression"], column_resolver, runtime_value_resolver=runtime_value_resolver))
  if node_type == "is_null":
    return IsNull(
      expr=_node_to_ast(node["expression"], column_resolver, runtime_value_resolver=runtime_value_resolver),
      negated=bool(node["negated"]),
    )
  if node_type == "between":
    return Between(
      expr=_node_to_ast(node["expression"], column_resolver, runtime_value_resolver=runtime_value_resolver),
      lower=_node_to_ast(node["lower"], column_resolver, runtime_value_resolver=runtime_value_resolver),
      upper=_node_to_ast(node["upper"], column_resolver, runtime_value_resolver=runtime_value_resolver),
      negated=bool(node["negated"]),
    )
  if node_type == "in":
    return InList(
      expr=_node_to_ast(node["expression"], column_resolver, runtime_value_resolver=runtime_value_resolver),
      values=[_node_to_ast(value, column_resolver, runtime_value_resolver=runtime_value_resolver) for value in node["values"]],
      negated=bool(node["negated"]),
    )
  if node_type == "case":
    return CaseWhen(
      branches=[
        CaseBranch(
          condition=_node_to_ast(branch["when"], column_resolver, runtime_value_resolver=runtime_value_resolver),
          result=_node_to_ast(branch["then"], column_resolver, runtime_value_resolver=runtime_value_resolver),
        )
        for branch in node["branches"]
      ],
      else_expr=(
        _node_to_ast(node["else"], column_resolver, runtime_value_resolver=runtime_value_resolver)
        if node.get("else") is not None
        else None
      ),
    )
  if node_type == "function":
    return FuncCall(
      name=str(node["name"]),
      args=[_node_to_ast(arg, column_resolver, runtime_value_resolver=runtime_value_resolver) for arg in node["args"]],
    )
  if node_type == "coalesce":
    return Coalesce(parts=[_node_to_ast(part, column_resolver, runtime_value_resolver=runtime_value_resolver) for part in node["parts"]])
  if node_type == "concat":
    return Concat(parts=[_node_to_ast(part, column_resolver, runtime_value_resolver=runtime_value_resolver) for part in node["parts"]])
  if node_type == "cast":
    return Cast(
      expr=_node_to_ast(node["expression"], column_resolver, runtime_value_resolver=runtime_value_resolver),
      target_type=str(node["target_type"]),
    )

  raise PortableExpressionError(f"Unsupported normalized expression node type: {node_type!r}.")


def _ast_to_node(expr: Expr) -> Dict[str, Any]:
  if isinstance(expr, ColumnRef):
    return {"type": "column", "name": expr.column_name}
  if isinstance(expr, Literal):
    if not _is_json_scalar(expr.value):
      raise PortableExpressionError("Literal AST value is not portable JSON data.")
    return {"type": "literal", "value": expr.value}
  if isinstance(expr, Arithmetic):
    _ensure_operator(expr.operator, ARITHMETIC_OPERATORS, "arithmetic")
    return {
      "type": "arithmetic",
      "operator": expr.operator,
      "left": _ast_to_node(expr.left),
      "right": _ast_to_node(expr.right),
    }
  if isinstance(expr, Comparison):
    operator = str(expr.operator).upper() if str(expr.operator).upper() == "LIKE" else str(expr.operator)
    _ensure_operator(operator, COMPARISON_OPERATORS, "comparison")
    return {
      "type": "comparison",
      "operator": operator,
      "left": _ast_to_node(expr.left),
      "right": _ast_to_node(expr.right),
    }
  if isinstance(expr, UnaryArithmetic):
    _ensure_operator(expr.operator, UNARY_ARITHMETIC_OPERATORS, "unary arithmetic")
    return {
      "type": "unary_arithmetic",
      "operator": expr.operator,
      "expression": _ast_to_node(expr.expr),
    }
  if isinstance(expr, BooleanOp):
    operator = str(expr.operator).upper()
    _ensure_operator(operator, BOOLEAN_OPERATORS, "boolean")
    if len(expr.parts) < 2:
      raise PortableExpressionError("Boolean AST expression requires at least two parts.")
    return {"type": "boolean", "operator": operator, "parts": [_ast_to_node(part) for part in expr.parts]}
  if isinstance(expr, Not):
    return {"type": "not", "expression": _ast_to_node(expr.expr)}
  if isinstance(expr, IsNull):
    return {"type": "is_null", "expression": _ast_to_node(expr.expr), "negated": expr.negated}
  if isinstance(expr, Between):
    return {
      "type": "between",
      "expression": _ast_to_node(expr.expr),
      "lower": _ast_to_node(expr.lower),
      "upper": _ast_to_node(expr.upper),
      "negated": expr.negated,
    }
  if isinstance(expr, InList):
    if not expr.values:
      raise PortableExpressionError("IN AST expression requires at least one value.")
    return {
      "type": "in",
      "expression": _ast_to_node(expr.expr),
      "values": [_ast_to_node(value) for value in expr.values],
      "negated": expr.negated,
    }
  if isinstance(expr, CaseWhen):
    if not expr.branches:
      raise PortableExpressionError("CASE AST expression requires at least one branch.")
    return {
      "type": "case",
      "branches": [
        {"when": _ast_to_node(branch.condition), "then": _ast_to_node(branch.result)}
        for branch in expr.branches
      ],
      "else": _ast_to_node(expr.else_expr) if expr.else_expr is not None else None,
    }
  if isinstance(expr, FuncCall):
    name = str(expr.name).upper()
    if name not in PORTABLE_FUNCTIONS:
      raise PortableExpressionError(f"Unsupported portable function AST: {name}.")
    _validate_function_arity(name, len(expr.args))
    return {"type": "function", "name": name, "args": [_ast_to_node(arg) for arg in expr.args]}
  if isinstance(expr, Coalesce):
    if len(expr.parts) < 2:
      raise PortableExpressionError("COALESCE AST requires at least two parts.")
    return {"type": "coalesce", "parts": [_ast_to_node(part) for part in expr.parts]}
  if isinstance(expr, Concat):
    if not expr.parts:
      raise PortableExpressionError("CONCAT AST requires at least one part.")
    return {"type": "concat", "parts": [_ast_to_node(part) for part in expr.parts]}
  if isinstance(expr, Cast):
    target_type = str(expr.target_type or "").strip().upper()
    if target_type not in PORTABLE_CAST_TYPES:
      raise PortableExpressionError(f"Unsupported portable CAST type AST: {target_type}.")
    return {"type": "cast", "expression": _ast_to_node(expr.expr), "target_type": target_type}

  raise PortableExpressionError(
    f"Expression AST node is not portable: {expr.__class__.__name__}."
  )


def _require_exact_keys(value: Mapping[str, Any], expected: Iterable[str], label: str) -> None:
  expected_set = set(expected)
  actual_set = set(value.keys())
  if actual_set != expected_set:
    missing = sorted(expected_set - actual_set)
    extra = sorted(actual_set - expected_set)
    details = []
    if missing:
      details.append(f"missing keys: {', '.join(missing)}")
    if extra:
      details.append(f"unknown keys: {', '.join(extra)}")
    raise PortableExpressionError(f"Invalid {label} ({'; '.join(details)}).")


def _required_string(value: Mapping[str, Any], key: str, label: str) -> str:
  raw = value.get(key)
  if not isinstance(raw, str) or not raw.strip():
    raise PortableExpressionError(f"{label.capitalize()} requires non-empty '{key}'.")
  return raw.strip()


def _required_bool(value: Mapping[str, Any], key: str, label: str) -> bool:
  raw = value.get(key)
  if not isinstance(raw, bool):
    raise PortableExpressionError(f"{label.capitalize()} requires boolean '{key}'.")
  return raw


def _normalized_operator(
  value: Mapping[str, Any],
  key: str,
  allowed: set[str],
  label: str,
) -> str:
  operator = _required_string(value, key, f"{label} expression")
  normalized = operator.upper() if operator.upper() in {"LIKE", "AND", "OR"} else operator
  _ensure_operator(normalized, allowed, label)
  return normalized


def _ensure_operator(operator: str, allowed: set[str], label: str) -> None:
  if operator not in allowed:
    allowed_text = ", ".join(sorted(allowed))
    raise PortableExpressionError(
      f"Unsupported {label} operator {operator!r}. Allowed: {allowed_text}."
    )


def _validate_function_arity(name: str, count: int) -> None:
  minimum, maximum = PORTABLE_FUNCTION_ARITY[name]
  if count < minimum or count > maximum:
    if minimum == maximum:
      expected = str(minimum)
    else:
      expected = f"{minimum} to {maximum}"
    raise PortableExpressionError(
      f"Portable function {name} requires {expected} argument(s); got {count}."
    )


def _normalize_node_list(
  value: Any,
  label: str,
  minimum: int,
  *,
  allowed_runtime_values: frozenset[str],
) -> list[Dict[str, Any]]:
  if not isinstance(value, list) or len(value) < minimum:
    raise PortableExpressionError(
      f"{label.capitalize()} requires at least {minimum} expression part(s)."
    )
  return [
    _normalize_node(part, allowed_runtime_values=allowed_runtime_values)
    for part in value
  ]


def _is_json_scalar(value: Any) -> bool:
  if value is None or isinstance(value, (str, int, bool)):
    return True
  if isinstance(value, float):
    return math.isfinite(value)
  return False
