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

from dataclasses import dataclass

from metadata.models import TargetDataset
from metadata.rendering.dialects.base import SqlDialect
from metadata.rendering.renderer import render_select_for_target
from metadata.rendering.load_sql import (
  render_merge_sql as _render_merge_sql,
  render_delete_missing_rows_sql as _render_delete_missing_rows_sql,
  render_load_sql_for_target as _render_load_sql_for_target,
)


@dataclass(frozen=True)
class SqlPresentationPolicy:
  """Rules for how SQL is presented to humans (UI copy/paste)."""

  # Runtime-only tokens must remain unresolved in UI by default.
  preserve_tokens: set[str]


DEFAULT_PRESENTATION_POLICY = SqlPresentationPolicy(
  preserve_tokens={
    "DELTA_CUTOFF",
    "load_timestamp",
    "load_run_id",
  }
)


def _tokenize_sql(sql: str) -> list[str]:
  """Tokenize generated SQL without interpreting dialect-specific semantics."""
  tokens: list[str] = []
  i = 0
  length = len(sql)
  while i < length:
    ch = sql[i]

    if ch.isspace():
      i += 1
      continue

    if sql.startswith("--", i):
      j = sql.find("\n", i)
      if j == -1:
        j = length
      tokens.append(sql[i:j].rstrip())
      i = j
      continue

    if sql.startswith("/*", i):
      j = sql.find("*/", i + 2)
      if j == -1:
        j = length - 2
      tokens.append(sql[i:j + 2])
      i = j + 2
      continue

    if sql.startswith("{{", i):
      j = sql.find("}}", i + 2)
      if j == -1:
        j = length - 2
      tokens.append(sql[i:j + 2])
      i = j + 2
      continue

    if ch in {"'", '"', '`'}:
      quote = ch
      j = i + 1
      while j < length:
        if sql[j] == quote:
          if j + 1 < length and sql[j + 1] == quote:
            j += 2
            continue
          j += 1
          break
        j += 1
      tokens.append(sql[i:j])
      i = j
      continue

    if ch == "[":
      j = i + 1
      while j < length:
        if sql[j] == "]":
          if j + 1 < length and sql[j + 1] == "]":
            j += 2
            continue
          j += 1
          break
        j += 1
      tokens.append(sql[i:j])
      i = j
      continue

    matched = False
    for operator in ("<=", ">=", "<>", "!=", "||", "::", "->>", "->"):
      if sql.startswith(operator, i):
        tokens.append(operator)
        i += len(operator)
        matched = True
        break
    if matched:
      continue

    if ch in "(),.;+-*/%=<>":
      tokens.append(ch)
      i += 1
      continue

    j = i + 1
    while j < length:
      if sql[j].isspace() or sql[j] in "(),.;+-*/%=<>[]'\"`":
        break
      if sql.startswith("--", j) or sql.startswith("/*", j) or sql.startswith("{{", j):
        break
      j += 1
    tokens.append(sql[i:j])
    i = j

  return [token for token in tokens if token]


def _matching_paren(tokens: list[str], start: int) -> int | None:
  depth = 0
  for index in range(start, len(tokens)):
    if tokens[index] == "(":
      depth += 1
    elif tokens[index] == ")":
      depth -= 1
      if depth == 0:
        return index
  return None


def _starts_subquery(tokens: list[str], start: int, end: int) -> bool:
  for index in range(start, end):
    token = tokens[index]
    if token.startswith("--") or token.startswith("/*"):
      continue
    return token.upper() in {"SELECT", "WITH"}
  return False


def _format_sql_tokens(tokens: list[str], *, indent: int = 0) -> list[str]:
  """Format one SQL query level while preserving nested expression structure."""
  lines: list[str] = []
  current = ""
  clause = ""
  inline_depth = 0
  i = 0

  def prefix(level: int = 0) -> str:
    return "  " * (indent + level)

  def flush() -> None:
    nonlocal current
    text = current.rstrip()
    if text:
      lines.append(text)
    current = ""

  def append(token: str, *, leading_space: bool = True) -> None:
    nonlocal current
    if not current:
      current = prefix() + token
    elif leading_space and not current.rstrip().endswith((".", "(", "::", "->", "->>")):
      current += " " + token
    else:
      current += token

  def next_words(count: int) -> tuple[str, ...]:
    words: list[str] = []
    j = i
    while j < len(tokens) and len(words) < count:
      token = tokens[j]
      if token.startswith("--") or token.startswith("/*"):
        break
      words.append(token.upper())
      j += 1
    return tuple(words)

  while i < len(tokens):
    token = tokens[i]
    upper = token.upper()

    if token.startswith("--") or token.startswith("/*"):
      flush()
      lines.append(prefix() + token)
      i += 1
      continue

    if token == "(":
      close = _matching_paren(tokens, i)
      if close is not None and _starts_subquery(tokens, i + 1, close):
        append("(", leading_space=False)
        opening_indent = len(current) - len(current.lstrip(" "))
        flush()
        lines.extend(
          _format_sql_tokens(
            tokens[i + 1:close],
            indent=(opening_indent // 2) + 1,
          )
        )
        current = (" " * opening_indent) + ")"
        i = close + 1
        continue
      append("(", leading_space=False)
      inline_depth += 1
      i += 1
      continue

    if token == ")":
      append(")", leading_space=False)
      inline_depth = max(0, inline_depth - 1)
      i += 1
      continue

    words4 = next_words(4)
    words3 = words4[:3]
    words2 = words4[:2]

    join_phrase = None
    if words3[:3] in {
      ("LEFT", "OUTER", "JOIN"),
      ("RIGHT", "OUTER", "JOIN"),
      ("FULL", "OUTER", "JOIN"),
    }:
      join_phrase = " ".join(words3[:3])
      consumed = 3
    elif words2 in {
      ("LEFT", "JOIN"),
      ("RIGHT", "JOIN"),
      ("FULL", "JOIN"),
      ("INNER", "JOIN"),
      ("CROSS", "JOIN"),
    }:
      join_phrase = " ".join(words2)
      consumed = 2
    elif upper == "JOIN":
      join_phrase = "JOIN"
      consumed = 1
    else:
      consumed = 0

    if join_phrase and inline_depth == 0:
      flush()
      current = prefix(1) + join_phrase
      clause = "JOIN"
      i += consumed
      continue

    phrase = None
    consumed = 1
    if words3 in {
      ("WHEN", "NOT", "MATCHED"),
      ("THEN", "UPDATE", "SET"),
    }:
      phrase = " ".join(words3)
      consumed = 3
    elif words2 in {
      ("GROUP", "BY"),
      ("ORDER", "BY"),
      ("UNION", "ALL"),
      ("UNION", "DISTINCT"),
      ("WHEN", "MATCHED"),
      ("THEN", "INSERT"),
      ("INSERT", "INTO"),
    }:
      phrase = " ".join(words2)
      consumed = 2
    elif upper in {
      "SELECT",
      "FROM",
      "WHERE",
      "HAVING",
      "LIMIT",
      "QUALIFY",
      "UNION",
      "MERGE",
      "USING",
      "DELETE",
      "UPDATE",
      "VALUES",
    }:
      phrase = upper

    if phrase is not None and inline_depth == 0:
      if phrase == "SELECT":
        flush()
        lines.append(prefix() + "SELECT")
        current = prefix(1)
        clause = "SELECT"
      elif phrase.startswith("UNION"):
        flush()
        lines.append(prefix() + phrase)
        clause = "UNION"
      elif phrase.startswith("WHEN "):
        flush()
        lines.append(prefix() + phrase)
        clause = phrase
      elif phrase.startswith("THEN "):
        flush()
        lines.append(prefix(1) + phrase)
        current = prefix(2)
        clause = phrase
      elif phrase == "VALUES" and clause == "THEN INSERT":
        flush()
        lines.append(prefix(1) + "VALUES")
        current = prefix(2)
        clause = "VALUES"
      else:
        flush()
        lines.append(prefix() + phrase)
        current = prefix(1)
        clause = phrase
      i += consumed
      continue

    if upper == "ON" and inline_depth == 0:
      if clause == "JOIN" and current.strip():
        append("ON")
        flush()
        current = prefix(2)
      else:
        flush()
        lines.append(prefix(1) + "ON")
        current = prefix(2)
      clause = "ON"
      i += 1
      continue

    if inline_depth == 0 and upper in {"AND", "OR"} and clause in {"WHERE", "HAVING", "ON"}:
      flush()
      current = prefix(2 if clause == "ON" else 1) + upper
      i += 1
      continue

    if token == ",":
      current = current.rstrip() + ","
      if inline_depth == 0 and clause in {
        "SELECT",
        "GROUP BY",
        "ORDER BY",
        "THEN UPDATE SET",
      }:
        flush()
        current = prefix(2 if clause == "THEN UPDATE SET" else 1)
      else:
        pass
      i += 1
      continue

    if token == ".":
      append(".", leading_space=False)
      i += 1
      continue

    if token == ";":
      current = current.rstrip() + ";"
      flush()
      i += 1
      continue

    if token in {"::"}:
      append(token, leading_space=False)
      i += 1
      continue

    if token in {"+", "-", "*", "/", "%", "=", "<", ">", "<=", ">=", "<>", "!=", "||", "->", "->>"}:
      append(token)
      i += 1
      continue

    append(token, leading_space=bool(current.strip()))
    i += 1

  flush()
  return [line.rstrip() for line in lines if line.strip()]


def beautify_sql(sql: str) -> str:
  """Format generated SQL for deterministic, human-readable UI previews."""
  if not isinstance(sql, str):
    return ""

  stripped = sql.strip()
  if not stripped:
    return ""

  tokens = _tokenize_sql(stripped)
  return "\n".join(_format_sql_tokens(tokens)).strip()


def render_select_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
  *,
  presentation: bool = True,
) -> str:
  canonical = render_select_for_target(dataset, dialect)
  return _to_presentation(canonical, policy=DEFAULT_PRESENTATION_POLICY) if presentation else canonical


def render_merge_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
  *,
  presentation: bool = True,
) -> str:
  canonical = _render_merge_sql(dataset, dialect)
  if not presentation:
    return canonical
  presented = _to_presentation(canonical, policy=DEFAULT_PRESENTATION_POLICY)
  return beautify_sql(presented)


def render_delete_detection_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
  *,
  presentation: bool = True,
) -> str:
  canonical = _render_delete_missing_rows_sql(dataset, dialect)
  if not presentation:
    return canonical
  presented = _to_presentation(canonical, policy=DEFAULT_PRESENTATION_POLICY)
  return beautify_sql(presented)


def render_load_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
  *,
  presentation: bool = False,
) -> str:
  # Load/execution should default to canonical.
  canonical = _render_load_sql_for_target(dataset, dialect)
  return _to_presentation(canonical, policy=DEFAULT_PRESENTATION_POLICY) if presentation else canonical


def render_preview_sql(
  dataset: TargetDataset,
  dialect: SqlDialect,
) -> str:
  """Render SQL for UI preview after validating the active query contract."""
  query_head = getattr(dataset, "query_head", None) or getattr(dataset, "query_root", None)
  if query_head is not None:
    from metadata.generation.query_contract import infer_query_node_contract
    contract = infer_query_node_contract(query_head)
    errors = [
      str(issue)
      for issue in (getattr(contract, "issues", None) or [])
      if str(issue or "").startswith("ERROR:")
    ]
    if errors:
      raise ValueError("Query contract is invalid: " + "; ".join(errors))
  schema_short = getattr(getattr(dataset, "target_schema", None), "schema_short", None)
  name = getattr(dataset, "target_dataset_name", None)

  if (
    schema_short == "rawcore"
    and isinstance(name, str)
    and name.endswith("_hist")
  ):
    raw_sql = (
      f"-- SQL preview for history dataset {name} is not implemented yet.\n"
      f"-- Use the corresponding rawcore dataset for preview and load logic.\n"
    )
    return beautify_sql(raw_sql)

  raw_sql = render_select_sql(dataset, dialect, presentation=True)
  return beautify_sql(raw_sql)


def _to_presentation(sql: str, *, policy: SqlPresentationPolicy) -> str:
  """Convert canonical SQL into human-friendly SQL without changing semantics.

  Today this is intentionally a no-op because the only {{...}} tokens used in
  canonical SQL are runtime-only (e.g. DELTA_CUTOFF, load_timestamp, load_run_id).

  Later we can optionally support a 'sample context' substitution layer here.
  """
  _ = policy
  return sql
