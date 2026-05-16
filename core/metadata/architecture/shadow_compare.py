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
from typing import Any, Iterable
import json
import re


SCHEMA_OP_ACTION_TYPES = {
  "RENAME_DATASET",
  "RENAME_COLUMN",
  "ADD_COLUMN",
  "DROP_COLUMN",
  "ALTER_COLUMN",
  "REBUILD_DATASET",
}

COLUMN_SCHEMA_OPS = {
  "RENAME_COLUMN",
  "ADD_COLUMN",
  "DROP_COLUMN",
  "ALTER_COLUMN",
}


@dataclass(frozen=True)
class SchemaOpTokenBuildResult:
  """
  Schema operation tokens derived from architecture intent.
  """
  tokens: tuple[str, ...]
  suppressed_full_refresh_col_renames: int = 0
  suppressed_full_refresh_add_columns: int = 0


@dataclass(frozen=True)
class SchemaOpCompareResult:
  """
  Comparison result for architecture intent and materialization steps.
  """
  expected: tuple[str, ...]
  actual: tuple[str, ...]
  missing: tuple[str, ...]
  unexpected: tuple[str, ...]
  suppressed_by_rebuild: tuple[str, ...]
  suppressed_rebuild_steps: tuple[str, ...]
  suppressed_hist_drop_columns: tuple[str, ...]

  @property
  def is_mismatch(self) -> bool:
    """
    Return True if expected and actual schema operation tokens differ.
    """
    return bool(self.missing or self.unexpected)

  def to_dict(self) -> dict[str, Any]:
    """
    Return a deterministic dictionary representation.
    """
    return {
      "expected": list(self.expected),
      "actual": list(self.actual),
      "missing": list(self.missing),
      "unexpected": list(self.unexpected),
      "suppressed_by_rebuild": list(self.suppressed_by_rebuild),
      "suppressed_rebuild_steps": list(self.suppressed_rebuild_steps),
      "suppressed_hist_drop_columns": list(self.suppressed_hist_drop_columns),
      "is_mismatch": self.is_mismatch,
    }


def make_schema_op_token(op: str, **parts: str | None) -> str:
  """
  Create a deterministic schema operation token.
  """
  payload = {
    "op": str(op),
  }
  for key, value in parts.items():
    if value is None:
      continue
    value_str = str(value).strip()
    if value_str:
      payload[str(key)] = value_str

  return json.dumps(
    payload,
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
  )


def parse_schema_op_token(token: str) -> dict[str, str] | None:
  """
  Parse a deterministic schema operation token.
  """
  try:
    raw = json.loads(token)
  except (TypeError, ValueError, json.JSONDecodeError):
    return None

  if not isinstance(raw, dict):
    return None

  out: dict[str, str] = {}
  for key, value in raw.items():
    if value is None:
      continue
    out[str(key)] = str(value)

  if not out.get("op"):
    return None

  return out


def format_schema_op_token(token: str) -> str:
  """
  Format a schema operation token for CLI output.
  """
  parsed = parse_schema_op_token(token)
  if parsed is None:
    return token

  op = parsed.get("op", "")

  if op == "RENAME_DATASET":
    return f"RENAME_DATASET {parsed.get('prev', '?')} -> {parsed.get('cur', '?')}"

  if op == "RENAME_COLUMN":
    return (
      f"RENAME_COLUMN {parsed.get('ds', '?')}."
      f"{parsed.get('prev', '?')} -> {parsed.get('cur', '?')}"
    )

  if op in {"ADD_COLUMN", "DROP_COLUMN", "ALTER_COLUMN"}:
    return f"{op} {parsed.get('ds', '?')}.{parsed.get('col', '?')}"

  if op == "REBUILD_DATASET":
    return f"REBUILD_DATASET {parsed.get('ds', '?')}"

  return token


def schema_op_token_for_action(action: Any) -> str | None:
  """
  Return the schema operation token for a migration action.
  """
  action_type = str(getattr(action, "action_type", "") or "")

  if action_type == "RENAME_DATASET":
    previous_dataset_key = getattr(action, "previous_dataset_key", None)
    dataset_key = getattr(action, "dataset_key", None)
    if previous_dataset_key and dataset_key:
      return make_schema_op_token(
        "RENAME_DATASET",
        prev=str(previous_dataset_key),
        cur=str(dataset_key),
      )

  if action_type == "RENAME_COLUMN":
    dataset_key = getattr(action, "dataset_key", None)
    previous_column_name = getattr(action, "previous_column_name", None)
    column_name = getattr(action, "column_name", None)
    if dataset_key and previous_column_name and column_name:
      return make_schema_op_token(
        "RENAME_COLUMN",
        ds=str(dataset_key),
        prev=str(previous_column_name),
        cur=str(column_name),
      )

  if action_type == "ADD_COLUMN":
    dataset_key = getattr(action, "dataset_key", None)
    column_name = getattr(action, "column_name", None)
    if dataset_key and column_name:
      return make_schema_op_token(
        "ADD_COLUMN",
        ds=str(dataset_key),
        col=str(column_name),
      )

  if action_type == "DROP_COLUMN":
    dataset_key = getattr(action, "dataset_key", None)
    column_name = getattr(action, "column_name", None)
    if dataset_key and column_name:
      return make_schema_op_token(
        "DROP_COLUMN",
        ds=str(dataset_key),
        col=str(column_name),
      )

  if action_type == "ALTER_COLUMN":
    dataset_key = getattr(action, "dataset_key", None)
    column_name = getattr(action, "column_name", None)
    if dataset_key and column_name:
      return make_schema_op_token(
        "ALTER_COLUMN",
        ds=str(dataset_key),
        col=str(column_name),
      )

  if action_type == "REBUILD_DATASET":
    dataset_key = getattr(action, "dataset_key", None)
    if dataset_key:
      return make_schema_op_token(
        "REBUILD_DATASET",
        ds=str(dataset_key),
      )

  return None


def build_expected_schema_op_tokens(
  *,
  actions: Iterable[Any],
  full_refresh_dataset_keys: Iterable[str] = (),
) -> SchemaOpTokenBuildResult:
  """
  Build expected schema operation tokens from migration actions.
  """
  full_refresh_keys = {
    str(dataset_key)
    for dataset_key in full_refresh_dataset_keys
    if dataset_key
  }

  tokens: list[str] = []
  suppressed_renames = 0
  suppressed_adds = 0

  for action in actions:
    action_type = str(getattr(action, "action_type", "") or "")
    if action_type not in SCHEMA_OP_ACTION_TYPES:
      continue

    dataset_key = str(getattr(action, "dataset_key", "") or "")

    if action_type == "RENAME_COLUMN" and dataset_key in full_refresh_keys:
      suppressed_renames += 1
      continue

    if action_type == "ADD_COLUMN" and dataset_key in full_refresh_keys:
      suppressed_adds += 1
      continue

    token = schema_op_token_for_action(action)
    if token:
      tokens.append(token)

  return SchemaOpTokenBuildResult(
    tokens=tuple(sorted(set(tokens))),
    suppressed_full_refresh_col_renames=suppressed_renames,
    suppressed_full_refresh_add_columns=suppressed_adds,
  )


def build_actual_schema_op_tokens_from_plan(
  *,
  plan: Any,
  schema_short: str,
) -> tuple[str, ...]:
  """
  Build actual schema operation tokens from a materialization plan.
  """
  tokens: set[str] = set()
  steps = list(getattr(plan, "steps", None) or [])
  dataset_key = str(getattr(plan, "dataset_key", "") or "")

  for step in steps:
    op = str(getattr(step, "op", "") or "")
    reason = str(getattr(step, "reason", "") or "")
    sql = str(getattr(step, "sql", "") or "")

    if op == "RENAME_COLUMN":
      token = _rename_column_token_from_step(
        dataset_key=dataset_key,
        reason=reason,
      )
      if token:
        tokens.add(token)
      continue

    if op == "RENAME_DATASET":
      token = _rename_dataset_token_from_step(
        schema_short=schema_short,
        reason=reason,
      )
      if token:
        tokens.add(token)
      continue

    if op == "ADD_COLUMN":
      column_name = _add_column_name_from_step(
        reason=reason,
        sql=sql,
      )
      if dataset_key and column_name:
        tokens.add(make_schema_op_token(
          "ADD_COLUMN",
          ds=dataset_key,
          col=column_name,
        ))
      continue

    if op == "ALTER_COLUMN_TYPE":
      column_name = _alter_column_name_from_step(reason=reason)
      if dataset_key and column_name:
        tokens.add(make_schema_op_token(
          "ALTER_COLUMN",
          ds=dataset_key,
          col=column_name,
        ))
      continue

    if op == "DROP_COLUMN":
      column_name = _drop_column_name_from_step(
        reason=reason,
        sql=sql,
      )
      if dataset_key and column_name:
        tokens.add(make_schema_op_token(
          "DROP_COLUMN",
          ds=dataset_key,
          col=column_name,
        ))
      continue

  if bool(getattr(plan, "requires_rebuild", False)) and dataset_key:
    tokens.add(make_schema_op_token(
      "REBUILD_DATASET",
      ds=dataset_key,
    ))

  return tuple(sorted(tokens))


def compare_schema_op_tokens(
  *,
  expected: Iterable[str],
  actual: Iterable[str],
  allow_hist_drop: bool = False,
) -> SchemaOpCompareResult:
  """
  Compare expected and actual schema operation tokens.
  """
  expected_set = {
    str(token)
    for token in expected
    if token
  }
  actual_set = {
    str(token)
    for token in actual
    if token
  }

  missing = sorted(expected_set - actual_set)
  unexpected = sorted(actual_set - expected_set)

  missing, suppressed_by_rebuild = _suppress_missing_column_ops_by_rebuild(
    missing=missing,
    actual=actual_set,
  )

  unexpected, suppressed_rebuild_steps = _suppress_unexpected_rebuild_steps(
    expected=expected_set,
    unexpected=unexpected,
  )

  missing, suppressed_hist_drop_columns = _suppress_hist_drop_columns(
    missing=missing,
    allow_hist_drop=allow_hist_drop,
  )

  return SchemaOpCompareResult(
    expected=tuple(sorted(expected_set)),
    actual=tuple(sorted(actual_set)),
    missing=tuple(sorted(missing)),
    unexpected=tuple(sorted(unexpected)),
    suppressed_by_rebuild=tuple(sorted(suppressed_by_rebuild)),
    suppressed_rebuild_steps=tuple(sorted(suppressed_rebuild_steps)),
    suppressed_hist_drop_columns=tuple(sorted(suppressed_hist_drop_columns)),
  )


def _rename_column_token_from_step(*, dataset_key: str, reason: str) -> str | None:
  """
  Return a rename-column token from a materialization step reason.
  """
  match = re.search(r"Rename column\s+(.+?)\s+->\s+(.+?)(?:\s|\(|$)", reason)
  if not match:
    return None

  old_name = (match.group(1) or "").strip()
  new_name = (match.group(2) or "").strip()
  if not dataset_key or not old_name or not new_name:
    return None

  return make_schema_op_token(
    "RENAME_COLUMN",
    ds=dataset_key,
    prev=old_name,
    cur=new_name,
  )


def _rename_dataset_token_from_step(*, schema_short: str, reason: str) -> str | None:
  """
  Return a rename-dataset token from a materialization step reason.
  """
  match = re.search(r":\s*(.+?)\s*->\s*(.+?)\s*$", reason)
  if not match:
    return None

  old_table = (match.group(1) or "").strip()
  new_table = (match.group(2) or "").strip()
  if not schema_short or not old_table or not new_table:
    return None

  return make_schema_op_token(
    "RENAME_DATASET",
    prev=f"{schema_short}.{old_table}",
    cur=f"{schema_short}.{new_table}",
  )


def _add_column_name_from_step(*, reason: str, sql: str) -> str | None:
  """
  Return an added column name from a materialization step.
  """
  match = re.search(r"Column\s+(.+?)\s+missing", reason)
  if match:
    return (match.group(1) or "").strip()

  ident = r"(`[^`]+`|\"[^\"]+\"|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)"
  match = re.search(
    rf"\balter\s+table\b.*?\badd\s+(?:column\s+)?(?P<col>{ident})(?=\s|,|\)|$)",
    sql,
    flags=re.IGNORECASE,
  )
  if not match:
    return None

  return _clean_identifier(match.group("col"))


def _alter_column_name_from_step(*, reason: str) -> str | None:
  """
  Return an altered column name from a materialization step.
  """
  match = re.search(r"\balter\s+(.+?)\s+to\b", reason, flags=re.IGNORECASE)
  if not match:
    return None
  return (match.group(1) or "").strip()


def _drop_column_name_from_step(*, reason: str, sql: str) -> str | None:
  """
  Return a dropped column name from a materialization step.
  """
  ident = r"(`[^`]+`|\"[^\"]+\"|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)"
  match = re.search(
    rf"\bdrop\s+column\s+(?P<col>{ident})(?=\s|,|\)|$)",
    sql,
    flags=re.IGNORECASE,
  )
  if match:
    return _clean_identifier(match.group("col"))

  match = re.search(r"\.([A-Za-z_][A-Za-z0-9_]*)\s*$", reason)
  if match:
    return (match.group(1) or "").strip()

  return None


def _clean_identifier(value: str | None) -> str | None:
  """
  Normalize a parsed SQL identifier.
  """
  if value is None:
    return None
  cleaned = str(value).strip()
  cleaned = cleaned.strip("`").strip('"').strip("[]").strip()
  return cleaned or None


def _suppress_missing_column_ops_by_rebuild(
  *,
  missing: list[str],
  actual: set[str],
) -> tuple[list[str], list[str]]:
  """
  Suppress missing column operations covered by dataset rebuild steps.
  """
  rebuild_dataset_keys = set()
  for token in actual:
    parsed = parse_schema_op_token(token) or {}
    if parsed.get("op") == "REBUILD_DATASET" and parsed.get("ds"):
      rebuild_dataset_keys.add(str(parsed["ds"]))

  if not rebuild_dataset_keys:
    return missing, []

  kept: list[str] = []
  suppressed: list[str] = []

  for token in missing:
    parsed = parse_schema_op_token(token) or {}
    if parsed.get("op") in COLUMN_SCHEMA_OPS:
      dataset_key = str(parsed.get("ds") or "")
      if dataset_key in rebuild_dataset_keys:
        suppressed.append(token)
        continue
    kept.append(token)

  return kept, suppressed


def _suppress_unexpected_rebuild_steps(
  *,
  expected: set[str],
  unexpected: list[str],
) -> tuple[list[str], list[str]]:
  """
  Suppress rebuild steps that satisfy column-level architecture intent.
  """
  expected_dataset_keys = set()
  for token in expected:
    parsed = parse_schema_op_token(token) or {}
    if parsed.get("op") in COLUMN_SCHEMA_OPS and parsed.get("ds"):
      expected_dataset_keys.add(str(parsed["ds"]))

  if not expected_dataset_keys:
    return unexpected, []

  kept: list[str] = []
  suppressed: list[str] = []

  for token in unexpected:
    parsed = parse_schema_op_token(token) or {}
    if (
      parsed.get("op") == "REBUILD_DATASET"
      and str(parsed.get("ds") or "") in expected_dataset_keys
    ):
      suppressed.append(token)
      continue
    kept.append(token)

  return kept, suppressed


def _suppress_hist_drop_columns(
  *,
  missing: list[str],
  allow_hist_drop: bool,
) -> tuple[list[str], list[str]]:
  """
  Suppress missing history column drops when history drops are not enabled.
  """
  if allow_hist_drop:
    return missing, []

  kept: list[str] = []
  suppressed: list[str] = []

  for token in missing:
    parsed = parse_schema_op_token(token) or {}
    if parsed.get("op") == "DROP_COLUMN":
      dataset_key = str(parsed.get("ds") or "")
      if dataset_key.endswith("_hist"):
        suppressed.append(token)
        continue
    kept.append(token)

  return kept, suppressed