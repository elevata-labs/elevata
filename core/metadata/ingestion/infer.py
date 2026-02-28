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

import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple


def try_parse_date(s: str) -> datetime.date | None:
  try:
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
      return datetime.date.fromisoformat(s)
  except Exception:
    return None
  return None


def try_parse_datetime(s: str) -> datetime.datetime | None:
  try:
    ss = s.replace("Z", "+00:00")
    if "T" in ss or " " in ss:
      return datetime.datetime.fromisoformat(ss)
  except Exception:
    return None
  return None


def try_parse_int(s: str) -> int | None:
  try:
    if s.strip() == "":
      return None
    if s.lstrip("+-").isdigit():
      return int(s)
  except Exception:
    return None
  return None


def try_parse_decimal(s: str) -> tuple[Decimal, int, int] | None:
  try:
    if s.strip() == "":
      return None
    d = Decimal(s)
    tup = d.as_tuple()
    digits = len(tup.digits)
    scale = -tup.exponent if tup.exponent < 0 else 0
    precision = digits
    return d, precision, scale
  except (InvalidOperation, Exception):
    return None


def kind_of_value(v: Any) -> str:
  # NULL, BOOLEAN, INTEGER, BIGINT, DECIMAL, FLOAT, DATE, TIMESTAMP, JSON, STRING
  if v is None:
    return "NULL"
  if isinstance(v, bool):
    return "BOOLEAN"
  if isinstance(v, int):
    return "INTEGER" if -(2**31) <= v <= (2**31 - 1) else "BIGINT"
  if isinstance(v, float):
    return "FLOAT"
  if isinstance(v, (dict, list)):
    return "JSON"
  if isinstance(v, str):
    s = v.strip()
    if s == "":
      return "NULL"
    dt = try_parse_datetime(s)
    if dt is not None:
      return "TIMESTAMP"
    d = try_parse_date(s)
    if d is not None:
      return "DATE"
    i = try_parse_int(s)
    if i is not None:
      return "INTEGER" if -(2**31) <= i <= (2**31 - 1) else "BIGINT"
    dec = try_parse_decimal(s)
    if dec is not None:
      return "DECIMAL"
    if s.lower() in ("true", "false"):
      return "BOOLEAN"
    return "STRING"
  return "STRING"


def promote_type(a: str, b: str) -> str:
  if a == b:
    return a
  if a == "NULL":
    return b
  if b == "NULL":
    return a
  if "JSON" in (a, b):
    return "JSON"
  if "STRING" in (a, b):
    return "STRING"
  order = ["BOOLEAN", "INTEGER", "BIGINT", "DECIMAL", "FLOAT", "DATE", "TIMESTAMP"]
  ia = order.index(a) if a in order else len(order)
  ib = order.index(b) if b in order else len(order)
  return order[max(ia, ib)] if max(ia, ib) < len(order) else "STRING"


def infer_column_profile(values: List[Any]) -> tuple[str, int | None, int | None, int | None]:
  """
  Infer (datatype, max_length, decimal_precision, decimal_scale) from sample values.
  Conservative: mixed types widen to STRING/JSON where appropriate.
  """
  kind = "NULL"
  max_len = 0
  dec_prec = 0
  dec_scale = 0

  for v in values:
    k = kind_of_value(v)
    kind = promote_type(kind, k)

    if isinstance(v, str):
      max_len = max(max_len, len(v))

    if k == "DECIMAL" and isinstance(v, str):
      parsed = try_parse_decimal(v.strip())
      if parsed is not None:
        _, p, s = parsed
        dec_prec = max(dec_prec, p)
        dec_scale = max(dec_scale, s)

  if kind in ("INTEGER", "BIGINT"):
    return (kind, None, None, None)
  if kind == "DECIMAL":
    p = dec_prec or None
    s = dec_scale or None
    return ("DECIMAL", None, p, s)
  if kind in ("FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "JSON"):
    return (kind, None, None, None)
  return ("STRING", (max_len or None), None, None)


def guess_pk_candidates(col_names: List[str]) -> List[str]:
  out = []
  for col in col_names:
    lc = col.lower()
    if lc in ("id", "uuid", "key"):
      out.append(col)
    elif lc.endswith("_id") or lc.endswith("_uuid") or lc.endswith("_key"):
      out.append(col)
  seen = set()
  res = []
  for c in out:
    if c not in seen:
      seen.add(c)
      res.append(c)
  return res


def unique_ratio(rows: List[Dict[str, Any]], col: str) -> Tuple[float, float]:
  vals = []
  present = 0
  for r in rows:
    if col in r and r[col] not in (None, ""):
      present += 1
      vals.append(r[col])
  if not rows:
    return 0.0, 0.0
  presence = present / float(len(rows))
  if present == 0:
    return presence, 0.0
  uniq = len(set(vals)) / float(len(vals))
  return presence, uniq


def infer_pk_columns(rows: List[Dict[str, Any]], col_names: List[str]) -> List[str]:
  cands = guess_pk_candidates(col_names)
  pk_final: List[str] = []
  for c in cands:
    presence, uniq = unique_ratio(rows, c)
    if presence >= 0.95 and uniq >= 0.99:
      pk_final.append(c)
  return pk_final


def get_value_by_json_path(rec: Dict[str, Any], json_path: str) -> Any:
  """
  Fast accessor for simple paths used by REST import flattening:
    - "$.k"
    - "$.k.k2"
  """
  p = (json_path or "").strip()
  if not p.startswith("$."):
    return None
  cur: Any = rec
  for part in p[2:].split("."):
    if not isinstance(cur, dict):
      return None
    cur = cur.get(part)
  return cur
