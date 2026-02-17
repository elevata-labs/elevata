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

import re
from typing import Optional, Tuple, Literal

"""
Canonical type mapper for multiple SQL dialects.

Return format (ALWAYS a 4-tuple):
  (datatype: str, max_length: Optional[int], decimal_precision: Optional[int], decimal_scale: Optional[int])

Supported dialect hints: mssql, postgresql, mysql, oracle, snowflake, bigquery,
redshift, duckdb, hana/sap_hana, fabric, sap_r3/abap
"""

# Canonical normalized types
STRING = "STRING"
INTEGER = "INTEGER"   # SMALLINT/TINYINT normalize to INTEGER
BIGINT = "BIGINT"
DECIMAL = "DECIMAL"
FLOAT = "FLOAT"
BOOLEAN = "BOOLEAN"
DATE = "DATE"
TIME = "TIME"
TIMESTAMP = "TIMESTAMP"
BINARY = "BINARY"
UUID = "UUID"
JSON = "JSON"

_PARAMS_RE = re.compile(r"\(([^)]+)\)")  # matches content inside parentheses, e.g. "(10,2)"

CanonicalTypeTuple = Tuple[str, Optional[int], Optional[int], Optional[int]]

TypeDriftKind = Literal["equivalent", "widening", "narrowing", "incompatible", "unknown"]


def _extract_params(raw: str) -> Tuple[Optional[int], Optional[int], Optional[int], bool]:
  """
  Extract (length, precision, scale, has_max) from a type string like:
    NVARCHAR(100)  -> (100, None, None, has_max=False)
    DECIMAL(12,2)  -> (None, 12, 2,   has_max=False)
    VARCHAR(MAX)   -> (None, None, None, has_max=True)
  """
  t = raw.strip().lower()
  has_max = "max" in t
  m = _PARAMS_RE.search(t)
  if not m:
    return None, None, None, has_max
  parts = [p.strip() for p in m.group(1).split(",")]
  if len(parts) == 1:
    try:
      n = int(parts[0])
      return n, None, None, has_max
    except ValueError:
      return None, None, None, has_max
  try:
    prec = int(parts[0])
    scale = int(parts[1])
    return None, prec, scale, has_max
  except ValueError:
    return None, None, None, has_max


def _ret(datatype: str,
         max_length: Optional[int] = None,
         decimal_precision: Optional[int] = None,
         decimal_scale: Optional[int] = None) -> CanonicalTypeTuple:  
  """Helper: return the canonical 4-tuple."""
  return (datatype, max_length, decimal_precision, decimal_scale)


def _generic_fallback(t: str,
                      length: Optional[int],
                      prec: Optional[int],
                      scale: Optional[int]) -> CanonicalTypeTuple:  
  tl = t.lower()
  if any(x in tl for x in ["char", "text", "string"]):
    return _ret(STRING, length, None, None)
  if any(x in tl for x in ["numeric", "decimal", "number", "money", "bignumeric"]):
    return _ret(DECIMAL, None, prec, scale)
  if any(x in tl for x in ["double", "float", "real"]):
    return _ret(FLOAT)
  if "bigint" in tl:
    return _ret(BIGINT)
  if any(x in tl for x in ["int", "integer", "smallint", "tinyint", "mediumint"]):
    return _ret(INTEGER)
  if "boolean" in tl or "bool" in tl or tl.strip() == "bit":
    return _ret(BOOLEAN)
  if "uuid" in tl or "uniqueidentifier" in tl:
    return _ret(UUID)
  if any(x in tl for x in ["json", "variant", "super", "object", "array", "geography"]):
    return _ret(JSON)
  if "date" in tl and "time" in tl:
    return _ret(TIMESTAMP)
  if tl.startswith("timestamp") or "datetime" in tl:
    return _ret(TIMESTAMP)
  if tl.strip() == "date":
    return _ret(DATE)
  if tl.startswith("time"):
    return _ret(TIME)
  if any(x in tl for x in ["bytea", "binary", "varbinary", "blob", "image", "raw", "bytes"]):
    return _ret(BINARY)
  return _ret(STRING)


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def map_sql_type(dialect_name: str, raw_type: object) -> CanonicalTypeTuple:
  """
  Normalize a dialect-specific type (string or SQLAlchemy type object) into elevata's
  canonical quadruple: (datatype, max_length, decimal_precision, decimal_scale).
  """
  # Normalize to string
  if not isinstance(raw_type, str):
    raw_type = str(raw_type)
  t = raw_type.strip().lower()

  length, prec, scale, has_max = _extract_params(t)
  d = (dialect_name or "").lower()

  # ---------------- MSSQL ----------------
  if d == "mssql":
    if any(x in t for x in ["nvarchar", "varchar", "nchar", "char", "ntext", "text"]):
      if has_max:
        return _ret(STRING)
      L = length if any(x in t for x in ["char", "varchar", "nvarchar"]) and length else None
      return _ret(STRING, L)
    if t == "bit":
      return _ret(BOOLEAN)
    if "bigint" in t:
      return _ret(BIGINT)
    if any(x in t for x in ["tinyint", "smallint", "int"]):
      return _ret(INTEGER)    
    if t == "money":
      return _ret(DECIMAL, None, 19, 4)
    if t == "smallmoney":
      return _ret(DECIMAL, None, 10, 4)
    if any(x in t for x in ["decimal", "numeric"]):
      return _ret(DECIMAL, None, prec, scale)
    if "float" in t or "real" in t:
      return _ret(FLOAT)
    if any(x in t for x in ["datetime", "datetime2", "smalldatetime", "datetimeoffset", "timestamp"]):
      return _ret(TIMESTAMP)
    if t == "date":
      return _ret(DATE)
    if t.startswith("time"):
      return _ret(TIME)
    if "uniqueidentifier" in t:
      return _ret(UUID)
    if any(x in t for x in ["varbinary", "binary", "image", "rowversion"]):
      return _ret(BINARY)

  # -------------- PostgreSQL --------------
  elif d == "postgresql":
    if any(x in t for x in ["character varying", "varchar", "char", "text"]):
      L = length if (length and any(x in t for x in ["char", "varchar", "character varying"])) else None
      return _ret(STRING, L)
    if "boolean" in t:
      return _ret(BOOLEAN)
    if "bigint" in t or t == "int8":
      return _ret(BIGINT)
    if any(x in t for x in ["integer", "int4", "smallint", "int2"]):
      return _ret(INTEGER)
    if any(x in t for x in ["numeric", "decimal"]):
      return _ret(DECIMAL, None, prec, scale)
    if any(x in t for x in ["double precision", "real", "float"]):
      return _ret(FLOAT)
    if t.startswith("timestamp") or "timestamp" in t or "timestamptz" in t:
      return _ret(TIMESTAMP)
    if t == "date":
      return _ret(DATE)
    if t.startswith("time"):
      return _ret(TIME)
    if "uuid" in t:
      return _ret(UUID)
    if "bytea" in t:
      return _ret(BINARY)
    if "jsonb" in t or t == "json":
      return _ret(JSON)

  # ---------------- MySQL ----------------
  elif d == "mysql":
    if any(x in t for x in ["varchar", "char", "text", "tinytext", "mediumtext", "longtext"]):
      L = length if (length and any(x in t for x in ["char", "varchar"])) else None
      return _ret(STRING, L)
    # boolean alias (tinyint(1))
    if t.startswith("tinyint(1)") or t in ("boolean", "bool"):
      return _ret(BOOLEAN)
    if "bigint" in t:
      return _ret(BIGINT)
    if any(x in t for x in ["tinyint", "smallint", "mediumint", "int"]):
      return _ret(INTEGER)
    if any(x in t for x in ["decimal", "numeric"]):
      return _ret(DECIMAL, None, prec, scale)
    if any(x in t for x in ["float", "double"]):
      return _ret(FLOAT)
    if "datetime" in t or t == "timestamp":
      return _ret(TIMESTAMP)
    if t == "date":
      return _ret(DATE)
    if t == "time":
      return _ret(TIME)
    if any(x in t for x in ["binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob"]):
      return _ret(BINARY)
    if "json" in t:
      return _ret(JSON)

  # ---------------- Oracle ----------------
  elif d == "oracle":
    # Strings
    if any(x in t for x in ["varchar2", "nvarchar2", "char", "nchar", "clob", "nclob"]):
      L = length if (length and any(x in t for x in ["char", "varchar2", "nvarchar2"])) else None
      return _ret(STRING, L)
    # Numbers
    if t.startswith("number") or "numeric" in t or "decimal" in t:
      return _ret(DECIMAL, None, prec, scale)
    if any(x in t for x in ["binary_float", "binary_double", "float"]):
      return _ret(FLOAT)
    # Dates & times
    if t == "date":  # Oracle DATE includes time → normalize to TIMESTAMP
      return _ret(TIMESTAMP)
    if t.startswith("timestamp"):
      return _ret(TIMESTAMP)
    # Binary
    if any(x in t for x in ["raw", "long raw", "blob"]):
      return _ret(BINARY)

  # --------------- Snowflake ---------------
  elif d == "snowflake":
    if any(x in t for x in ["varchar", "string", "char", "text"]):
      L = length if (length and any(x in t for x in ["char", "varchar", "string"])) else None
      return _ret(STRING, L)
    if t.startswith("number") or "decimal" in t or "numeric" in t:
      # Snowflake frequently represents integer columns as NUMBER(38,0).
      # Treat NUMBER(38,0) as INTEGER to avoid persistent false-positive drift.
      if (prec == 38 and (scale == 0 or scale is None)):
        return _ret(INTEGER)
      return _ret(DECIMAL, None, prec, scale)
    if "bigint" in t:
      return _ret(BIGINT)
    if any(x in t for x in ["int", "integer", "smallint", "tinyint", "byteint"]):
      return _ret(INTEGER)
    if any(x in t for x in ["float", "double", "real"]):
      return _ret(FLOAT)
    if "boolean" in t:
      return _ret(BOOLEAN)
    if t == "date":
      return _ret(DATE)
    if t.startswith("time"):
      return _ret(TIME)
    if t.startswith("timestamp"):
      return _ret(TIMESTAMP)
    if any(x in t for x in ["variant", "object", "array"]):
      return _ret(JSON)
    if "binary" in t:
      return _ret(BINARY)

  # ---------------- BigQuery ----------------
  elif d == "bigquery":
    if t == "string":
      return _ret(STRING)
    if t == "bytes":
      return _ret(BINARY)
    if t == "int64":
      return _ret(INTEGER)
    if t in ("bignumeric", "numeric", "decimal"):
      return _ret(DECIMAL, None, prec, scale)
    if t == "float64":
      return _ret(FLOAT)
    if t in ("bool", "boolean"):
      return _ret(BOOLEAN)
    if t == "date":
      return _ret(DATE)
    if t == "time":
      return _ret(TIME)
    if t in ("datetime", "timestamp"):
      return _ret(TIMESTAMP)
    if t in ("json", "geography"):
      return _ret(JSON)

  # ---------------- Redshift ----------------
  elif d == "redshift":
    if any(x in t for x in ["varchar", "char", "bpchar"]):
      L = length if (length and any(x in t for x in ["varchar", "char", "bpchar"])) else None
      return _ret(STRING, L)
    if "boolean" in t:
      return _ret(BOOLEAN)
    if "bigint" in t:
      return _ret(BIGINT)
    if any(x in t for x in ["int", "integer", "smallint"]):
      return _ret(INTEGER)
    if any(x in t for x in ["numeric", "decimal"]):
      return _ret(DECIMAL, None, prec, scale)
    if any(x in t for x in ["real", "float4", "double precision", "float8"]):
      return _ret(FLOAT)
    if t == "date":
      return _ret(DATE)
    if any(x in t for x in ["timestamp", "timestamptz"]):
      return _ret(TIMESTAMP)
    if "super" in t:
      return _ret(JSON)

  # ---------------- DuckDB ----------------
  elif d == "duckdb":
    if any(x in t for x in ["varchar", "char", "text"]):
      L = length if (length and any(x in t for x in ["char", "varchar"])) else None
      return _ret(STRING, L)
    if any(x in t for x in ["boolean", "bool"]):
      return _ret(BOOLEAN)
    # hugeint → BIGINT (simplify)
    if "hugeint" in t or "bigint" in t:
      return _ret(BIGINT)
    if any(x in t for x in ["tinyint", "smallint", "int", "integer", "uint32"]):
      return _ret(INTEGER)
    if any(x in t for x in ["decimal", "numeric"]):
      return _ret(DECIMAL, None, prec, scale)
    if any(x in t for x in ["double", "float", "real"]):
      return _ret(FLOAT)
    if t == "date":
      return _ret(DATE)
    if t.startswith("time"):
      return _ret(TIME)
    if any(x in t for x in ["timestamp", "timestamptz"]):
      return _ret(TIMESTAMP)
    if "json" in t:
      return _ret(JSON)
    if any(x in t for x in ["blob", "binary", "varbinary"]):
      return _ret(BINARY)

  # ---------------- SAP HANA ----------------
  elif d in ("sap", "hana", "sap_hana", "saphana"):
    # Strings
    if any(x in t for x in ["nvarchar", "varchar", "nchar", "char", "alphanum", "shorttext", "clob", "nclob", "text"]):
      L = length if (length and any(x in t for x in ["char", "varchar", "nvarchar", "nchar", "alphanum", "shorttext"])) else None
      return _ret(STRING, L)
    # Integers
    if any(x in t for x in ["tinyint", "smallint", "integer", "int", "bigint"]):
      if "bigint" in t:
        return _ret(BIGINT)
      return _ret(INTEGER)
    # Decimals
    if any(x in t for x in ["decimal", "dec", "numeric", "number"]):
      return _ret(DECIMAL, None, prec, scale)
    # Floats
    if any(x in t for x in ["real", "double", "float", "binary_float", "binary_double"]):
      return _ret(FLOAT)
    # Dates & times
    if t == "date":
      return _ret(DATE)
    if t.startswith("time"):
      return _ret(TIME)
    if any(x in t for x in ["timestamp", "seconddate", "longdate", "longtime"]):
      return _ret(TIMESTAMP)
    # Boolean
    if "boolean" in t or t == "bool":
      return _ret(BOOLEAN)
    # UUID / Binary
    if "uuid" in t:
      return _ret(UUID)
    if any(x in t for x in ["varbinary", "binary", "blob"]):
      return _ret(BINARY)
    # JSON
    if "json" in t:
      return _ret(JSON)

  # ---------------- Fabric (Warehouse) ----------------
  elif d == "fabric":
    # Strings (Fabric Warehouse typically VARCHAR/CHAR/TEXT)
    if any(x in t for x in ["varchar", "char", "text"]):
      L = length if (length and any(x in t for x in ["char", "varchar"])) else None
      return _ret(STRING, L)
    # Boolean (sometimes BIT)
    if t == "bit" or "boolean" in t or "bool" in t:
      return _ret(BOOLEAN)
    # Integers
    if "bigint" in t:
      return _ret(BIGINT)
    if any(x in t for x in ["tinyint", "smallint", "int", "integer"]):
      return _ret(INTEGER)
    # Decimals
    if any(x in t for x in ["decimal", "numeric"]):
      return _ret(DECIMAL, None, prec, scale)
    # Floats
    if any(x in t for x in ["float", "double", "real"]):
      return _ret(FLOAT)
    # Dates & times
    if t == "date":
      return _ret(DATE)
    if t.startswith("time"):
      return _ret(TIME)
    if any(x in t for x in ["datetime", "datetime2", "timestamp"]):
      return _ret(TIMESTAMP)
    # Binary
    if any(x in t for x in ["binary", "varbinary", "image"]):
      return _ret(BINARY)
    # JSON
    if "json" in t:
      return _ret(JSON)

  # ---------------- SAP R/3 / ABAP ----------------
  elif d in ("sap_r3", "abap", "sap"):
    # 1) Character-like
    if t.startswith("char") or t.startswith("nchar"):
      return _ret(STRING, length)
    if t.startswith("lchr") or t == "string":
      return _ret(STRING)
    if t.startswith("numc"):
      return _ret(STRING, length)

    # 2) Dates & times
    if t == "dats":
      return _ret(DATE)
    if t == "tims":
      return _ret(TIME)

    # 3) Packed decimals (ABAP type P) and related domains
    if t.startswith("dec") or t == "p" or t.startswith("p(") or t.startswith("curr") or t.startswith("quan"):
      return _ret(DECIMAL, None, prec or length, scale)

    # 4) Integers
    if t in ("int1", "int2", "int4"):
      return _ret(INTEGER)
    if t == "int8":
      return _ret(BIGINT)

    # 5) Floating point
    if t == "fltp" or t.startswith("float"):
      return _ret(FLOAT)

    # 6) Binary / raw
    if t.startswith("raw") or t in ("xstring", "rawstring", "lraw"):
      L = length if t.startswith("raw(") else None
      return _ret(BINARY, L)

    # 7) Common DDIC domains
    if t == "clnt":
      return _ret(STRING, 3)
    if t == "lang":
      return _ret(STRING, 1)
    if t == "cuky":
      return _ret(STRING, 5)
    if t == "unit":
      return _ret(STRING, 3)

    return _ret(STRING)  # fallback for unusual DDIC types

  # ------------- Fallback (generic) -------------
  return _generic_fallback(t, length, prec, scale)


# -----------------------------------------------------------------------------
# Type drift helpers (planner / preflight)
# -----------------------------------------------------------------------------
def canonicalize_type(dialect_name: str, raw_type: object) -> CanonicalTypeTuple:
  """
  Convenience wrapper around map_sql_type().
  """
  return map_sql_type(dialect_name=dialect_name, raw_type=raw_type)


def canonical_type_str(t: CanonicalTypeTuple) -> str:
  """
  Stable, human-readable type signature used for drift messages.
  """
  dt, ml, p, s = t
  dt = (dt or "").upper()
  if dt == STRING:
    return f"{dt}({ml})" if ml else dt
  if dt == DECIMAL:
    if p is not None and s is not None:
      return f"{dt}({p},{s})"
    if p is not None:
      return f"{dt}({p})"
    return dt
  return dt


def semantically_equivalent(desired: CanonicalTypeTuple, actual: CanonicalTypeTuple) -> bool:
  """
  Determine if two canonical types should be treated as equivalent
  for drift detection (noise suppression).

  Rules are intentionally conservative and deterministic.
  """
  d_dt, d_ml, d_p, d_s = desired
  a_dt, a_ml, a_p, a_s = actual

  d_dt = (d_dt or "").upper()
  a_dt = (a_dt or "").upper()

  if not d_dt or not a_dt:
    return False

  if d_dt != a_dt:
    return False

  # Ignore unspecified vs specified length for STRING.
  if d_dt == STRING:
    if d_ml is None or a_ml is None:
      return True
    return int(d_ml) == int(a_ml)

  # DECIMAL precision/scale: treat missing precision as unknown -> not equivalent.
  if d_dt == DECIMAL:
    if d_p is None or d_s is None or a_p is None or a_s is None:
      return False
    return int(d_p) == int(a_p) and int(d_s) == int(a_s)

  # Everything else: same canonical type is equivalent.
  return True


def classify_type_drift(
  *,
  desired: CanonicalTypeTuple,
  actual: CanonicalTypeTuple,
) -> tuple[TypeDriftKind, str]:
  """
  Classify drift between (desired metadata type) and (actual physical type).

  Returns (kind, reason), where kind is one of:
    - equivalent: no real drift
    - widening: desired is wider than actual (safe evolution likely possible)
    - narrowing: desired is narrower than actual (risk of overflow/truncation)
    - incompatible: different type families (cast/rebuild likely required)
    - unknown: insufficient info
  """
  d_dt, d_ml, d_p, d_s = desired
  a_dt, a_ml, a_p, a_s = actual

  d_dt = (d_dt or "").upper()
  a_dt = (a_dt or "").upper()

  if not d_dt or not a_dt:
    return ("unknown", "missing datatype")

  # Exact equivalence + tolerated equivalence rules
  if semantically_equivalent(desired, actual):
    return ("equivalent", "semantically equivalent")

  # Same family: evaluate parameters
  if d_dt == a_dt:
    if d_dt == STRING:
      # If either length is unknown, we cannot safely decide narrowing/widening.
      if d_ml is None or a_ml is None:
        return ("unknown", "string length unknown")
      if int(a_ml) < int(d_ml):
        return ("widening", f"string length larger: actual={a_ml} < desired={d_ml}")
      if int(a_ml) > int(d_ml):
        return ("narrowing", f"string length smaller: actual={a_ml} > desired={d_ml}")
      
      return ("equivalent", "same string length")

    if d_dt == DECIMAL:
      # BigQuery and some engines do not reliably expose precision/scale.
      # If both sides are DECIMAL but parameters are missing, treat as equivalent.
      if (
        (d_p is None or d_s is None) and (a_p is None or a_s is None)
      ):
        return ("equivalent", "decimal precision/scale unknown but same type")

      # If only one side is known, we cannot safely classify drift.
      if d_p is None or d_s is None or a_p is None or a_s is None:
        return ("equivalent", "decimal precision/scale partially unknown")

      # Compare scale first (scale reduction is risky)
      if int(a_s) < int(d_s):
        return ("widening", f"decimal scale larger: actual={a_s} < desired={d_s}")
      if int(a_s) > int(d_s):
        return ("narrowing", f"decimal scale smaller: actual={a_s} > desired={d_s}")

      # Same scale: compare precision
      if int(a_p) < int(d_p):
        return ("widening", f"decimal precision larger: actual={a_p} < desired={d_p}")
      if int(a_p) > int(d_p):
        return ("narrowing", f"decimal precision smaller: actual={a_p} > desired={d_p}")

      return ("equivalent", "same decimal precision/scale")

    # For all other same-family types (INT, BIGINT, BOOLEAN, DATE, TIMESTAMP, ...),
    # treat same datatype as equivalent (parameters already handled above where applicable).
    return ("equivalent", "same datatype")

  # Cross-family widening/narrowing that is reasonably ordered
  # Note: we classify from "actual physical" -> "desired metadata"
  if (a_dt, d_dt) == (INTEGER, BIGINT):
    return ("widening", "integer -> bigint")
  if (a_dt, d_dt) == (BIGINT, INTEGER):
    return ("narrowing", "bigint -> integer")

  # DATE vs TIMESTAMP: direction matters (classify from actual -> desired).
  # DATE -> TIMESTAMP is widening (safe-ish; time is set deterministically, usually midnight).
  if (a_dt, d_dt) == (DATE, TIMESTAMP):
    return ("widening", "date -> timestamp")
  # TIMESTAMP -> DATE is narrowing (lossy; time component is dropped).
  if (a_dt, d_dt) == (TIMESTAMP, DATE):
    return ("narrowing", "timestamp -> date (lossy)")  

  # FLOAT vs DECIMAL: direction matters (classify from actual -> desired)
  # DECIMAL -> FLOAT is usually possible but may lose precision (lossy).
  if (a_dt, d_dt) == (DECIMAL, FLOAT):
    return ("widening", "decimal -> float (lossy)")
  # FLOAT -> DECIMAL requires scale/precision decisions and can change semantics.
  if (a_dt, d_dt) == (FLOAT, DECIMAL):
    return ("incompatible", "float -> decimal (precision semantics)")  

  # Default: incompatible families
  return ("incompatible", f"type mismatch: desired={d_dt} actual={a_dt}")
