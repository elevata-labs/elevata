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

import json
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.parse import parse_qsl, quote_plus, urlencode, urlparse

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from django.conf import settings

from .ref_builder import build_secret_ref
from metadata.secrets.resolver import resolve_profile_secret_value


# -----------------------------------------------------------------------------
# Dataclass representing a DB secret in normalized form
# -----------------------------------------------------------------------------
@dataclass
class DbSecret:
  # minimal common fields
  dialect: Optional[str] = None           # e.g. "postgresql+psycopg2", "mssql+pyodbc"
  host: Optional[str] = None
  port: Optional[int] = None
  database: Optional[str] = None
  schema: Optional[str] = None
  username: Optional[str] = None
  password: Optional[str] = None
  # driver for ODBC-style (MSSQL)
  driver: Optional[str] = None            # e.g. "ODBC Driver 18 for SQL Server"
  # extra normalized
  extra_kv: Optional[Dict[str, str]] = None     # e.g. {"warehouse":"..."} or {"http_path":"..."}
  extra_raw: Optional[str] = None               # legacy raw string (ODBC extras etc.)
  # raw alternatives
  url: Optional[str] = None               # full SQLAlchemy URL (if provided)
  odbc_connect: Optional[str] = None      # ODBC connect string (Driver=...;Server=...;...)


  @staticmethod
  def _normalize_extra(extra) -> tuple[Optional[Dict[str, str]], Optional[str]]:
    """
    Normalize `extra` to a dict when possible.
    - dict stays dict
    - JSON object string -> dict
    - querystring-like "a=b&c=d" (or leading '?') -> dict
    - otherwise keep as raw string (for ODBC-style extras like "Encrypt=yes;...").
    """
    if extra is None:
      return None, None

    if isinstance(extra, dict):
      # stringify values
      return {str(k): str(v) for k, v in extra.items()}, None

    if isinstance(extra, str):
      s = extra.strip()
      if not s:
        return None, None
      if s.startswith("{"):
        try:
          obj = json.loads(s)
          if isinstance(obj, dict):
            return {str(k): str(v) for k, v in obj.items()}, None
        except Exception:
          # treat as raw
          return None, s
      # query string?
      qs = s[1:] if s.startswith("?") else s
      if "=" in qs and "&" in qs or "=" in qs:
        try:
          pairs = parse_qsl(qs, keep_blank_values=True)
          if pairs:
            return {str(k): str(v) for k, v in pairs}, None
        except Exception:
          pass
      return None, s

    # unknown type -> store as raw string
    return None, str(extra)


  @staticmethod
  def from_value(value: str) -> "DbSecret":
    """
    Accepts:
      - SQLAlchemy URL (has '://')
      - ODBC connect string (contains 'Driver=' ... ';')
      - JSON object with fields listed above
    """
    val = (value or "").strip()

    # SQLAlchemy URL?
    if "://" in val and not val.strip().startswith("{"):
      return DbSecret(url=val)

    # JSON object?
    if val.startswith("{"):
      data = json.loads(val)
      extra_kv, extra_raw = DbSecret._normalize_extra(data.get("extra"))
      return DbSecret(
        dialect=data.get("dialect"),
        host=data.get("host"),
        port=int(data["port"]) if "port" in data and str(data["port"]).isdigit() else None,
        database=data.get("database"),
        schema=data.get("schema"),
        username=data.get("username"),
        password=data.get("password"),
        driver=data.get("driver"),
        extra_kv=extra_kv,
        extra_raw=extra_raw,
        url=data.get("url"),
        odbc_connect=data.get("odbc_connect"),
      )

    # ODBC connect string (very loose detection: contains 'Driver=' and ';')
    if "driver=" in val.lower() and ";" in val:
      return DbSecret(dialect="mssql+pyodbc", odbc_connect=val)

    # If nothing matched, assume plain URL as last resort
    return DbSecret(url=val)


# -----------------------------------------------------------------------------
# URL builders
# -----------------------------------------------------------------------------
def _build_sqlalchemy_url_from_parts(sec: DbSecret) -> str:
  """
  Build a SQLAlchemy URL (without ODBC) from structured fields.
  Supported: postgresql, mysql, snowflake-sqlalchemy (if present), etc.
  """
  if not sec.dialect:
    raise ValueError("Missing 'dialect' in DB secret (cannot build URL).")
  if sec.dialect.startswith("mssql+pyodbc"):
    # handled by dedicated ODBC builder
    raise ValueError("Use _build_mssql_odbc_url for pyodbc dialects.")
  if not (sec.username and sec.password and sec.host and sec.database):
    raise ValueError("Missing fields to build URL (username/password/host/database required).")

  # optional port
  netloc = f"{sec.username}:{quote_plus(sec.password)}@{sec.host}"
  if sec.port:
    netloc = f"{sec.username}:{quote_plus(sec.password)}@{sec.host}:{sec.port}"

  url = f"{sec.dialect}://{netloc}/{sec.database}"

  if sec.extra_kv:
    sep = "&" if "?" in url else "?"
    url = f"{url}{sep}{urlencode(sec.extra_kv)}"
  elif sec.extra_raw:
    # legacy: allow passing raw query string
    sep = "&" if "?" in url else "?"
    url = f"{url}{sep}{sec.extra_raw.lstrip('?')}"

  return url


def _build_mssql_odbc_url(sec: DbSecret) -> str:
  """
  Build a mssql+pyodbc URL using an ODBC connect string.
  - Accepts either sec.odbc_connect (full "Driver=...;Server=...;...") OR
    builds one from fields (driver, host, database, username, password, extra).
  """
  if not sec.dialect or not sec.dialect.startswith("mssql+pyodbc"):
    raise ValueError("ODBC URL requested but dialect is not 'mssql+pyodbc'.")

  if sec.odbc_connect:
    odbc = sec.odbc_connect
  else:
    if not (sec.driver and sec.host and sec.database and sec.username and sec.password):
      raise ValueError("Missing fields to build ODBC connect string for MSSQL.")
    # Prefer legacy raw extras for ODBC semantics; otherwise convert kv -> "k=v;" pairs.
    extra = sec.extra_raw or ""
    if not extra and sec.extra_kv:
      extra = ";".join([f"{k}={v}" for k, v in sec.extra_kv.items()])
    if extra:
      extra = extra.strip()
      if not extra.endswith(";"):
        extra = extra + ";"

    odbc = (
      f"Driver={{{sec.driver}}};"
      f"Server={sec.host};"
      f"Database={sec.database};"
      f"Uid={sec.username};"
      f"Pwd={sec.password};"
      f"{extra}"
    )

  return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}"


def _build_databricks_url(sec: DbSecret) -> str:
  """
  Build a SQLAlchemy URL for Databricks from generic secret fields:
    host      -> server_hostname
    database  -> catalog
    password  -> access_token
    extra_kv  -> must contain http_path
    schema    -> optional query param
  """
  if not sec.dialect:
    raise ValueError("Missing 'dialect' in DB secret (cannot build URL).")

  http_path = (sec.extra_kv or {}).get("http_path")
  if not (sec.host and sec.password and http_path):
    raise ValueError(
      "Databricks secret requires host, password (token), and extra.http_path."
    )

  catalog = sec.database or "default"
  token = quote_plus(sec.password)
  http_path_q = quote_plus(str(http_path))
  schema_q = f"&schema={quote_plus(sec.schema)}" if sec.schema else ""

  return (
    f"{sec.dialect}://token:{token}@{sec.host}:443/{catalog}"
    f"?http_path={http_path_q}{schema_q}"
  )


def _coerce_secret_to_url(sec: DbSecret) -> str:
  """
  Return a SQLAlchemy URL from DbSecret in any accepted form.
  """
  # Full URL already provided?
  if sec.url:
    return sec.url

  # pyodbc MSSQL?
  if sec.dialect and sec.dialect.startswith("mssql+pyodbc"):
    return _build_mssql_odbc_url(sec)
  
  # Databricks (needed for SQLAlchemy-based introspection/materialization)
  if sec.dialect and sec.dialect.startswith("databricks"):
    return _build_databricks_url(sec)
  
  # structured parts?
  return _build_sqlalchemy_url_from_parts(sec)


# -----------------------------------------------------------------------------
# Engine factories
# -----------------------------------------------------------------------------
def _resolve_db_secret(ref: str) -> DbSecret:
  """
  Resolve a secret reference against the active profile and return normalized DbSecret.
  """
  raw_value = resolve_profile_secret_value(
    profiles_path=settings.ELEVATA_PROFILES_PATH,
    ref=ref,
  )
  return DbSecret.from_value(raw_value)


def resolve_secret_ref(ref: str):
  """
  Resolve a secret reference against the active profile.
  Returns the raw secret value (string or dict).
  """
  return resolve_profile_secret_value(
    profiles_path=settings.ELEVATA_PROFILES_PATH,
    ref=ref,
  )


def rest_config_for_source_system(*, system_type: str, short_name: str) -> dict:
  """
  Resolve REST connector config for a source system.
  Secret convention: sec/{profile}/conn/{type}/{short_name}

  Recommended secret shape:
    {
      "base_url": "https://api.example.com",
      "headers": {"Authorization": "Bearer ..."},
      "query": {"fixed_param": "x"}
    }

  Alternative: plain string base_url.
  """
  ref = build_secret_ref(
    profiles_path=settings.ELEVATA_PROFILES_PATH,
    type=system_type.lower(),
    short_name=short_name,
  )
  sec = resolve_secret_ref(ref)
  if isinstance(sec, str):
    s = sec.strip()
    # Support JSON object secrets stored as strings (e.g. from .env providers).
    if s.startswith("{"):
      try:
        obj = json.loads(s)
        if isinstance(obj, dict):
          return {
            "base_url": str(obj.get("base_url") or "").strip(),
            "headers": dict(obj.get("headers") or {}),
            "query": dict(obj.get("query") or {}),
          }
      except Exception:
        pass
    # Fallback: treat string as plain base_url.
    return {"base_url": s, "headers": {}, "query": {}}

  if isinstance(sec, dict):
    return {
      "base_url": str(sec.get("base_url") or "").strip(),
      "headers": dict(sec.get("headers") or {}),
      "query": dict(sec.get("query") or {}),
    }
  raise ValueError(f"Unsupported REST secret format for ref={ref}")


def engine_from_secret_ref(ref: str) -> Engine:
  """
  Generic entrypoint: provide a final secret *reference*, get a SQLAlchemy engine back.
  """
  sec = _resolve_db_secret(ref)
  url = _coerce_secret_to_url(sec)
  return create_engine(url, future=True, pool_pre_ping=True)


def engine_for_source_system(*, system_type: str, short_name: str) -> Engine:
  """
  Build engine for a *source* system.
  Secret reference is constructed via profile's secret_ref_template or explicit template.
  Convention (default): sec/{profile}/conn/{type}/{short_name}
  """
  ref = build_secret_ref(
    profiles_path=settings.ELEVATA_PROFILES_PATH,
    type=system_type.lower(),
    short_name=short_name,
  )
  return engine_from_secret_ref(ref)


def engine_for_target(*, target_short_name: str, system_type: Optional[str] = None, template: Optional[str] = None) -> Engine:

  """
  Build engine for the *target* platform.
  By default uses a conventional template: sec/{profile}/conn/{type}/{short_name}
  You can override the template if your profile defines something custom.
  """
  tpl = template or "sec/{profile}/conn/{type}/{short_name}"
  kwargs = {"short_name": target_short_name}
  # Only require/emit {type} if the template actually uses it.
  if "{type}" in tpl:
    if not system_type:
      raise ValueError(
        f"engine_for_target requires system_type because template contains '{{type}}': {tpl}"
      )
    kwargs["type"] = system_type.lower()

  ref = build_secret_ref(
    profiles_path=settings.ELEVATA_PROFILES_PATH,
    template=tpl,
    **kwargs,
  )

  return engine_from_secret_ref(ref)

# -----------------------------------------------------------------------------
# Small helpers (optional)
# -----------------------------------------------------------------------------
def is_url_like(value: str) -> bool:
  """
  Quick heuristic to detect URL-shaped secrets.
  """
  try:
    u = urlparse(value)
    return bool(u.scheme and (u.hostname or u.path))
  except Exception:
    return False

# -----------------------------------------------------------------------------
# Ingestion dispatcher (source -> RAW)
# -----------------------------------------------------------------------------
def ingest_raw_for_source_dataset(
  *,
  source_dataset,
  td,
  target_system,
  dialect,
  profile,
  batch_run_id: str,
  load_run_id: str,
  meta_schema: str = "meta",
  **kwargs,
):
  """
  Dispatch RAW ingestion based on source system type.
  Keeps routing logic in one place.
  """
  # IMPORTANT:
  # Keep imports local to avoid circular deps (native_raw imports engine_for_target).
  from metadata.ingestion.rest import ingest_raw_rest
  from metadata.ingestion.native_raw import ingest_raw_file, ingest_raw_relational

  sys = getattr(source_dataset, "source_system", None)
  if not sys:
    raise ValueError("source_dataset.source_system is required")

  st = str(getattr(sys, "type", "") or "").strip().lower()

  if st == "rest":
    return ingest_raw_rest(
      source_dataset=source_dataset,
      td=td,
      target_system=target_system,
      dialect=dialect,
      profile=profile,
      batch_run_id=batch_run_id,
      load_run_id=load_run_id,
      meta_schema=meta_schema,
      **kwargs,
    )

  # File-like sources: source_system.type is the file type (csv/json/jsonl/...)
  # (matches auto-import behavior in file_import.py).
  # File-like sources: source_system.type is the file type (csv/json/jsonl/...)
  if st in ("file", "csv", "json", "jsonl", "ndjson", "parquet", "excel"):
    return ingest_raw_file(
      source_dataset=source_dataset,
      td=td,
      target_system=target_system,
      dialect=dialect,
      profile=profile,
      batch_run_id=batch_run_id,
      load_run_id=load_run_id,
      meta_schema=meta_schema,
      file_type=st,
      **kwargs,
    )

  # Fallback: treat everything else as relational/native source (db connectors).
  return ingest_raw_relational(
    source_dataset=source_dataset,
    td=td,
    target_system=target_system,
    dialect=dialect,
    profile=profile,
    batch_run_id=batch_run_id,
    load_run_id=load_run_id,
    **kwargs,
  )
