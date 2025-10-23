"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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
from typing import Optional, Tuple
from urllib.parse import quote_plus, urlparse

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
  username: Optional[str] = None
  password: Optional[str] = None
  # driver for ODBC-style (MSSQL)
  driver: Optional[str] = None            # e.g. "ODBC Driver 18 for SQL Server"
  # extra query/kv-string
  extra: Optional[str] = None             # e.g. "Encrypt=yes;TrustServerCertificate=no"
  # raw alternatives
  url: Optional[str] = None               # full SQLAlchemy URL (if provided)
  odbc_connect: Optional[str] = None      # ODBC connect string (Driver=...;Server=...;...)

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
      return DbSecret(
        dialect=data.get("dialect"),
        host=data.get("host"),
        port=int(data["port"]) if "port" in data and str(data["port"]).isdigit() else None,
        database=data.get("database"),
        username=data.get("username"),
        password=data.get("password"),
        driver=data.get("driver"),
        extra=data.get("extra"),
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
  if sec.extra:
    # If extra looks like query string, append with '?'; otherwise append as-is
    sep = "&" if "?" in url else "?"
    url = f"{url}{sep}{sec.extra}"
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
    extra = sec.extra or ""
    if extra and not extra.strip().endswith(";"):
      extra = extra.strip() + ";"
    odbc = (
      f"Driver={{{sec.driver}}};"
      f"Server={sec.host};"
      f"Database={sec.database};"
      f"Uid={sec.username};"
      f"Pwd={sec.password};"
      f"{extra}"
    )

  return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}"


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


def engine_for_target(*, target_short_name: str, template: Optional[str] = None) -> Engine:
  """
  Build engine for the *target* platform.
  By default uses a conventional template: sec/{profile}/conn/{type}/{short_name}
  You can override the template if your profile defines something custom.
  """
  tpl = template or "sec/{profile}/conn/{type}/{short_name}"
  ref = build_secret_ref(
    profiles_path=settings.ELEVATA_PROFILES_PATH,
    template=tpl,
    short_name=target_short_name,
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
