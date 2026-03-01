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

"""Generate reserved keyword modules for SQL dialects.

This Django management command is a maintainer tool.
It produces deterministic keyword modules at:
  core/metadata/rendering/dialects/keywords/<dialect>.py

Dialect sources:
  - Engine-truth (preferred): query the engine for reserved keywords.
  - Doc-truth (fallback): scrape vendor documentation pages.
"""

import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Iterable

from django.core.management.base import BaseCommand, CommandError

from metadata.config.targets import get_target_system
from metadata.rendering.dialects.dialect_factory import (
  get_available_dialect_names,
  get_active_dialect,
)


# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------

_HEX_RE = re.compile(r"^[A-F0-9]{10,}$")

CORE_SQL_KEYWORDS: set[str] = {
  "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "HAVING",
  "JOIN", "ON", "AS", "DISTINCT", "LIMIT",
  "INSERT", "UPDATE", "DELETE",
  "CREATE", "ALTER", "DROP",
  "MERGE",
}

# Fail hard if we clearly parsed the wrong table / fell back to noise.
DOC_TRUTH_MIN_COUNTS: dict[str, int] = {
  "mssql": 120,
  "fabric_warehouse": 120,
  "postgres": 50,
  "snowflake": 80,
  "bigquery": 20,
}

DOC_TRUTH_MAX_COUNTS: dict[str, int] = {
  # Guard against "whole-page tokenization" contamination.
  # These numbers are intentionally generous, but they will catch 978/1466 style failures.
  "postgres": 250,
  "snowflake": 600,
  "mssql": 400,
  "fabric_warehouse": 400,
  "bigquery": 250,
}

# Dialect-specific "tripwire" tokens/substrings that must never appear in RESERVED lists.
# If they appear, it usually means we accidentally scraped non-keyword page content.
DOC_TRUTH_FORBIDDEN: dict[str, list[str]] = {
  "postgres": ["ABSOLUTELY", "BACKGROUND", "ABOUT", "AWAY"],
  "snowflake": ["CORTEX_", "_HISTORY", "_USAGE", "_METERING", "ACCOUNT_USAGE"],
}

SANITY_REQUIRED_ANY: set[str] = {"SELECT", "FROM"}
SANITY_MIN_KEYWORD_COUNT = 10

ENGINE_TRUTH_DIALECTS: set[str] = {
  "databricks",
  "duckdb",
}

DOC_TRUTH_DIALECTS: set[str] = {
  "bigquery",
  "postgres",
  "mssql",
  "snowflake",
}

REUSE_DIALECTS: dict[str, str] = {
  "fabric_warehouse": "mssql",
}


# --------------------------------------------------------------------------------------
# Token validation / normalization
# --------------------------------------------------------------------------------------

def _is_valid_keyword(token: str) -> bool:
  """
  Validate that a token is a plausible SQL reserved keyword.

  Filters out:
    - tracking IDs / hex blobs
    - tokens containing digits
    - feature flags (__)
    - weird leading/trailing underscores
    - excessively long tokens (doc noise)
  """
  if not token:
    return False

  token = token.strip()
  if not token:
    return False

  # Reserved keywords are not single-letter identifiers.
  if len(token) < 2:
    return False

  # Convention: store as uppercase tokens.
  if token != token.upper():
    return False

  if _HEX_RE.match(token):
    return False

  if any(c.isdigit() for c in token):
    return False

  if "__" in token:
    return False

  if token.startswith("_") or token.endswith("_"):
    return False

  if len(token) > 40:
    return False

  if not any(c.isalpha() for c in token):
    return False

  return True


def _normalize_keywords(values: Iterable[Any]) -> set[str]:
  """
  Normalize raw keyword tokens to a set of UPPERCASE strings.
  Applies _is_valid_keyword filtering.
  """
  out: set[str] = set()
  for v in values:
    s = str(v or "").strip().upper()
    if not s:
      continue
    if _is_valid_keyword(s):
      out.add(s)
  return out


def _extract_identifier_tokens(text: str) -> set[str]:
  """
  Extract identifier-like tokens from arbitrary text (fallback only).
  """
  raw = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", text or "")
  return _normalize_keywords([t.upper() for t in raw])


# --------------------------------------------------------------------------------------
# HTML helpers (dependency-free)
# --------------------------------------------------------------------------------------

class _TextCollector(HTMLParser):
  """Collect visible text from an HTML document."""
  def __init__(self) -> None:
    super().__init__()
    self._chunks: list[str] = []

  def handle_data(self, data: str) -> None:
    if data:
      self._chunks.append(data)

  def text(self) -> str:
    return "\n".join(self._chunks)


class _HtmlTableExtractor(HTMLParser):
  """Extract tables (rows/cells) from HTML and track nearby headings."""
  def __init__(self) -> None:
    super().__init__()
    self._in_heading = False
    self._heading_buf: list[str] = []
    self.current_heading: str | None = None

    self._in_table = False
    self._in_tr = False
    self._in_cell = False
    self._cell_buf: list[str] = []

    self.tables: list[dict[str, Any]] = []
    self._current_rows: list[list[str]] = []
    self._current_row: list[str] = []

  def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
    t = tag.lower()

    if t in {"h1", "h2", "h3", "h4", "h5", "h6"}:
      self._in_heading = True
      self._heading_buf = []
      return

    if t == "table":
      self._in_table = True
      self._current_rows = []
      return

    if self._in_table and t == "tr":
      self._in_tr = True
      self._current_row = []
      return

    if self._in_tr and t in {"td", "th"}:
      self._in_cell = True
      self._cell_buf = []
      return

  def handle_endtag(self, tag: str) -> None:
    t = tag.lower()

    if t in {"h1", "h2", "h3", "h4", "h5", "h6"} and self._in_heading:
      self._in_heading = False
      text = _normalize_whitespace("".join(self._heading_buf))
      if text:
        self.current_heading = text
      return

    if t in {"td", "th"} and self._in_cell:
      self._in_cell = False
      cell = _normalize_whitespace("".join(self._cell_buf))
      self._current_row.append(cell)
      return

    if t == "tr" and self._in_tr:
      self._in_tr = False
      if self._current_row:
        self._current_rows.append(self._current_row)
      self._current_row = []
      return

    if t == "table" and self._in_table:
      self._in_table = False
      if self._current_rows:
        self.tables.append({
          "heading": self.current_heading,
          "rows": self._current_rows,
        })
      self._current_rows = []
      return

  def handle_data(self, data: str) -> None:
    if not data:
      return
    if self._in_heading:
      self._heading_buf.append(data)
      return
    if self._in_cell:
      self._cell_buf.append(data)
      return


def _normalize_whitespace(s: str) -> str:
  return re.sub(r"\s+", " ", (s or "").strip())


def _http_get(url: str) -> str:
  req = urllib.request.Request(
    url,
    headers={"User-Agent": "elevata-keyword-generator/1.0"},
  )
  with urllib.request.urlopen(req, timeout=30) as resp:
    raw = resp.read()
  return raw.decode("utf-8", errors="replace")


def _select_best_keyword_table(
  *,
  html: str,
  heading_hint_contains: str | None,
  require_header_contains: list[str],
) -> dict[str, Any] | None:
  """
  Pick the most plausible keyword table from a page.
  """
  p = _HtmlTableExtractor()
  p.feed(html)

  best: tuple[int, dict[str, Any]] | None = None

  for t in p.tables:
    rows: list[list[str]] = t.get("rows") or []
    if not rows:
      continue

    header = [c.lower() for c in (rows[0] or [])]
    header_join = " ".join(header)

    if any(req not in header_join for req in (require_header_contains or [])):
      continue

    score = 0
    heading = (t.get("heading") or "").lower()
    if heading_hint_contains and heading_hint_contains.lower() in heading:
      score += 50

    score += min(len(header), 6) * 3
    score += min(len(rows), 300)

    if best is None or score > best[0]:
      best = (score, t)

  return best[1] if best else None


def _extract_keywords_from_table(
  *,
  table: dict[str, Any],
  keyword_column_candidates: list[str],
  reserved_flag_column_candidates: list[str] | None,
) -> set[str]:
  """
  Extract keyword tokens from a parsed HTML table.
  """
  rows: list[list[str]] = table.get("rows") or []
  if len(rows) < 2:
    return set()

  header = [c.strip() for c in rows[0]]
  header_l = [c.lower() for c in header]

  def _find_col(candidates: list[str]) -> int | None:
    for cand in candidates:
      cand_l = cand.lower()
      for i, h in enumerate(header_l):
        if cand_l == h:
          return i
      for i, h in enumerate(header_l):
        if cand_l in h:
          return i
    return None

  kw_col = _find_col(keyword_column_candidates)
  if kw_col is None:
    kw_col = 0

  reserved_col: int | None = None
  if reserved_flag_column_candidates:
    reserved_col = _find_col(reserved_flag_column_candidates)

  tokens: list[str] = []

  for r in rows[1:]:
    if not r or kw_col >= len(r):
      continue

    if reserved_col is not None:
      if reserved_col >= len(r):
        continue
      flag = (r[reserved_col] or "").strip().lower()
      if flag not in {"y", "yes", "true", "reserved"}:
        continue

    cell = r[kw_col] or ""
    parts = re.findall(r"\b[A-Z][A-Z_]*\b", cell.upper())
    tokens.extend(parts)

  return _normalize_keywords(tokens)


def _extract_keywords_from_all_tables(
  *,
  html: str,
  header_must_contain_any: list[str],
  keyword_column_candidates: list[str],
) -> set[str]:
  """
  Extract keyword tokens by unioning results across all matching tables on a page.

  Useful for docs that split keywords across multiple tables (e.g. A/B/C sections).
  """
  p = _HtmlTableExtractor()
  p.feed(html)

  needles = [n.lower() for n in (header_must_contain_any or [])]
  out: set[str] = set()

  for t in p.tables:
    rows = t.get("rows") or []
    if len(rows) < 2:
      continue
    header = [c.strip().lower() for c in (rows[0] or [])]
    header_join = " ".join(header)
    if not any(n in header_join for n in needles):
      continue

    out |= _extract_keywords_from_table(
      table=t,
      keyword_column_candidates=keyword_column_candidates,
      reserved_flag_column_candidates=None,
    )

  return out


def _normalize_header_token(s: str) -> str:
  """
  Normalize header cell text for robust matching.
  Examples:
    "Key Word"   -> "keyword"
    "Reserved ?" -> "reserved"
    "Key-word"   -> "keyword"
  """
  s2 = (s or "").strip().lower()
  s2 = re.sub(r"[^a-z0-9]+", "", s2)
  return s2


def _doc_truth_size_guard(dialect: str, keywords: set[str]) -> None:
  min_expected = DOC_TRUTH_MIN_COUNTS.get(dialect)
  if min_expected is not None and len(keywords) < min_expected:
    raise CommandError(
      f"{dialect}: doc-truth extraction too small ({len(keywords)} < {min_expected}). "
      "Likely parsed the wrong table or a site layout changed."
    )


def _doc_truth_plausibility_guard(dialect: str, keywords: set[str]) -> None:
  """
  Fail hard if a doc-truth extraction is implausibly large or contains known doc-noise tripwires.

  This is a maintainer safety net to avoid publishing contaminated keyword lists.
  """
  max_expected = DOC_TRUTH_MAX_COUNTS.get(dialect)
  if max_expected is not None and len(keywords) > max_expected:
    raise CommandError(
      f"{dialect}: doc-truth extraction too large ({len(keywords)} > {max_expected}). "
      "Likely scraped non-keyword page content."
    )

  forbidden = DOC_TRUTH_FORBIDDEN.get(dialect) or []
  if forbidden:
    upper = set(keywords)
    for needle in forbidden:
      # Treat needles with trailing/leading underscores as substring rules.
      if needle.endswith("_") or needle.startswith("_"):
        hit = next((k for k in upper if needle in k), None)
        if hit:
          raise CommandError(
            f"{dialect}: doc-truth contamination detected (found '{hit}' matching '{needle}')."
          )
      else:
        if needle in upper:
          raise CommandError(
            f"{dialect}: doc-truth contamination detected (found forbidden token '{needle}')."
          )


def _sanity_check_keywords(dialect: str, keywords: set[str]) -> None:
  """
  Sanity checks for generated keyword sets.

  IMPORTANT:
    - Engine-truth providers may return only *reserved* keywords; those can legitimately
      exclude SELECT/FROM depending on the engine's classification.
    - Doc-truth scraping must be stricter to detect page-noise contamination.
  """
  if dialect in DOC_TRUTH_DIALECTS:
    # Reserved-only lists can legitimately exclude some core tokens depending on the engine's
    # classification (e.g., PostgreSQL may mark FROM as non-reserved).
    if not (keywords & SANITY_REQUIRED_ANY):
      raise CommandError(
        f"{dialect}: sanity check failed. Expected at least one of: "
        f"{', '.join(sorted(SANITY_REQUIRED_ANY))}"
      )

  if len(keywords) < SANITY_MIN_KEYWORD_COUNT:
    raise CommandError(
      f"{dialect}: sanity check failed. Only {len(keywords)} keywords found "
      f"(minimum expected: {SANITY_MIN_KEYWORD_COUNT})."
    )


# --------------------------------------------------------------------------------------
# Module renderer
# --------------------------------------------------------------------------------------

def _render_keywords_module(*, dialect: str, keywords: set[str], source: str) -> str:
  """
  Render a deterministic Python module defining RESERVED_KEYWORDS.
  """
  ordered = sorted(set(keywords))
  lines: list[str] = []
  lines.append('"""')

  current_year = datetime.utcnow().year
  lines.append("elevata – Metadata-driven Data Platform Framework")
  lines.append(f"Copyright © 2025-{current_year} Ilona Tag")
  lines.append("SPDX-License-Identifier: AGPL-3.0-only")
  lines.append("")

  lines.append(f"{dialect} reserved keywords for elevata.")
  lines.append("")
  lines.append(f"Source: {source}")
  lines.append("")
  lines.append("Notes:")
  lines.append("  - Stored as UPPERCASE tokens.")
  lines.append("  - Generated by elevata_generate_reserved_keywords; do not edit by hand.")
  lines.append('"""')
  lines.append("")

  lines.append("from __future__ import annotations")
  lines.append("")
  lines.append("RESERVED_KEYWORDS: frozenset[str] = frozenset({")
  for k in ordered:
    lines.append(f'  "{k}",')
  lines.append("})")
  lines.append("")
  return "\n".join(lines)


# --------------------------------------------------------------------------------------
# Provider model
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class ProviderResult:
  dialect: str
  keywords: set[str]
  source: str


ProviderFn = Callable[[dict[str, str]], ProviderResult]


def _get_system_by_short_name(short_name: str):
  """
  Resolve a System via the official elevata target resolver.
  Ensures environment/profile-based security hydration.
  """
  try:
    return get_target_system(short_name)
  except RuntimeError as exc:
    raise CommandError(str(exc))


# --------------------------------------------------------------------------------------
# Providers
# --------------------------------------------------------------------------------------

def _engine_truth_databricks(systems: dict[str, str]) -> ProviderResult:
  """
  Engine-truth reserved keywords for Databricks using sql_keywords().
  """
  dialect = get_active_dialect("databricks")
  short_name = systems["databricks"]
  system = _get_system_by_short_name(short_name)
  exec_engine = dialect.get_execution_engine(system)

  fetch_all = getattr(exec_engine, "fetch_all", None)
  if not callable(fetch_all):
    raise RuntimeError("Databricks execution engine must implement fetch_all(sql: str).")

  sql_fn = getattr(dialect, "render_reserved_keywords_query", None)
  if callable(sql_fn):
    sql = sql_fn()
  else:
    sql = dialect.render_reserved_keywords_query()

  rows = fetch_all(sql) or []
  kw = _normalize_keywords([r[0] for r in rows if r])

  return ProviderResult(
    dialect="databricks",
    keywords=kw,
    source="Databricks engine-truth via sql_keywords()",
  )


def _engine_truth_duckdb(systems: dict[str, str]) -> ProviderResult:
  """
  Engine-truth reserved keywords for DuckDB via duckdb_keywords().
  """
  dialect = get_active_dialect("duckdb")
  short_name = systems["duckdb"]
  system = _get_system_by_short_name(short_name)
  exec_engine = dialect.get_execution_engine(system)

  fetch_all = getattr(exec_engine, "fetch_all", None)
  if not callable(fetch_all):
    raise RuntimeError("DuckDB execution engine must implement fetch_all(sql: str).")

  sql_fn = getattr(dialect, "render_reserved_keywords_query", None)
  if callable(sql_fn):
    sql = sql_fn()
  else:
    sql = dialect.render_reserved_keywords_query()

  rows = fetch_all(sql) or []
  kw = _normalize_keywords([r[0] for r in rows if r])

  return ProviderResult(
    dialect="duckdb",
    keywords=kw,
    source="DuckDB engine-truth via duckdb_keywords() WHERE keyword_category = 'reserved'",
  )


def _doc_truth_bigquery(_systems: dict[str, str]) -> ProviderResult:
  """
  Doc-truth reserved keywords for BigQuery GoogleSQL from Lexical Structure docs.
  """
  url = "https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/lexical"
  html = _http_get(url)
  parser = _TextCollector()
  parser.feed(html)

  text = parser.text()
  marker = "GoogleSQL has the following reserved keywords."
  start = text.find(marker)

  if start == -1:
    tokens = _extract_identifier_tokens(text)
    _doc_truth_size_guard("bigquery", tokens)
    _sanity_check_keywords("bigquery", tokens)
    return ProviderResult(dialect="bigquery", keywords=tokens, source=url)

  section = text[start + len(marker):]
  end_markers = [
    "## Terminating semicolons",
    "Terminating semicolons",
    "##",
  ]
  end_pos = None
  for em in end_markers:
    pos = section.find(em)
    if pos != -1:
      end_pos = pos if end_pos is None else min(end_pos, pos)
  if end_pos is not None:
    section = section[:end_pos]

  raw_tokens = re.findall(r"\b[A-Z][A-Z_]*\b", section)
  tokens = _normalize_keywords(raw_tokens)

  _doc_truth_size_guard("bigquery", tokens)
  _sanity_check_keywords("bigquery", tokens)

  return ProviderResult(dialect="bigquery", keywords=tokens, source=url)


def _find_postgres_keyword_table(html: str) -> tuple[dict[str, Any], int, int]:
  """
  Find the PostgreSQL SQL keywords table and infer the reserved-flag column.

  Returns:
    (table_dict, keyword_col_idx, pg_status_col_idx)

  Why:
    The docs page layout is stable, but header labels can vary ("Reserved", "Reserved?",
    multi-row header, etc.). Instead of strict header matching we:
      - locate the table where a header cell contains 'keyword'
      - locate the PostgreSQL classification column (values: reserved/non-reserved)
  """
  p = _HtmlTableExtractor()
  p.feed(html)

  def _candidate_headers(rows: list[list[str]]) -> list[tuple[list[str], list[list[str]]]]:
    """
    Return candidate (header, body) pairs.
    PostgreSQL docs tables can use multi-row headers; the "Key Word" row is not always row 0.
    We therefore consider the first few rows as potential headers.
    """
    if not rows:
      return []
    out: list[tuple[list[str], list[list[str]]]] = []
    max_try = min(3, len(rows) - 1)  # need at least 1 body row
    for hi in range(0, max_try + 1):
      hdr = rows[hi] or []
      if not any((c or "").strip() for c in hdr):
        continue
      body = rows[hi + 1:] if hi + 1 < len(rows) else []
      if not body:
        continue
      out.append(([c.strip() for c in hdr], body))
    return out

  best: tuple[int, dict[str, Any], int, int] | None = None

  for t in p.tables:
    rows = t.get("rows") or []
    for header, body in _candidate_headers(rows):
      header_l = [(h or "").lower() for h in header]
      header_n = [_normalize_header_token(h) for h in header]

      kw_col = None
      for i, hn in enumerate(header_n):
        # matches "keyword", "key word", "key-word", ...
        if "keyword" in hn:
          kw_col = i
          break
      if kw_col is None:
        continue

      # PostgreSQL column contains values 'reserved' / 'non-reserved' (NOT yes/no).
      pg_col = None
      for i, hn in enumerate(header_n):
        if hn == "postgresql" or "postgres" in hn:
          pg_col = i
          break

      # If header matching fails, infer the PostgreSQL column via content density.
      if pg_col is None:
        col_count = max(len(r) for r in body[:20] if r) if body else 0
        best_density = 0.0
        best_idx = None
        for ci in range(col_count):
          checked = 0
          hits = 0
          for r in body[:40]:
            if not r or ci >= len(r):
              continue
            v = (r[ci] or "").strip().lower()
            if not v:
              continue
            checked += 1
            if v in {"reserved", "non-reserved", "nonreserved"}:
              hits += 1
          if checked >= 5:
            density = hits / checked
            if density > best_density:
              best_density = density
              best_idx = ci
        if best_idx is not None and best_density >= 0.6:
          pg_col = best_idx

      if pg_col is None:
        continue

      # Score: prefer larger tables and strong reserved inference.
      score = 0
      score += min(len(body), 400)
      if any("postgres" in hn for hn in header_n):
        score += 50
      score += 30

      if best is None or score > best[0]:
        best = (score, t, kw_col, pg_col)

  if not best:
    raise CommandError(
      "postgres: could not locate SQL keyword table (no table with 'keyword' header and "
      "no inferable PostgreSQL classification column). The docs HTML structure may have changed."
    )

  _, table, kw_col, pg_col = best
  return table, kw_col, pg_col


def _doc_truth_postgres(_systems: dict[str, str]) -> ProviderResult:
  """
  Doc-truth reserved keywords for PostgreSQL from the official appendix.

  IMPORTANT:
    We must parse the keyword table and filter PostgreSQL classification = reserved.
    Falling back to whole-page tokenization produces contaminated supersets.
  """
  url = "https://www.postgresql.org/docs/current/sql-keywords-appendix.html"
  html = _http_get(url)

  table, kw_col, pg_col = _find_postgres_keyword_table(html)

  rows: list[list[str]] = table.get("rows") or []
  if len(rows) < 2:
    raise CommandError("postgres: keyword table has no data rows.")

  # Build reserved-only by checking PostgreSQL classification column (reserved/non-reserved).

  extracted: list[str] = []
  for r in rows[1:]:
    if not r:
      continue
    if kw_col >= len(r) or pg_col >= len(r):
      continue
    kw_raw = (r[kw_col] or "").strip()
    flag = (r[pg_col] or "").strip().lower()
    if not kw_raw:
      continue
    if flag != "reserved":
      continue
    extracted.append(kw_raw.upper())

  tokens = _normalize_keywords(extracted)

  if not tokens:
    raise CommandError(
      "postgres: extracted 0 reserved keywords after filtering Reserved=yes. "
      "Reserved column values may differ (expected yes/no)."
    )

  _doc_truth_size_guard("postgres", tokens)
  _doc_truth_plausibility_guard("postgres", tokens)
  _sanity_check_keywords("postgres", tokens)

  return ProviderResult(dialect="postgres", keywords=tokens, source=url)


def _doc_truth_mssql(_systems: dict[str, str]) -> ProviderResult:
  """
  Doc-truth reserved keywords for Microsoft T-SQL from Microsoft Learn.
  """
  url = (
    "https://learn.microsoft.com/en-us/sql/t-sql/language-elements/"
    "reserved-keywords-transact-sql?view=sql-server-ver17"
  )
  html = _http_get(url)

  tokens: set[str] = set()

  # Microsoft Learn frequently renders the keyword list without a plain <table>.
  # Use a robust text anchor first, then fall back to table heuristics.
  parser = _TextCollector()
  parser.feed(html)
  text = parser.text()

  start_marker = "The following table lists SQL Server and Azure Synapse Analytics reserved keywords."
  end_markers = [
    "The following table lists reserved keywords that are exclusive to Azure Synapse Analytics.",
    "## ODBC Reserved Keywords",
    "ODBC Reserved Keywords",
    "## See also",
    "See also",
  ]

  start = text.find(start_marker)
  if start != -1:
    section = text[start + len(start_marker):]
    end_pos = None
    for em in end_markers:
      pos = section.find(em)
      if pos != -1:
        end_pos = pos if end_pos is None else min(end_pos, pos)
    if end_pos is not None:
      section = section[:end_pos]

    # Extract uppercase tokens from the anchored section.
    raw_tokens = re.findall(r"\b[A-Z][A-Z_]*\b", section.upper())
    tokens = _normalize_keywords(raw_tokens)

  if not tokens:
    table = _select_best_keyword_table(
      html=html,
      heading_hint_contains="reserved keywords",
      require_header_contains=["keyword"],
    )
    if table:
      tokens = _extract_keywords_from_table(
        table=table,
        keyword_column_candidates=["Keyword", "Keywords"],
        reserved_flag_column_candidates=None,
      )

  if not tokens:
    p = _HtmlTableExtractor()
    p.feed(html)
    raw: list[str] = []
    for t in p.tables:
      for r in (t.get("rows") or []):
        for c in r:
          raw.extend(re.findall(r"\b[A-Z][A-Z_]*\b", (c or "").upper()))
    tokens = _normalize_keywords(raw)

  _doc_truth_size_guard("mssql", tokens)
  _sanity_check_keywords("mssql", tokens)

  return ProviderResult(dialect="mssql", keywords=tokens, source=url)


def _doc_truth_snowflake(_systems: dict[str, str]) -> ProviderResult:
  """
  Doc-truth reserved keywords for Snowflake from the official docs page.
  """
  url = "https://docs.snowflake.com/en/sql-reference/reserved-keywords"
  html = _http_get(url)

  # Snowflake docs can split keywords across multiple tables (A/B/C...).
  # Union all keyword tables to avoid partial extraction (e.g., only "A" section).
  tokens = _extract_keywords_from_all_tables(
    html=html,
    header_must_contain_any=["Keyword", "Schlüsselwort"],
    keyword_column_candidates=["Keyword", "Keywords", "Schlüsselwort"],
  )

  if not tokens:
    parser = _TextCollector()
    parser.feed(html)
    raw = re.findall(r"\b[A-Z][A-Z_]*\b", parser.text().upper())
    tokens = _normalize_keywords(raw)

  _doc_truth_size_guard("snowflake", tokens)
  _sanity_check_keywords("snowflake", tokens)

  return ProviderResult(dialect="snowflake", keywords=tokens, source=url)


def _reuse_provider(dialect_name: str, source_dialect: str, systems: dict[str, str]) -> ProviderResult:
  base = _run_provider(source_dialect, systems)
  return ProviderResult(
    dialect=dialect_name,
    keywords=set(base.keywords),
    source=f"Reuse of {source_dialect} keyword set (source: {base.source})",
  )


def _run_provider(dialect_name: str, systems: dict[str, str]) -> ProviderResult:
  d = dialect_name.lower()

  if d in REUSE_DIALECTS:
    return _reuse_provider(d, REUSE_DIALECTS[d], systems)

  if d == "databricks":
    return _engine_truth_databricks(systems)

  if d == "duckdb":
    return _engine_truth_duckdb(systems)

  if d == "bigquery":
    return _doc_truth_bigquery(systems)

  if d == "postgres":
    return _doc_truth_postgres(systems)

  if d == "mssql":
    return _doc_truth_mssql(systems)

  if d == "snowflake":
    return _doc_truth_snowflake(systems)

  raise RuntimeError(f"No provider implemented for dialect: {dialect_name!r}")


# --------------------------------------------------------------------------------------
# CLI helpers
# --------------------------------------------------------------------------------------

def _parse_systems_kv(items: list[str]) -> dict[str, str]:
  out: dict[str, str] = {}
  for it in items or []:
    s = (it or "").strip()
    if not s:
      continue
    if "=" not in s:
      raise CommandError(f"Invalid --systems item {s!r}. Expected key=value.")
    k, v = s.split("=", 1)
    k = k.strip().lower()
    v = v.strip()
    if not k or not v:
      raise CommandError(f"Invalid --systems item {s!r}. Expected key=value.")
    out[k] = v
  return out


def _resolve_requested_dialects(requested: list[str], available: list[str]) -> list[str]:
  req = [r.strip().lower() for r in (requested or []) if (r or "").strip()]
  if not req:
    raise CommandError("At least one --dialect is required (or use --dialect all).")

  if "all" in req:
    return sorted(available)

  unknown = sorted(set(req) - set(available))
  if unknown:
    raise CommandError(
      f"Unknown dialect(s): {', '.join(unknown)}. Available: {', '.join(sorted(available))}."
    )

  return req


def _validate_engine_truth_systems(dialects: list[str], systems: dict[str, str]) -> None:
  missing = [d for d in dialects if d in ENGINE_TRUTH_DIALECTS and d not in systems]
  if missing:
    need = " ".join([f"{d}=<short_name>" for d in sorted(missing)])
    raise CommandError(
      "Missing system mapping for engine-truth dialect(s): "
      + ", ".join(sorted(missing))
      + ".\nUse: --systems "
      + need
    )


# --------------------------------------------------------------------------------------
# Django management command
# --------------------------------------------------------------------------------------

class Command(BaseCommand):
  help = (
    "Generate reserved keyword modules for SQL dialects.\n\n"
    "Examples:\n"
    "  python manage.py elevata_generate_reserved_keywords --dialect all "
    "--systems databricks=my_dbricks duckdb=my_duck\n"
    "  python manage.py elevata_generate_reserved_keywords --dialect postgres --dialect snowflake\n"
    "  python manage.py elevata_generate_reserved_keywords --dialect databricks --system my_dbricks\n"
  )

  def add_arguments(self, parser) -> None:
    parser.add_argument(
      "--dialect",
      action="append",
      default=[],
      help="Dialect name (repeatable). Use 'all' to generate for all registered dialects.",
    )
    parser.add_argument(
      "--system",
      default=None,
      help="Shorthand for single engine-truth dialect runs (maps to that dialect).",
    )
    parser.add_argument(
      "--systems",
      nargs="+",
      default=[],
      help=(
        "Dialect system mapping key=value pairs. "
        "Example: --systems databricks=my_dbricks duckdb=my_duck"
      ),
    )
    parser.add_argument(
      "--out-root",
      default="metadata/rendering/dialects/keywords",
      help="Output folder for generated keyword modules.",
    )
    parser.add_argument(
      "--no-core-overlay",
      action="store_true",
      help="Disable CORE SQL keyword overlay safety net.",
    )

  def handle(self, *args: Any, **options: Any) -> None:
    requested: list[str] = options.get("dialect") or []
    out_root = Path(options.get("out_root") or "").resolve()
    use_overlay = not bool(options.get("no_core_overlay"))

    available = sorted(get_available_dialect_names())
    dialects = _resolve_requested_dialects(requested, available)

    systems_map = _parse_systems_kv(options.get("systems") or [])

    system_single = (options.get("system") or "").strip()
    if system_single:
      if len(dialects) != 1:
        raise CommandError("--system is only valid when exactly one --dialect is provided.")
      d = dialects[0]
      systems_map[d] = system_single

    _validate_engine_truth_systems(dialects, systems_map)

    out_root.mkdir(parents=True, exist_ok=True)

    wrote: list[tuple[str, int, str]] = []
    for d in dialects:
      res = _run_provider(d, systems_map)
      provider_kw = set(res.keywords)

      if not provider_kw:
        raise CommandError(f"{d}: provider returned no keywords (source: {res.source}).")

      # Maintainer guardrails for doc-truth dialects.
      if d in DOC_TRUTH_DIALECTS or d in DOC_TRUTH_MIN_COUNTS or d in DOC_TRUTH_MAX_COUNTS:
        _doc_truth_size_guard(d, provider_kw)
        _doc_truth_plausibility_guard(d, provider_kw)

      _sanity_check_keywords(d, provider_kw)

      kw = set(provider_kw)
      if use_overlay:
        kw |= set(CORE_SQL_KEYWORDS)

      content = _render_keywords_module(dialect=d, keywords=kw, source=res.source)
      out_file = out_root / f"{d}.py"
      out_file.write_text(content, encoding="utf-8")

      wrote.append((d, len(kw), str(out_file)))

    self.stdout.write(self.style.SUCCESS("Reserved keyword modules generated:"))
    for d, n, path in wrote:
      self.stdout.write(f"  - {d:<16} {n:>6} keywords  ->  {path}")