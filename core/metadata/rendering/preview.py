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

from django.http import HttpResponse
from django.shortcuts import get_object_or_404

from metadata.models import TargetDataset
from metadata.rendering.builder import build_logical_select_for_target
from metadata.rendering.renderer import render_sql
from metadata.rendering.dialects.duckdb import DuckDBDialect


def beautify_sql(sql: str) -> str:
  """
  Lightweight SQL beautifier for previews.
  - Uppercases main SQL keywords
  - Normalizes spacing and line breaks
  - Puts SELECT list on separate indented lines
  - Indents FROM and following table reference
  """
  import re

  # 1) Normalize whitespace
  sql = re.sub(r"[ \t]+", " ", sql)
  sql = re.sub(r"\n\s*", "\n", sql.strip())

  # 2) Uppercase key SQL keywords
  keywords = [
    "select", "from", "where", "group by", "order by", "union", "union all",
    "inner join", "left join", "right join", "full join", "on",
    "as", "and", "or", "case", "when", "then", "else", "end"
  ]
  for kw in sorted(keywords, key=len, reverse=True):
    pattern = r"\b" + re.escape(kw) + r"\b"
    sql = re.sub(pattern, kw.upper(), sql, flags=re.IGNORECASE)

  # 3) Line breaks before major clauses
  sql = re.sub(
    r"\b(FROM|WHERE|GROUP BY|ORDER BY|UNION ALL|UNION|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN)\b",
    r"\n\1",
    sql,
  )

  # 4) Put SELECT list on separate lines:
  #    "SELECT a, b, c"  ->  "SELECT\n  a,\n  b,\n  c"
  sql = re.sub(r"SELECT\s+", "SELECT\n  ", sql)
  sql = sql.replace(", ", ",\n  ")

  # 5) Indent the line following FROM / WHERE / GROUP BY / ORDER BY
  sql = re.sub(r"\n(FROM|WHERE|GROUP BY|ORDER BY)\s+", r"\n\1\n  ", sql)

  return sql.strip()


def build_sql_preview_for_target(target_ds):
  """Return SQL string only (used internally)."""
  logical_plan = build_logical_select_for_target(target_ds)
  dialect = DuckDBDialect()
  raw_sql = render_sql(logical_plan, dialect)
  return beautify_sql(raw_sql)


def preview_target_sql(request, pk):
  """Return rendered HTML preview."""
  try:
    target_ds = get_object_or_404(TargetDataset, pk=pk)
    sql = build_sql_preview_for_target(target_ds)
    return HttpResponse(
      '<div class="alert alert-success py-1 px-2 mb-0 small">'
      '<pre class="mb-0" style="white-space: pre-wrap;">'
      f'{sql}'
      '</pre>'
      '</div>'
    )
  except Exception as e:
    return HttpResponse(
      f'<div class="alert alert-danger py-1 px-2 mb-0 small">'
      f'SQL preview failed: {e}</div>',
      status=500,
    )
