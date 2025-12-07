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

from metadata.rendering.renderer import render_select_for_target
from metadata.rendering.dialects import get_active_dialect
from metadata.rendering.dialects.base import SqlDialect


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
    "as", "and", "or", "case", "when", "then", "else", "end",
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

  # 4) Put SELECT list on separate lines
  sql = re.sub(r"SELECT\s+", "SELECT\n  ", sql)
  sql = sql.replace(", ", ",\n  ")

  # 5) Indent lines following FROM / WHERE / GROUP BY / ORDER BY
  sql = re.sub(r"\n(FROM|WHERE|GROUP BY|ORDER BY)\s+", r"\n\1\n  ", sql)

  return sql.strip()


def build_sql_preview_for_target(dataset, dialect: SqlDialect | None = None) -> str:
  """
  Build the final SQL preview for a target dataset.

  If `dialect` is None, the active dialect is resolved from env/profile.
  """

  if dialect is None:
    dialect = get_active_dialect()

  # History datasets do not have a dedicated query yet.
  # We return a descriptive comment instead of a misleading SELECT.
  schema = getattr(dataset, "target_schema", None)
  schema_short = getattr(schema, "short_name", None)
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

  raw_sql = render_select_for_target(dataset, dialect)
  return beautify_sql(raw_sql)