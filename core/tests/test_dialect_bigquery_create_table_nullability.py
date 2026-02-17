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

from metadata.rendering.dialects.bigquery import BigQueryDialect


def test_bigquery_render_create_table_from_columns_does_not_emit_nullable_keyword():
  d = BigQueryDialect()

  sql = d.render_create_table_from_columns(
    schema="rawcore",
    table="t1",
    columns=[
      {"name": "a", "type": "STRING", "nullable": True},
      {"name": "b", "type": "INT64", "nullable": False},
      {"name": "c", "type": "DATE", "nullable": True},
    ],
  )

  # Tokenize in a way that does not confuse "NULL," or "NULL)" etc.
  tokens_raw = sql.upper().replace("\n", " ").replace("\t", " ").split()
  tokens = [t.strip(",);") for t in tokens_raw]

  # 1) BigQuery must not contain explicit NULL (only allowed as part of NOT NULL).
  for i, tok in enumerate(tokens):
    if tok == "NULL":
      assert i > 0 and tokens[i - 1] == "NOT"

  # 2) If nullable=False is requested, ensure NOT NULL is rendered.
  # Use token pairs so "IF NOT EXISTS" does not satisfy this.
  pairs = list(zip(tokens, tokens[1:]))
  assert ("NOT", "NULL") in pairs
