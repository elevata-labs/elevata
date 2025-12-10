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

"""
Tests for history load SQL routing.

For *_hist datasets, render_load_sql_for_target should NOT call the
generic full/merge/append renderers, but a dedicated history renderer
which currently returns a descriptive SQL comment.
"""

import textwrap

from metadata.rendering import load_sql
from metadata.rendering.load_sql import render_hist_incremental_sql


class DummyTargetSchema:
  def __init__(self, schema_name: str, short_name: str):
    self.schema_name = schema_name
    self.short_name = short_name


class DummyHistTargetDataset:
  def __init__(
    self,
    schema_name: str = "rawcore",
    schema_short_name: str = "rawcore",
    dataset_name: str = "rc_aw_product_hist",
  ):
    self.target_schema = DummyTargetSchema(
      schema_name=schema_name,
      short_name=schema_short_name,
    )
    self.target_dataset_name = dataset_name

# We do not need a real dialect here because the current implementation
# of render_hist_incremental_sql does not use it. A simple dummy is enough.
class DummyDialect:
  def render_identifier(self, name: str) -> str:
    # For tests we keep it simple: no quoting logic, just return the name
    return name

  def render_table_identifier(self, schema: str | None, name: str) -> str:
    if schema:
      return f"{schema}.{name}"
    return name
  
def test_render_load_sql_for_hist_routes_to_hist_renderer(monkeypatch):
  """
  Ensure that *_hist datasets are routed to render_hist_incremental_sql
  and that the returned SQL is a descriptive comment, not a MERGE/FULL statement.
  """

  td = DummyHistTargetDataset()

  dialect = DummyDialect()

  sql = load_sql.render_load_sql_for_target(td, dialect)
  normalized = textwrap.dedent(sql).strip()

  # Basic expectations: comment, schema+table mentioned, SCD wording present.
  assert normalized.startswith("-- History load for rawcore.rc_aw_product_hist")
  assert "SCD Type 2" in normalized or "SCD Type 2" in normalized
  assert "row_hash" in normalized
  assert "version_started_at" in normalized
  assert "version_ended_at" in normalized
  assert "load_run_id" in normalized

def test_hist_sql_contains_changed_update_block():
  td = DummyHistTargetDataset()
  dialect = DummyDialect()

  sql = load_sql.render_hist_incremental_sql(td, dialect)

  # Changed-UPDATE-Block sollte enthalten sein
  assert "version_state    = 'changed'" in sql
  assert "row_hash <>" in sql
  assert "UPDATE rawcore.rc_aw_product_hist AS h" in sql
