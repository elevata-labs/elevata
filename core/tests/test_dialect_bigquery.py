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


def test_bigquery_alter_column_type_accepts_old_type():
  d = BigQueryDialect()

  sql = d.render_alter_column_type(
    schema="rawcore",
    table="rc_customer",
    column="person_type_code",
    new_type="NUMERIC",
    old_type="INT64",
  )

  assert "ALTER TABLE" in sql
  assert "rawcore" in sql
  assert "rc_customer" in sql
  assert "ALTER COLUMN person_type_code SET DATA TYPE NUMERIC" in sql


def test_bigquery_alter_column_type_rebuilds_for_date_to_timestamp():
  d = BigQueryDialect()

  sql = d.render_alter_column_type(
    schema="rawcore",
    table="rc_sales_order",
    column="order_date",
    new_type="TIMESTAMP",
    old_type="DATE",
  )

  assert sql == ""


def test_bigquery_add_column_is_idempotent():
  d = BigQueryDialect()

  sql = d.render_add_column(
    schema="rawcore",
    table="rc_aw_sales_order_hist",
    column="sales_order_date",
    column_type="TIMESTAMP",
  )

  assert "ALTER TABLE" in sql
  assert "ADD COLUMN IF NOT EXISTS sales_order_date TIMESTAMP" in sql


def test_bigquery_rebuild_uses_cast_expression_for_rendered_sql_strings():
  d = BigQueryDialect()

  assert d.cast_expression("order_date", "TIMESTAMP") == "CAST(order_date AS TIMESTAMP)"
