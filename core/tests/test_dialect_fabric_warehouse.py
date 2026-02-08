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

from metadata.rendering.dialects.dialect_factory import get_active_dialect
from metadata.rendering.dialects.fabric_warehouse import FabricWarehouseDialect


def test_fabric_warehouse_dialect_is_registered():
  d = get_active_dialect("fabric_warehouse")
  assert isinstance(d, FabricWarehouseDialect)


def test_fabric_warehouse_quote_ident_uses_double_quotes():
  d = FabricWarehouseDialect()
  assert d.quote_ident("foo") == '"foo"'


def test_fabric_warehouse_hash_expression_uses_hashbytes():
  d = FabricWarehouseDialect()
  sql = d.hash_expression("('x')")
  low = sql.lower()
  assert "hashbytes" in low
  assert "sha2_256" in sql.lower()


def test_fabric_warehouse_delete_detection_contains_delete():
  d = FabricWarehouseDialect()
  sql = d.render_delete_detection_statement(
    target_schema="dw",
    target_table="dim_x",
    stage_schema="stg",
    stage_table="dim_x_stage",
    join_predicates=["t.id = s.id"],
  )
  assert "delete" in sql.lower()
  assert "not exists" in sql.lower()
