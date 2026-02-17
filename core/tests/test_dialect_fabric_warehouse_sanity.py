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

import pytest

from metadata.rendering.dialects.dialect_factory import get_available_dialect_names, get_active_dialect
from metadata.rendering.dialects.fabric_warehouse import FabricWarehouseDialect


def test_fabric_warehouse_sanity_lengths_and_log_typing():
  if "fabric_warehouse" not in set(get_available_dialect_names()):
    pytest.skip("fabric_warehouse dialect not registered")

  d = get_active_dialect("fabric_warehouse")

  # Load-run-log mapping: conservative, deterministic lengths
  assert getattr(d, "LOAD_RUN_LOG_TYPE_MAP", {}).get("string") == "VARCHAR(500)"
  assert d.map_load_run_log_type("error_message", "string") == "VARCHAR(2000)"
  assert d.map_load_run_log_type("snapshot_json", "string") == "VARCHAR(4000)"

  # Canonical type mapping must avoid MAX/8000 and NVARCHAR in Fabric Warehouse
  t1 = d.render_physical_type(canonical="string", max_length=None, precision=None, scale=None, strict=True)
  assert "VARCHAR(" in t1.upper()
  assert "MAX" in t1.upper()
  assert "NVARCHAR" not in t1.upper()

  # UUID should be stored as portable string (avoid UNIQUEIDENTIFIER cross-endpoint surprises)
  t_uuid = d.render_physical_type(canonical="uuid", max_length=None, precision=None, scale=None, strict=True)
  assert t_uuid.upper() == "VARCHAR(36)"

  # Binary should be bounded
  t_bin = d.render_physical_type(canonical="binary", max_length=None, precision=None, scale=None, strict=True)
  assert "VARBINARY(" in t_bin.upper()
  assert "MAX" not in t_bin.upper()

def test_fabric_warehouse_enforces_length_on_bare_varchar_in_ddl():
  if "fabric_warehouse" not in set(get_available_dialect_names()):
    pytest.skip("fabric_warehouse dialect not registered")

  d = get_active_dialect("fabric_warehouse")
  assert isinstance(d, FabricWarehouseDialect)

  # 1) ALTER TABLE ADD ... VARCHAR must never be emitted without explicit length.
  ddl_add = d.render_add_column(
    schema="rawcore",
    table="t_test",
    col_name="c_text",
    physical_type="VARCHAR",
  )
  up_add = ddl_add.upper()
  assert "VARCHAR(" in up_add
  assert "VARCHAR)" not in up_add

  # 2) CREATE TABLE ... VARCHAR must never be emitted without explicit length.
  ddl_create = d.render_create_table_if_not_exists_from_columns(
    schema="rawcore",
    table="t_test",
    columns=[
      {"name": "c_text", "type": "VARCHAR", "nullable": True},
    ],
  )
  up_create = ddl_create.upper()
  assert "VARCHAR(" in up_create
  assert "VARCHAR)" not in up_create
  

def test_fabric_warehouse_ensure_length_spec_preserves_varchar_max_and_fills_bare_varchar():
  """
  Regression guards:
    - VARCHAR(MAX) must never be "normalized" to VARCHAR(4000)
    - bare VARCHAR must become deterministic VARCHAR(<DEFAULT>)
  """
  if "fabric_warehouse" not in set(get_available_dialect_names()):
    pytest.skip("fabric_warehouse dialect not registered")

  d = get_active_dialect("fabric_warehouse")
  assert isinstance(d, FabricWarehouseDialect)

  # 1) Preserve MAX (must not be replaced with default length)
  assert d._ensure_length_spec("VARCHAR(MAX)").upper() == "VARCHAR(MAX)"

  # 2) Fill in deterministic default for bare VARCHAR
  filled = d._ensure_length_spec("VARCHAR").upper()
  assert filled.startswith("VARCHAR(")
  assert filled.endswith(")")
  assert filled == "VARCHAR(4000)"
  

def test_fabric_warehouse_ensure_length_spec_preserves_varchar_max_and_fills_bare_varchar():
  """
  Regression guards:
    - VARCHAR(MAX) must never be normalized to VARCHAR(4000)
    - bare VARCHAR must become deterministic VARCHAR(4000)
  """
  if "fabric_warehouse" not in set(get_available_dialect_names()):
    pytest.skip("fabric_warehouse dialect not registered")

  d = get_active_dialect("fabric_warehouse")
  assert isinstance(d, FabricWarehouseDialect)

  # Preserve MAX (must not be replaced with default length)
  assert d._ensure_length_spec("VARCHAR(MAX)").upper() == "VARCHAR(MAX)"

  # Fill deterministic default for bare VARCHAR (Fabric would otherwise create VARCHAR(1))
  assert d._ensure_length_spec("VARCHAR").upper() == "VARCHAR(4000)"
