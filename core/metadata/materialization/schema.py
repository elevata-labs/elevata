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

def ensure_target_schema(*, engine, dialect, schema_name: str, auto_provision: bool) -> None:
  """
  Ensure schema exists. Safe to call multiple times.
  Best-effort: does nothing if auto_provision is False or dialect lacks DDL helper.
  """
  if not auto_provision:
    return

  if not hasattr(dialect, "render_create_schema_if_not_exists"):
    return

  ddl = dialect.render_create_schema_if_not_exists(schema_name)
  if ddl:
    engine.execute(ddl)