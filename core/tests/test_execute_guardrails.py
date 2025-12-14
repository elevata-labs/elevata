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

import pytest

# Adjust import path if needed (wherever _looks_like_cross_system_sql lives now)
from metadata.management.commands.elevata_load import _looks_like_cross_system_sql


def test_guardrail_allows_alias_column_references_in_select_list():
  sql = """
  SELECT
    s.productid AS productid,
    s.name AS name
  FROM
    raw.raw_aw1_product AS s
  """

  # target_schema is 'stage' here, but allowed schemas include raw/stage/rawcore/meta
  assert _looks_like_cross_system_sql(sql, target_schema="stage") is False


def test_guardrail_blocks_non_target_schema_in_from_join():
  sql = """
  SELECT p.ProductID
  FROM Production.Product AS p
  """

  assert _looks_like_cross_system_sql(sql, target_schema="rawcore") is True
