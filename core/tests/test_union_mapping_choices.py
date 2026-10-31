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

from types import SimpleNamespace

from metadata import views_scoped


def test_union_mapping_choices_use_only_branch_output_columns(monkeypatch):
  contract = SimpleNamespace(
    columns=["product_model_name", "instructions_desc"],
    available_input_columns=[
      "product_model_name",
      "instructions_desc",
      "catalog_desc",
      "rc_aw_product_model_key",
    ],
  )

  from metadata.generation import query_contract

  monkeypatch.setattr(query_contract, "infer_query_node_contract", lambda _node: contract)

  assert views_scoped._query_node_output_column_names(object()) == [
    "product_model_name",
    "instructions_desc",
  ]
