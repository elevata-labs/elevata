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

import pytest
from django.urls import reverse

from metadata.services.query_graph_mutation import QueryGraphMutationError
from metadata.views import _query_builder_placement


def _request(*, method="GET", get=None, post=None):
  return SimpleNamespace(
    method=method,
    GET=get or {},
    POST=post or {},
  )


def test_query_builder_placement_defaults_to_final_output():
  assert _query_builder_placement(_request()) == ("after_head", None, None)


def test_query_builder_placement_reads_before_node_target():
  placement = _query_builder_placement(
    _request(get={"placement": "before_node", "target_node_id": "42"})
  )
  assert placement == ("before_node", 42, None)


def test_query_builder_placement_reads_union_branch_from_post():
  placement = _query_builder_placement(
    _request(
      method="POST",
      post={"placement": "union_branch", "branch_id": "17"},
    )
  )
  assert placement == ("union_branch", None, 17)


def test_query_builder_placement_rejects_unknown_position():
  with pytest.raises(QueryGraphMutationError, match="Unsupported query placement"):
    _query_builder_placement(_request(get={"placement": "somewhere"}))


def test_query_builder_move_route_is_registered():
  assert reverse("targetdataset_move_query_node", args=[42]).endswith(
    "/metadata/target-datasets/42/query/move-step/"
  )


def test_union_reconciliation_routes_are_registered():
  assert reverse("queryunion_reconciliation", args=[42]).endswith(
    "/metadata/queryunion/42/reconciliation/"
  )
  assert reverse("queryunionbranch_apply_safe_matches", args=[17]).endswith(
    "/metadata/queryunionbranch/17/mappings/apply-safe/"
  )
