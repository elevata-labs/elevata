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

def test_query_tree_allowed_for_dataset_only_bizcore_and_serving():
  from metadata.generation.policies import query_tree_allowed_for_dataset

  class FakeSchema:
    def __init__(self, short_name):
      self.short_name = short_name

  class FakeTD:
    def __init__(self, short_name):
      self.target_schema = FakeSchema(short_name)

  assert query_tree_allowed_for_dataset(FakeTD("bizcore")) is True
  assert query_tree_allowed_for_dataset(FakeTD("serving")) is True

  assert query_tree_allowed_for_dataset(FakeTD("raw")) is False
  assert query_tree_allowed_for_dataset(FakeTD("rawcore")) is False
  assert query_tree_allowed_for_dataset(FakeTD("stage")) is False
