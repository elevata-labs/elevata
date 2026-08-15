"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2026 Ilona Tag

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
Scoped-list filter and navigation contract tests.
"""

import metadata.views_scoped as views_scoped
from metadata.views_scoped import QueryNodeScopedView, TargetColumnScopedView


def test_scoped_filter_config_excludes_url_defined_parent_relation():
  view = TargetColumnScopedView()
  configs = view.build_scoped_auto_filter_config()
  field_paths = {config["field_path"] for config in configs}

  assert "target_dataset" not in field_paths
  assert "target_column_name" in field_paths


def _fake_target_dataset(pk=7):
  TargetDataset = type(
    "TargetDataset",
    (),
    {
      "pk": pk,
      "__str__": lambda self: "rc_aw_product",
    },
  )
  return TargetDataset()


def _fake_reverse(route, args=None, **kwargs):
  pk = (args or [None])[0]
  if route == "targetdataset_detail":
    return f"/metadata/targetdataset/{pk}/"
  if route == "targetdataset_query_builder":
    return f"/metadata/targetdataset/{pk}/query-builder/"
  raise RuntimeError(f"No fake route for {route}")


def test_non_query_scoped_navigation_keeps_direct_parent_back_link(monkeypatch):
  parent = _fake_target_dataset()
  view = TargetColumnScopedView()
  monkeypatch.setattr(view, "get_parent_pk", lambda: parent.pk)
  monkeypatch.setattr(view, "get_parent_object", lambda: parent)
  monkeypatch.setattr(views_scoped, "reverse", _fake_reverse)
  monkeypatch.setattr(views_scoped, "_get_query_builder_dataset", lambda obj: parent)

  ctx = {}
  view._apply_scoped_nav_context(ctx)

  assert ctx["scoped_parent_label"] == "rc_aw_product"
  assert ctx["scoped_parent_url"] == "/metadata/targetdataset/7/"
  assert ctx["scoped_parent_is_query_builder"] is False
  assert ctx["scoped_query_dataset_pk"] == 7


def test_query_scoped_navigation_still_returns_to_query_builder(monkeypatch):
  parent = _fake_target_dataset()
  view = QueryNodeScopedView()
  monkeypatch.setattr(view, "get_parent_pk", lambda: parent.pk)
  monkeypatch.setattr(view, "get_parent_object", lambda: parent)
  monkeypatch.setattr(views_scoped, "reverse", _fake_reverse)
  monkeypatch.setattr(views_scoped, "_get_query_builder_dataset", lambda obj: parent)

  ctx = {}
  view._apply_scoped_nav_context(ctx)

  assert ctx["scoped_parent_url"] == "/metadata/targetdataset/7/query-builder/"
  assert ctx["scoped_parent_is_query_builder"] is True
  assert ctx["scoped_query_dataset_pk"] == 7
