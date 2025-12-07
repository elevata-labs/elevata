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
Tests for build_sql_preview_for_target, especially the *_hist guard.
"""

import pytest

from metadata.rendering import preview as preview_mod


class DummySchema:
  def __init__(self, short_name: str):
    self.short_name = short_name


class DummyTargetDataset:
  """Minimal stub; the preview code only needs name + schema."""
  def __init__(self, name: str, schema_short: str = "rawcore"):
    self.target_dataset_name = name
    self.target_schema = DummySchema(schema_short)


def test_build_sql_preview_for_target_calls_renderer_for_non_hist(monkeypatch):
  """Non-history datasets should call render_select_for_target as usual."""
  td = DummyTargetDataset("rc_customer")
  dialect = object()

  calls = {"select": 0}

  def fake_render_select(dataset_arg, dialect_arg):
    calls["select"] += 1
    # Sanity: parameters are passed through unchanged
    assert dataset_arg is td
    assert dialect_arg is dialect
    return "SELECT 1"

  monkeypatch.setattr(preview_mod, "render_select_for_target", fake_render_select)

  sql = preview_mod.build_sql_preview_for_target(td, dialect)

  # Whitespace-insensitive check: beautify_sql may introduce line breaks.
  normalized = " ".join(sql.split())
  assert normalized == "SELECT 1"
  assert calls["select"] == 1


def test_build_sql_preview_for_target_skips_renderer_for_hist(monkeypatch):
  """History datasets (rawcore *_hist) must not call render_select_for_target."""
  td = DummyTargetDataset("rc_customer_hist")
  dialect = object()

  def exploding_renderer(*_args, **_kwargs):
    raise AssertionError("render_select_for_target must not be called for *_hist")

  # If the guard fails, this renderer will immediately blow up the test.
  monkeypatch.setattr(preview_mod, "render_select_for_target", exploding_renderer)

  sql = preview_mod.build_sql_preview_for_target(td, dialect)

  # Guard: we should get a descriptive comment instead of a real SELECT.
  assert "SQL preview for history dataset rc_customer_hist is not implemented yet." in sql
