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

from metadata.rendering import load_sql as load_mod
from tests._dialect_test_mixin import DialectTestMixin


class DummyTargetDataset:
  """Minimal stub; the routing code only passes it through."""

  def __init__(self, name: str = "dummy_dataset"):
    self.target_dataset_name = name

  @property
  def is_hist(self) -> bool:
    return (
      getattr(getattr(self, "target_schema", None), "short_name", None) == "rawcore"
      and getattr(self, "incremental_strategy", None) == "historize"
    )


class DummyDialect(DialectTestMixin):
  pass


def _make_plan(mode: str, handle_deletes: bool = False):
  """Create a minimal plan-like object."""
  return SimpleNamespace(mode=mode, handle_deletes=handle_deletes)


def test_render_load_sql_for_target_routes_full_mode(monkeypatch):
  td = DummyTargetDataset()
  dialect = DummyDialect()

  calls = {
    "full": 0,
    "append": 0,
    "merge": 0,
    "snapshot": 0,
    "delete": 0,
  }

  # Plan: mode="full"
  monkeypatch.setattr(load_mod, "build_load_plan", lambda _td: _make_plan("full"))

  def fake_full(td_arg, dialect_arg):
    calls["full"] += 1
    # sanity: caller passes through params
    assert td_arg is td
    assert dialect_arg is dialect
    return "-- full refresh sql"

  # Other renderers should not be called in this test
  monkeypatch.setattr(load_mod, "render_full_refresh_sql", fake_full)
  monkeypatch.setattr(
    load_mod,
    "render_append_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_append_sql must not be called for mode=full"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_merge_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_merge_sql must not be called for mode=full"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_snapshot_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_snapshot_sql must not be called for mode=full"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_delete_missing_rows_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_delete_missing_rows_sql must not be called for mode=full"),
    ),
  )

  sql = load_mod.render_load_sql_for_target(td, dialect)

  assert "-- full refresh sql" in sql
  assert calls["full"] == 1


def test_render_load_sql_for_target_routes_append_mode(monkeypatch):
  td = DummyTargetDataset()
  dialect = DummyDialect()

  monkeypatch.setattr(load_mod, "build_load_plan", lambda _td: _make_plan("append"))

  called = {"append": 0}

  def fake_append(td_arg, dialect_arg):
    called["append"] += 1
    assert td_arg is td
    assert dialect_arg is dialect
    return "-- append sql"

  monkeypatch.setattr(load_mod, "render_append_sql", fake_append)

  # Other renderers should not be called
  monkeypatch.setattr(
    load_mod,
    "render_full_refresh_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_full_refresh_sql must not be called for mode=append"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_merge_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_merge_sql must not be called for mode=append"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_snapshot_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_snapshot_sql must not be called for mode=append"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_delete_missing_rows_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_delete_missing_rows_sql must not be called for mode=append"),
    ),
  )

  sql = load_mod.render_load_sql_for_target(td, dialect)

  assert "-- append sql" in sql
  assert called["append"] == 1


def test_render_load_sql_for_target_routes_snapshot_mode(monkeypatch):
  td = DummyTargetDataset()
  dialect = DummyDialect()

  monkeypatch.setattr(load_mod, "build_load_plan", lambda _td: _make_plan("snapshot"))

  called = {"snapshot": 0}

  def fake_snapshot(td_arg, dialect_arg):
    called["snapshot"] += 1
    assert td_arg is td
    assert dialect_arg is dialect
    return "-- snapshot sql"

  monkeypatch.setattr(load_mod, "render_snapshot_sql", fake_snapshot)

  # Other renderers must not be called
  monkeypatch.setattr(
    load_mod,
    "render_full_refresh_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_full_refresh_sql must not be called for mode=snapshot"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_append_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_append_sql must not be called for mode=snapshot"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_merge_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_merge_sql must not be called for mode=snapshot"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_delete_missing_rows_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_delete_missing_rows_sql must not be called for mode=snapshot"),
    ),
  )

  sql = load_mod.render_load_sql_for_target(td, dialect)

  assert "-- snapshot sql" in sql
  assert called["snapshot"] == 1


def test_render_load_sql_for_target_merge_mode_without_delete(monkeypatch):
  """
  MERGE mode where render_delete_missing_rows_sql returns None -> only merge SQL.
  """
  td = DummyTargetDataset()
  dialect = DummyDialect()

  monkeypatch.setattr(
    load_mod,
    "build_load_plan",
    lambda _td: _make_plan("merge", handle_deletes=False),
  )

  called = {"merge": 0, "delete": 0}

  def fake_merge(td_arg, dialect_arg):
    called["merge"] += 1
    assert td_arg is td
    assert dialect_arg is dialect
    return "-- merge sql"

  def fake_delete(td_arg, dialect_arg):
    called["delete"] += 1
    # For this test, simulate "no delete detection SQL"
    return None

  monkeypatch.setattr(load_mod, "render_merge_sql", fake_merge)
  monkeypatch.setattr(load_mod, "render_delete_missing_rows_sql", fake_delete)

  # Other renderers should not be called
  monkeypatch.setattr(
    load_mod,
    "render_full_refresh_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_full_refresh_sql must not be called for mode=merge"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_append_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_append_sql must not be called for mode=merge"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_snapshot_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_snapshot_sql must not be called for mode=merge"),
    ),
  )

  sql = load_mod.render_load_sql_for_target(td, dialect)

  assert sql.strip() == "-- merge sql"
  assert called["merge"] == 1
  assert called["delete"] == 1  # function was called, but returned None


def test_render_load_sql_for_target_merge_mode_with_delete(monkeypatch):
  """
  MERGE mode where render_delete_missing_rows_sql returns a non-empty string ->
  delete SQL + blank line + merge SQL.
  """
  td = DummyTargetDataset()
  dialect = DummyDialect()

  monkeypatch.setattr(
    load_mod,
    "build_load_plan",
    lambda _td: _make_plan("merge", handle_deletes=True),
  )

  called = {"merge": 0, "delete": 0}

  def fake_merge(td_arg, dialect_arg):
    called["merge"] += 1
    assert td_arg is td
    assert dialect_arg is dialect
    return "-- merge sql"

  def fake_delete(td_arg, dialect_arg):
    called["delete"] += 1
    assert td_arg is td
    assert dialect_arg is dialect
    return "-- delete sql"

  monkeypatch.setattr(load_mod, "render_merge_sql", fake_merge)
  monkeypatch.setattr(load_mod, "render_delete_missing_rows_sql", fake_delete)

  # Other renderers should not be called
  monkeypatch.setattr(
    load_mod,
    "render_full_refresh_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_full_refresh_sql must not be called for mode=merge"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_append_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_append_sql must not be called for mode=merge"),
    ),
  )
  monkeypatch.setattr(
    load_mod,
    "render_snapshot_sql",
    lambda *_args, **_kwargs: (_ for _ in ()).throw(
      AssertionError("render_snapshot_sql must not be called for mode=merge"),
    ),
  )

  sql = load_mod.render_load_sql_for_target(td, dialect)

  # Expect concatenation: delete + blank line + merge
  assert "-- delete sql" in sql
  assert "-- merge sql" in sql
  assert "\n\n" in sql

  assert called["merge"] == 1
  assert called["delete"] == 1
