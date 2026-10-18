"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

This file is part of elevata.

elevata is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.
"""

from types import SimpleNamespace

import metadata.views_scoped as views_scoped


class _FakeColumn:
  def __init__(self, *, name, active, lineage_origin, is_system_managed=False, ordinal_position=1):
    self.target_column_name = name
    self.active = active
    self.lineage_origin = lineage_origin
    self.is_system_managed = is_system_managed
    self.ordinal_position = ordinal_position
    self.id = ordinal_position
    self.retired_at = object() if not active else None
    self.updated_by = None
    self.saved_update_fields = None

  def save(self, update_fields=None):
    self.saved_update_fields = list(update_fields or [])


class _FakeQuerySet:
  def __init__(self, items):
    self.items = list(items)

  def exclude(self, **kwargs):
    out = self.items
    for field, value in kwargs.items():
      out = [item for item in out if getattr(item, field) != value]
    return _FakeQuerySet(out)

  def order_by(self, *fields):
    def key(item):
      return tuple(getattr(item, field, 0) for field in fields)
    return sorted(self.items, key=key)


class _FakeManager:
  def __init__(self, items):
    self.items = list(items)

  def filter(self, **kwargs):
    out = self.items
    for field, value in kwargs.items():
      if field == "target_dataset":
        continue
      out = [item for item in out if getattr(item, field) == value]
    return _FakeQuerySet(out)


def test_aggregate_rewind_reactivates_only_inactive_authored_columns(monkeypatch):
  authored = _FakeColumn(
    name="product",
    active=False,
    lineage_origin="direct",
    ordinal_position=3,
  )
  query_derived = _FakeColumn(
    name="product_list",
    active=False,
    lineage_origin="query_derived",
    ordinal_position=4,
  )
  system_managed = _FakeColumn(
    name="bc_dim_product_key",
    active=False,
    lineage_origin="direct",
    is_system_managed=True,
    ordinal_position=1,
  )
  already_active = _FakeColumn(
    name="description",
    active=True,
    lineage_origin="direct",
    ordinal_position=2,
  )

  monkeypatch.setattr(
    views_scoped.TargetColumn,
    "objects",
    _FakeManager([authored, query_derived, system_managed, already_active]),
  )

  actor = SimpleNamespace(is_authenticated=True)
  views_scoped._reactivate_authored_columns_for_aggregate_rewind(object(), actor=actor)

  assert authored.active is True
  assert authored.retired_at is None
  assert authored.updated_by is actor
  assert set(authored.saved_update_fields) == {"active", "retired_at", "updated_by"}

  assert query_derived.active is False
  assert query_derived.saved_update_fields is None
  assert system_managed.active is False
  assert system_managed.saved_update_fields is None
  assert already_active.active is True
  assert already_active.saved_update_fields is None
