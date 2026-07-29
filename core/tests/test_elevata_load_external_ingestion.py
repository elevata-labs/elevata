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

from __future__ import annotations

from types import SimpleNamespace

import pytest
from django.core.management.base import CommandError

from metadata.management.commands import elevata_load as mod


def _target_dataset():
  return SimpleNamespace(
    target_schema=SimpleNamespace(
      short_name="raw",
      schema_name="raw",
    ),
    target_dataset_name="raw_sap_kna1",
  )


def _target_system():
  return SimpleNamespace(
    short_name="dwh",
    type="duckdb",
  )


def test_external_raw_landing_keeps_existing_table_untouched(monkeypatch):
  td = _target_dataset()
  system = _target_system()
  engine = object()
  dialect = object()
  calls = []

  monkeypatch.setattr(mod, "get_active_dialect", lambda _type: dialect)
  monkeypatch.setattr(
    mod,
    "_ensure_target_schema_for_batch",
    lambda **kwargs: calls.append(("schema", kwargs)),
  )
  monkeypatch.setattr(
    mod,
    "_target_table_exists_for_ensure",
    lambda **_kwargs: True,
  )
  monkeypatch.setattr(
    mod,
    "ensure_target_table",
    lambda **kwargs: calls.append(("table", kwargs)),
  )

  attempted = mod._ensure_external_raw_landing(
    target_dataset=td,
    target_system=system,
    target_system_engine=engine,
    schema_ensure_state=set(),
  )

  assert attempted is False
  assert [kind for kind, _kwargs in calls] == ["schema"]
  assert calls[0][1]["schema_name"] == "raw"


def test_external_raw_landing_provisions_missing_table_and_continues(monkeypatch):
  td = _target_dataset()
  system = _target_system()
  engine = object()
  dialect = object()
  exists = iter((False, True))
  ensured = []

  monkeypatch.setattr(mod, "get_active_dialect", lambda _type: dialect)
  monkeypatch.setattr(mod, "_ensure_target_schema_for_batch", lambda **_kwargs: None)
  monkeypatch.setattr(
    mod,
    "_target_table_exists_for_ensure",
    lambda **_kwargs: next(exists),
  )
  monkeypatch.setattr(
    mod,
    "ensure_target_table",
    lambda **kwargs: ensured.append(kwargs),
  )

  attempted = mod._ensure_external_raw_landing(
    target_dataset=td,
    target_system=system,
    target_system_engine=engine,
  )

  assert attempted is True
  assert len(ensured) == 1
  assert ensured[0]["auto_provision"] is mod.AUTO_PROVISION_TABLES


def test_external_raw_landing_allows_unverifiable_idempotent_ensure(monkeypatch):
  td = _target_dataset()
  system = _target_system()
  engine = object()
  dialect = object()
  ensured = []

  monkeypatch.setattr(mod, "get_active_dialect", lambda _type: dialect)
  monkeypatch.setattr(mod, "_ensure_target_schema_for_batch", lambda **_kwargs: None)
  monkeypatch.setattr(
    mod,
    "_target_table_exists_for_ensure",
    lambda **_kwargs: None,
  )
  monkeypatch.setattr(
    mod,
    "ensure_target_table",
    lambda **kwargs: ensured.append(kwargs),
  )

  attempted = mod._ensure_external_raw_landing(
    target_dataset=td,
    target_system=system,
    target_system_engine=engine,
  )

  assert attempted is True
  assert len(ensured) == 1


def test_external_raw_landing_blocks_when_table_stays_explicitly_missing(monkeypatch):
  td = _target_dataset()
  system = _target_system()
  engine = object()
  dialect = object()
  exists = iter((False, False))

  monkeypatch.setattr(mod, "get_active_dialect", lambda _type: dialect)
  monkeypatch.setattr(mod, "_ensure_target_schema_for_batch", lambda **_kwargs: None)
  monkeypatch.setattr(
    mod,
    "_target_table_exists_for_ensure",
    lambda **_kwargs: next(exists),
  )
  monkeypatch.setattr(mod, "ensure_target_table", lambda **_kwargs: None)

  with pytest.raises(CommandError, match="is still missing"):
    mod._ensure_external_raw_landing(
      target_dataset=td,
      target_system=system,
      target_system_engine=engine,
    )


def test_external_raw_landing_resolves_target_engine(monkeypatch):
  td = _target_dataset()
  system = _target_system()
  engine = object()
  dialect = SimpleNamespace(
    get_execution_engine=lambda selected_system: (
      engine if selected_system is system else None
    ),
  )
  observed_engines = []

  monkeypatch.setattr(mod, "get_active_dialect", lambda _type: dialect)
  monkeypatch.setattr(mod, "_ensure_target_schema_for_batch", lambda **_kwargs: None)
  monkeypatch.setattr(
    mod,
    "_target_table_exists_for_ensure",
    lambda **kwargs: observed_engines.append(kwargs["exec_engine"]) or True,
  )
  monkeypatch.setattr(mod, "ensure_target_table", lambda **_kwargs: None)

  attempted = mod._ensure_external_raw_landing(
    target_dataset=td,
    target_system=system,
  )

  assert attempted is False
  assert observed_engines == [engine]


def test_execute_external_raw_ensures_landing_and_keeps_downstream_eligible(monkeypatch):
  td = _target_dataset()
  system = _target_system()
  profile = SimpleNamespace(name="dev")
  source_dataset = object()
  calls = []

  monkeypatch.setattr(
    mod,
    "resolve_single_source_dataset_for_raw",
    lambda selected_td: source_dataset if selected_td is td else None,
  )
  monkeypatch.setattr(mod, "resolve_ingest_mode", lambda _source: "external")
  monkeypatch.setattr(
    mod,
    "_ensure_external_raw_landing",
    lambda **kwargs: calls.append(kwargs) or True,
  )

  result = mod.execute_raw_via_ingestion(
    target_dataset=td,
    target_system=system,
    profile=profile,
    target_system_engine=object(),
    schema_ensure_state=set(),
  )

  assert result == {
    "status": "skipped",
    "reason": "external_ingest",
  }
  assert len(calls) == 1
  assert calls[0]["target_dataset"] is td
  assert calls[0]["target_system"] is system
