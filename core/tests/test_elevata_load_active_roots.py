from types import SimpleNamespace

import pytest

from metadata.management.commands import elevata_load


class _StopAfterRootResolution(Exception):
  """Stop the command immediately after root selection for a focused unit test."""


class _DummyQuerySet:
  def __init__(self, items, calls):
    self._items = list(items)
    self._calls = calls

  def filter(self, **kwargs):
    self._calls.append(("filter", kwargs))
    return self

  def __iter__(self):
    return iter(self._items)


class _DummyManager:
  def __init__(self, items, calls):
    self._items = list(items)
    self._calls = calls

  def all(self):
    self._calls.append(("all", {}))
    return _DummyQuerySet(self._items, self._calls)

  def filter(self, **kwargs):
    self._calls.append(("filter", kwargs))
    return _DummyQuerySet(self._items, self._calls)


def test_all_datasets_uses_only_active_target_datasets_as_roots(monkeypatch):
  """Full-load root selection must exclude inactive TargetDatasets."""
  calls = []
  active_td = SimpleNamespace(
    target_schema=SimpleNamespace(short_name="bizcore"),
    target_dataset_name="bc_active",
  )

  dummy_target_dataset = SimpleNamespace(
    objects=_DummyManager([active_td], calls),
  )
  monkeypatch.setattr(elevata_load, "TargetDataset", dummy_target_dataset)

  def _stop_after_roots(_profile_name):
    raise _StopAfterRootResolution()

  monkeypatch.setattr(elevata_load, "load_profile", _stop_after_roots)

  command = elevata_load.Command()
  with pytest.raises(_StopAfterRootResolution):
    command.handle(
      target_name=None,
      schema_short=None,
      all_datasets=True,
      partial_load_name=None,
      dialect_name=None,
      target_system_name=None,
      execute=False,
      no_print=True,
    )

  assert calls[0] == ("filter", {"active": True})
  assert not any(name == "all" for name, _kwargs in calls)
