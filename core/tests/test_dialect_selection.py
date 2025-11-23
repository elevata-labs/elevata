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

import os

import pytest

from metadata.rendering.dialects import get_active_dialect
from metadata.rendering.dialects.duckdb import DuckDBDialect


class DummyProfile:
  """Simple profile stub used for dialect resolution tests."""

  def __init__(self, default_dialect: str) -> None:
    self.default_dialect = default_dialect


@pytest.fixture(autouse=True)
def clear_dialect_env(monkeypatch):
  """Ensure dialect-related env vars are clean by default."""
  monkeypatch.delenv("ELEVATA_SQL_DIALECT", raising=False)
  monkeypatch.delenv("ELEVATA_DIALECT", raising=False)
  yield


def test_explicit_name_bypasses_env_and_profile(monkeypatch):
  """
  When a name is passed explicitly to get_active_dialect(),
  it must not depend on env vars or profiles.
  """
  # If get_active_dialect tried to call load_profile here, we want to notice.
  from metadata.config import profiles as profiles_mod

  monkeypatch.setattr(
    profiles_mod,
    "load_profile",
    lambda *args, **kwargs: (_ for _ in ()).throw(
      AssertionError("load_profile must not be called for explicit name"),
    ),
  )

  # Even if an env var is set, the explicit argument should dominate.
  monkeypatch.setenv("ELEVATA_SQL_DIALECT", "duckdb")

  dialect = get_active_dialect("duckdb")
  assert isinstance(dialect, DuckDBDialect)


def test_env_override_used_without_loading_profile(monkeypatch):
  """
  If ELEVATA_SQL_DIALECT (or ELEVATA_DIALECT) is set,
  get_active_dialect() must not load a profile.
  """
  from metadata.config import profiles as profiles_mod

  # Make sure that calling load_profile would fail loudly.
  monkeypatch.setattr(
    profiles_mod,
    "load_profile",
    lambda *args, **kwargs: (_ for _ in ()).throw(
      AssertionError("load_profile must not be called when env override is set"),
    ),
  )

  monkeypatch.setenv("ELEVATA_SQL_DIALECT", "duckdb")

  dialect = get_active_dialect()
  assert isinstance(dialect, DuckDBDialect)


def test_profile_default_dialect_used_when_no_env(monkeypatch):
  """
  If no explicit name and no env overrides are set,
  get_active_dialect() should use profile.default_dialect.
  """
  from metadata.config import profiles as profiles_mod

  dummy_profile = DummyProfile(default_dialect="duckdb")
  monkeypatch.setattr(
    profiles_mod,
    "load_profile",
    lambda *args, **kwargs: dummy_profile,
  )

  dialect = get_active_dialect()
  assert isinstance(dialect, DuckDBDialect)


def test_unknown_dialect_raises_value_error(monkeypatch):
  """
  Passing an unknown dialect name must raise a ValueError,
  listing the available dialects.
  """
  with pytest.raises(ValueError) as excinfo:
    get_active_dialect("does_not_exist")

  # Optional: a minimal sanity check on the error message
  msg = str(excinfo.value).lower()
  assert "unknown sql dialect" in msg or "unknown dialect" in msg
