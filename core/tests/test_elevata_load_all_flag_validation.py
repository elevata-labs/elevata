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

import pytest
from django.core.management.base import CommandError

from metadata.management.commands.elevata_load import Command


def test_validate_root_selection_rejects_target_and_all():
  cmd = Command()
  with pytest.raises(CommandError):
    cmd._validate_root_selection(target_name="x", all_datasets=True)


def test_validate_root_selection_rejects_missing_both():
  cmd = Command()
  with pytest.raises(CommandError):
    cmd._validate_root_selection(target_name=None, all_datasets=False)


def test_validate_root_selection_accepts_target_only():
  cmd = Command()
  cmd._validate_root_selection(target_name="x", all_datasets=False)


def test_validate_root_selection_accepts_all_only():
  cmd = Command()
  cmd._validate_root_selection(target_name=None, all_datasets=True)
