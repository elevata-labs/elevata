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

from copy import deepcopy

from .settings import *  # noqa: F403,F401

"""
Test settings for elevata.

- Never use the real metadata DB for tests.
- If sqlite is selected, force the Django TEST database to run in-memory.
- If postgres is selected, Django will create and use a separate test database.
"""

# Make a defensive copy so we don't mutate the base settings dict in-place.
DATABASES = deepcopy(DATABASES)  # noqa: F405

default_db = DATABASES.get("default", {})
engine = str(default_db.get("ENGINE", "")).lower()

# For sqlite, ensure tests do NOT create a test_*.sqlite3 file.
if "sqlite" in engine:
  default_db.setdefault("TEST", {})
  default_db["TEST"]["NAME"] = ":memory:"
  DATABASES["default"] = default_db