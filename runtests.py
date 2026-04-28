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
import sys
from pathlib import Path
import importlib.util

import pytest


def main(argv=None):
  """Configure Django and run pytest."""
  root = Path(__file__).resolve().parent
  core = root / "core"

  if str(root) not in sys.path:
    sys.path.insert(0, str(root))
  if str(core) not in sys.path:
    sys.path.insert(0, str(core))

  # Safety: tests must run with pytest-django, otherwise they may touch the real DB.
  if importlib.util.find_spec("pytest_django") is None:
    raise SystemExit(
      "pytest-django is required to run elevata tests safely.\n"
      "Install it via: pip install pytest-django\n"
    )

  # Use test settings that enforce an isolated DB setup (sqlite => in-memory).
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "elevata_site.settings_test")

  if argv is None:
    argv = sys.argv[1:]

  # Always start in core/tests, allow extra filters/paths
  args = ["core/tests"]
  args.extend(argv)

  return pytest.main(args)

if __name__ == "__main__":
  raise SystemExit(main())

