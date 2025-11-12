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

import pytest


def main():
  """Configure Django and run pytest."""
  root = Path(__file__).resolve().parent

  # Ensure repository root is on sys.path so 'core' and 'utils' can be imported
  if str(root) not in sys.path:
    sys.path.insert(0, str(root))

  # Match your manage.py setting
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "elevata_site.settings")

  # Run tests in core/tests
  return pytest.main(["core/tests"])


if __name__ == "__main__":
  raise SystemExit(main())
