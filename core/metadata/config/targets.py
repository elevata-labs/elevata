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

from __future__ import annotations

import os
from typing import Optional

from metadata.models import System
from metadata.config.profiles import load_profile
from metadata.secrets.resolver import resolve_ref_template_value


def resolve_target_system_name(explicit: Optional[str] = None) -> str:
  """
  Decide which System.short_name should act as the target.

  Resolution order:
    1. explicit argument (e.g. CLI flag or function param)
    2. env var ELEVATA_TARGET_SYSTEM
    3. otherwise: fail with a clear error
  """
  if explicit:
    return explicit

  env_name = os.getenv("ELEVATA_TARGET_SYSTEM")
  if env_name:
    return env_name

  raise RuntimeError(
    "No target system specified. "
    "Provide an explicit target system name, or set ELEVATA_TARGET_SYSTEM."
  )


def get_target_system(explicit: Optional[str] = None) -> System:
  """
  Return the System object that should be used as target.

  Ensures:
    - the system exists
    - the system is marked as a target (is_target = True)

  Additionally attaches runtime-only security:
    system.security["connection_string"]
  """
  name = resolve_target_system_name(explicit)

  try:
    system = System.objects.get(short_name=name)
  except System.DoesNotExist as exc:
    raise RuntimeError(
      f"System with short_name='{name}' not found. "
      "Check your configuration or the ELEVATA_TARGET_SYSTEM value."
    ) from exc

  if not system.is_target:
    raise RuntimeError(
      f"System '{name}' is not marked as a target (is_target = False). "
      "Set is_target=True in the admin if this system should be used as a target."
    )

  # Resolve connection string via profile template + provider chain (env / key vault / ...)
  profile = load_profile(None)
  conn_str = resolve_ref_template_value(
    profiles_path=None,
    ref_template=profile.secret_ref_template,
    type=system.type,
    short_name=system.short_name,
  )

  # Attach runtime-only security payload (System model intentionally does not persist secrets)
  system.security = {"connection_string": conn_str}

  return system
  