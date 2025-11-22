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
import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from metadata.models import System

@dataclass
class Profile:
  name: str
  providers: List[Dict[str, Any]]
  # Single ref template for all connections (source and target)
  # e.g. "sec/{profile}/conn/{type}/{short_name}"
  secret_ref_template: str

  overrides: Dict[str, str]
  security: Dict[str, Any]

  # Dialect used for SQL generation (unless env override)
  default_dialect: str

  # Selector for active target system (references System.short_name with is_target=True)
  default_target_system: Optional[str] = None


def _find_profiles_path(explicit_path: str | None = None) -> Path:
  """
  Locate elevata_profiles.yaml in several common locations.
  """
  # 1. Use explicit path from settings or .env if provided
  candidates = []
  if explicit_path:
    candidates.append(explicit_path)

  # 2. Relative to BASE_DIR or project root
  here = Path(__file__).resolve()
  candidates += [
    here.parents[3] / "config" / "elevata_profiles.yaml",  # e.g. elevata/config
    Path.cwd() / "config" / "elevata_profiles.yaml",
    Path("/etc/elevata/elevata_profiles.yaml"),
  ]

  for c in candidates:
    c = Path(c)
    if c.exists():
      return c

  raise FileNotFoundError("elevata_profiles.yaml not found in expected locations")

def load_profile(profiles_path: Optional[str] = None) -> Profile:
  """Evaluates and returns the current active profile"""
  path = _find_profiles_path(profiles_path)
  with open(path, "r") as f:
    data = yaml.safe_load(f) or {}

  active = os.getenv("ELEVATA_PROFILE", data.get("active_profile", "dev"))
  profiles = data.get("profiles") or {}

  if active not in profiles:
    raise KeyError(
      f"Active profile '{active}' not found in elevata_profiles.yaml. "
      "Check 'active_profile' or ELEVATA_PROFILE."
    )

  p = profiles[active] or {}

  return Profile(
    name=active,
    providers=p.get("providers", []) or [],
    secret_ref_template=p["secret_ref_template"],
    overrides=p.get("overrides", {}) or {},
    security=p.get("security", {}) or {},
    default_dialect=p.get("default_dialect", "duckdb"),
    default_target_system=p.get("default_target_system"),
  )


def render_ref(template: str, profile: Profile, **kwargs) -> str:
  """Render a template like 'sec/{profile}/pepper' with profile + kwargs."""
  return (template or "").format(profile=profile.name, **kwargs)


def apply_overrides(ref: str, profile: Profile) -> str:
  """Apply profile-specific ref overrides if present."""
  return profile.overrides.get(ref, ref)


def build_system_secret_ref(profile: Profile, system: System) -> str:
  tmpl = profile.secret_ref_template
  raw_ref = render_ref(
    tmpl,
    profile,
    type=system.type,
    short_name=system.short_name,
  )
  return apply_overrides(raw_ref, profile)


def get_active_target_system(profile: Profile, name: Optional[str] = None) -> System:
  code = name or profile.default_target_system
  if not code:
    raise KeyError(
      f"Profile '{profile.name}' has no default_target_system configured "
      "and no explicit target name was provided."
    )

  try:
    system = System.objects.get(short_name=code)
  except System.DoesNotExist as exc:
    raise KeyError(
      f"System with short_name='{code}' not found."
    ) from exc

  if not system.is_target:
    raise ValueError(
      f"System '{code}' is not marked as target (is_target=False). "
      "Set is_target=True in the admin if this should act as a target platform."
    )

  return system
