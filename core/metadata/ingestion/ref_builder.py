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

from typing import Optional
from metadata.config.profiles import load_profile, render_ref, apply_overrides

def build_secret_ref(*, profiles_path: str, template: Optional[str] = None, **kwargs) -> str:
  """
  Build a secret reference using the active profile.
  - If 'template' is not provided, use profile.secret_ref_template.
  - Applies profile overrides before returning.
  """
  profile = load_profile(profiles_path)
  tpl = template or profile.secret_ref_template or "sec/{profile}/conn/{type}/{short_name}"
  ref = render_ref(tpl, profile, **kwargs)
  return apply_overrides(ref, profile)

def build_pepper_ref(*, profiles_path: str) -> str:
  """Evaluates the pepper reference and applies overrides"""
  profile = load_profile(profiles_path)
  tpl = profile.security.get("pepper_ref") or "sec/{profile}/pepper"
  ref = render_ref(tpl, profile)
  return apply_overrides(ref, profile)