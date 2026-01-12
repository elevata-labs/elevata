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

from django import template
from metadata.constants import classify_type, TYPE_SUPPORT_LABEL, TYPE_BADGE_CLASS

register = template.Library()


@register.filter
def get(d, key):
  """Dict item lookup: {{ dict|get:key }}."""
  try:
    return d.get(key)
  except Exception:
    return None

@register.filter
def support_level(type_code: str) -> str:
  """Returns 'auto' | 'beta' | 'manual'."""
  return classify_type((type_code or "").lower())

@register.filter
def support_badge_class(level: str) -> str:
  """Bootstrap class for a given level."""
  return TYPE_BADGE_CLASS.get(level, "bg-secondary")

@register.filter
def support_label(level: str) -> str:
  """Human label for a given level."""
  return TYPE_SUPPORT_LABEL.get(level, "Manual only")

@register.filter
def is_import_supported(type_code: str) -> bool:
  """True if level is auto or beta."""
  lvl = classify_type((type_code or "").lower())
  return lvl in ("auto", "beta")
