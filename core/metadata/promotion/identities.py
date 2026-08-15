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

from dataclasses import dataclass
from typing import Any, Mapping

from metadata.promotion.canonical import canonical_sha256
from metadata.promotion.contracts import MetadataIdentityContract


@dataclass(frozen=True)
class MetadataObjectIdentity:
  """
  Canonical, local-ID-independent identity of one metadata object.
  """
  model_name: str
  components: tuple[tuple[str, Any], ...]

  @property
  def payload(self) -> dict[str, Any]:
    """
    Return the deterministic identity payload.
    """
    return {
      "model": self.model_name,
      "components": {
        name: value
        for name, value in self.components
      },
    }

  @property
  def fingerprint(self) -> str:
    """
    Return the stable identity fingerprint.
    """
    return canonical_sha256(self.payload)

  @property
  def object_key(self) -> str:
    """
    Return the opaque portable object key.
    """
    return f"{self.model_name}:{self.fingerprint}"


def build_metadata_object_identity(
  *,
  model_name: str,
  contract: MetadataIdentityContract,
  values: Mapping[str, Any],
) -> MetadataObjectIdentity:
  """
  Build one identity from already resolved logical component values.
  """
  candidates = (contract.components,) + contract.fallback_components
  candidate_failures: list[str] = []

  for candidate in candidates:
    missing = [
      component
      for component in candidate
      if component not in values
    ]
    if missing:
      candidate_failures.append(
        f"[{', '.join(candidate)}]: missing {', '.join(missing)}"
      )
      continue

    components: list[tuple[str, Any]] = []
    empty: list[str] = []
    for component in candidate:
      value = values[component]
      allow_empty = component in contract.normalize_empty_components
      if allow_empty:
        value = "" if value is None else value
      if not allow_empty and (value is None or value == ""):
        empty.append(component)
        continue
      components.append((component, value))

    if not empty:
      return MetadataObjectIdentity(
        model_name=model_name,
        components=tuple(components),
      )

    candidate_failures.append(
      f"[{', '.join(candidate)}]: empty {', '.join(empty)}"
    )

  detail = "; ".join(candidate_failures)
  raise ValueError(
    f"Cannot build metadata identity for {model_name}; "
    f"no logical identity candidate is complete ({detail})."
  )
