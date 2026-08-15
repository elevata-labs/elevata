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

from dataclasses import dataclass, field
import os
import re
from urllib.parse import urlsplit, urlunsplit


TARGET_LIST_ENV = "ELEVATA_PROMOTION_TARGETS"
TARGET_TIMEOUT_ENV = "ELEVATA_PROMOTION_TARGET_TIMEOUT_SECONDS"
DEFAULT_TARGET_TIMEOUT_SECONDS = 30
_TARGET_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class EnvironmentPromotionTargetRegistryError(ValueError):
  """Raised when authoring-side Promotion Target configuration is invalid."""


@dataclass(frozen=True)
class EnvironmentPromotionTarget:
  """One configured headless Promotion Target Runner."""

  environment_label: str
  base_url: str
  bearer_token: str = field(repr=False)
  timeout_seconds: int = DEFAULT_TARGET_TIMEOUT_SECONDS

  @property
  def env_suffix(self) -> str:
    """Return the deterministic environment-variable suffix for this target."""
    return _target_env_suffix(self.environment_label)


def load_environment_promotion_targets() -> tuple[EnvironmentPromotionTarget, ...]:
  """Load the explicit authoring-side Promotion Target registry from env vars."""
  raw_labels = os.getenv(TARGET_LIST_ENV, "")
  labels = tuple(
    item.strip()
    for item in raw_labels.split(",")
    if item.strip()
  )
  if not labels:
    return ()

  timeout_seconds = _target_timeout_seconds()
  seen_labels: set[str] = set()
  suffix_to_label: dict[str, str] = {}
  targets = []

  for label in labels:
    if not _TARGET_LABEL_RE.fullmatch(label):
      raise EnvironmentPromotionTargetRegistryError(
        f"Promotion target label is invalid: {label}."
      )
    if label in seen_labels:
      raise EnvironmentPromotionTargetRegistryError(
        f"Promotion target label is configured more than once: {label}."
      )
    seen_labels.add(label)

    suffix = _target_env_suffix(label)
    existing_label = suffix_to_label.get(suffix)
    if existing_label is not None and existing_label != label:
      raise EnvironmentPromotionTargetRegistryError(
        "Promotion target labels map to the same environment-variable suffix: "
        f"{existing_label}, {label}."
      )
    suffix_to_label[suffix] = label

    url_env = f"ELEVATA_PROMOTION_TARGET_{suffix}_URL"
    token_env = f"ELEVATA_PROMOTION_TARGET_{suffix}_TOKEN"
    base_url = _normalize_target_url(os.getenv(url_env, ""), env_name=url_env)
    token = str(os.getenv(token_env, "") or "").strip()
    if len(token) < 32:
      raise EnvironmentPromotionTargetRegistryError(
        f"{token_env} must contain at least 32 characters."
      )

    targets.append(EnvironmentPromotionTarget(
      environment_label=label,
      base_url=base_url,
      bearer_token=token,
      timeout_seconds=timeout_seconds,
    ))

  return tuple(targets)


def get_environment_promotion_target(
  environment_label: str,
  *,
  targets: tuple[EnvironmentPromotionTarget, ...] | None = None,
) -> EnvironmentPromotionTarget:
  """Resolve one exact configured Promotion Target by environment label."""
  label = str(environment_label or "").strip()
  for target in targets if targets is not None else load_environment_promotion_targets():
    if target.environment_label == label:
      return target
  raise EnvironmentPromotionTargetRegistryError(
    f"Promotion target is not configured: {label or '<empty>'}."
  )


def _target_env_suffix(environment_label: str) -> str:
  return re.sub(r"[^A-Za-z0-9]", "_", environment_label).upper()


def _target_timeout_seconds() -> int:
  raw_value = str(os.getenv(TARGET_TIMEOUT_ENV, "") or "").strip()
  if not raw_value:
    return DEFAULT_TARGET_TIMEOUT_SECONDS
  try:
    value = int(raw_value)
  except ValueError as exc:
    raise EnvironmentPromotionTargetRegistryError(
      f"{TARGET_TIMEOUT_ENV} must be an integer."
    ) from exc
  if value < 1 or value > 600:
    raise EnvironmentPromotionTargetRegistryError(
      f"{TARGET_TIMEOUT_ENV} must be between 1 and 600 seconds."
    )
  return value


def _normalize_target_url(value: str, *, env_name: str) -> str:
  raw_url = str(value or "").strip()
  if not raw_url:
    raise EnvironmentPromotionTargetRegistryError(f"{env_name} is required.")
  parsed = urlsplit(raw_url)
  if parsed.scheme not in {"http", "https"} or not parsed.hostname:
    raise EnvironmentPromotionTargetRegistryError(
      f"{env_name} must be an absolute http or https URL."
    )
  if parsed.username or parsed.password:
    raise EnvironmentPromotionTargetRegistryError(
      f"{env_name} must not contain embedded credentials."
    )
  if parsed.query or parsed.fragment:
    raise EnvironmentPromotionTargetRegistryError(
      f"{env_name} must not contain a query string or fragment."
    )
  path = parsed.path.rstrip("/")
  return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
