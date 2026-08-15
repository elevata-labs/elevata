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

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import re
from typing import Any

from metadata.generation.hashing import RUNTIME_PEPPER_TOKEN
from metadata.promotion.model_contracts import (
  REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS,
)
from metadata.promotion.release import ArchitectureReleaseBundle


_ENV_REFERENCE_RE = re.compile(r"^\$\{[A-Z][A-Z0-9_]*\}$")
_ENV_REFERENCE_TOKEN_RE = re.compile(r"\$\{[A-Z][A-Z0-9_]*\}")
_CREDENTIAL_URL_RE = re.compile(
  r"^[a-z][a-z0-9+.-]*://[^/@\s:]+:[^/@\s]+@",
  re.IGNORECASE,
)
_WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"^[A-Za-z]:[\\/]")

_ALWAYS_FORBIDDEN_CONFIG_KEYS = frozenset({
  "profile",
  "profile_name",
  "runtime_profile",
  "provider",
  "provider_name",
  "secret_provider",
  "target_system",
  "target_system_short",
  "env_file",
  "dotenv_path",
})
_NESTED_SYMBOLIC_CONFIG_KEYS = frozenset({
  "credential",
  "credentials",
})
_SYMBOLIC_VALUE_CONFIG_KEYS = frozenset({
  "password",
  "passwd",
  "secret",
  "client_secret",
  "token",
  "access_token",
  "refresh_token",
  "api_key",
  "apikey",
  "sas_token",
  "private_key",
  "pepper",
  "pepper_value",
  "client_id",
  "tenant_id",
  "connection_string",
  "connection_url",
  "connection_uri",
  "database_url",
  "dsn",
  "host",
  "hostname",
  "port",
  "username",
  "user",
  "base_url",
  "account_url",
})
# Only keys with unambiguous local-filesystem semantics belong here.
# Generic "path" is intentionally excluded because REST connectors use it for
# root-relative resource paths such as "/posts".
_PATH_CONFIG_KEYS = frozenset({
  "file_path",
  "directory",
  "folder",
  "root_path",
  "base_path",
  "local_path",
  "archive_path",
  "download_path",
  "upload_path",
})


class ArchitectureReleaseValidationError(ValueError):
  """
  Raised when a release bundle contains non-portable metadata.
  """


@dataclass(frozen=True)
class ArchitectureReleaseValidationIssue:
  """
  One deterministic release validation finding.
  """
  code: str
  message: str
  object_key: str | None = None
  field_path: str | None = None

  def to_dict(self) -> dict[str, str | None]:
    return {
      "code": self.code,
      "message": self.message,
      "object_key": self.object_key,
      "field_path": self.field_path,
    }


@dataclass(frozen=True)
class ArchitectureReleaseValidationResult:
  """
  Complete validation outcome for one release bundle.
  """
  errors: tuple[ArchitectureReleaseValidationIssue, ...] = ()
  warnings: tuple[ArchitectureReleaseValidationIssue, ...] = ()

  @property
  def is_valid(self) -> bool:
    return not self.errors

  def to_dict(self) -> dict[str, Any]:
    return {
      "is_valid": self.is_valid,
      "errors": [item.to_dict() for item in self.errors],
      "warnings": [item.to_dict() for item in self.warnings],
    }


def validate_architecture_release_bundle(
  bundle: ArchitectureReleaseBundle,
) -> ArchitectureReleaseValidationResult:
  """
  Validate portability and system-schema integrity of a release bundle.
  """
  errors = []
  errors.extend(_validate_target_schema_registry(bundle))
  errors.extend(_validate_source_ingestion_configs(bundle))
  errors.extend(_validate_surrogate_expression_runtime_bindings(bundle))
  ordered_errors = tuple(sorted(
    errors,
    key=lambda item: (
      item.code,
      item.object_key or "",
      item.field_path or "",
      item.message,
    ),
  ))
  return ArchitectureReleaseValidationResult(errors=ordered_errors)


def require_valid_architecture_release_bundle(
  bundle: ArchitectureReleaseBundle,
) -> ArchitectureReleaseValidationResult:
  """
  Validate a bundle and raise when it is not portable.
  """
  result = validate_architecture_release_bundle(bundle)
  if result.is_valid:
    return result
  details = "; ".join(
    f"{item.code}: {item.message}"
    for item in result.errors
  )
  raise ArchitectureReleaseValidationError(
    "Architecture Release Bundle validation failed: " + details
  )


def _validate_target_schema_registry(
  bundle: ArchitectureReleaseBundle,
) -> list[ArchitectureReleaseValidationIssue]:
  schema_objects = [
    item
    for item in bundle.snapshot.metadata.objects
    if item.model_name == "TargetSchema"
  ]
  by_short_name = {
    str(item.fields.get("short_name") or ""): item
    for item in schema_objects
  }
  issues = []

  for short_name in sorted(REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS):
    item = by_short_name.get(short_name)
    if item is None:
      issues.append(ArchitectureReleaseValidationIssue(
        code="required_target_schema_missing",
        message=(
          f"Required system-managed TargetSchema {short_name!r} is missing."
        ),
        field_path="TargetSchema.short_name",
      ))
      continue
    if item.fields.get("is_system_managed") is not True:
      issues.append(ArchitectureReleaseValidationIssue(
        code="required_target_schema_not_system_managed",
        message=(
          f"Required TargetSchema {short_name!r} must be system-managed."
        ),
        object_key=item.object_key,
        field_path="TargetSchema.is_system_managed",
      ))

  for item in schema_objects:
    short_name = str(item.fields.get("short_name") or "")
    if (
      item.fields.get("is_system_managed") is True
      and short_name not in REQUIRED_SYSTEM_MANAGED_TARGET_SCHEMA_KEYS
    ):
      issues.append(ArchitectureReleaseValidationIssue(
        code="unknown_system_managed_target_schema",
        message=(
          f"TargetSchema {short_name!r} is marked system-managed but is not "
          "part of the application registry."
        ),
        object_key=item.object_key,
        field_path="TargetSchema.is_system_managed",
      ))

  return issues


def _validate_surrogate_expression_runtime_bindings(
  bundle: ArchitectureReleaseBundle,
) -> list[ArchitectureReleaseValidationIssue]:
  """
  Require portable SK/FK expressions to bind the pepper symbolically.

  Concrete runtime peppers are environment-local secrets. Generated surrogate
  and foreign-key expressions therefore carry exactly one {runtime:pepper}
  token in portable metadata and resolve it only during runtime rendering.
  """
  issues = []
  for item in bundle.snapshot.metadata.objects:
    if item.model_name != "TargetColumn":
      continue

    role = str(item.fields.get("system_role") or "").strip()
    if role not in {"surrogate_key", "foreign_key"}:
      continue

    expression = item.fields.get("surrogate_expression")
    if not isinstance(expression, str) or not expression.strip():
      continue

    if expression.count(RUNTIME_PEPPER_TOKEN) == 1:
      continue

    issues.append(ArchitectureReleaseValidationIssue(
      code="runtime_pepper_not_symbolic_in_surrogate_expression",
      message=(
        "TargetColumn surrogate_expression for surrogate/foreign keys must "
        f"contain exactly one {RUNTIME_PEPPER_TOKEN} runtime binding and must "
        "not embed an environment-specific pepper value."
      ),
      object_key=item.object_key,
      field_path="surrogate_expression",
    ))
  return issues


def _validate_source_ingestion_configs(
  bundle: ArchitectureReleaseBundle,
) -> list[ArchitectureReleaseValidationIssue]:
  issues = []
  for item in bundle.snapshot.metadata.objects:
    if item.model_name != "SourceDataset":
      continue
    config = item.fields.get("ingestion_config")
    if config is None:
      continue
    if not isinstance(config, Mapping):
      issues.append(ArchitectureReleaseValidationIssue(
        code="ingestion_config_not_object",
        message="SourceDataset.ingestion_config must be a JSON object.",
        object_key=item.object_key,
        field_path="ingestion_config",
      ))
      continue
    issues.extend(_inspect_config_value(
      value=config,
      object_key=item.object_key,
      field_path="ingestion_config",
      parent_key=None,
    ))
  return issues


def _inspect_config_value(
  *,
  value: Any,
  object_key: str,
  field_path: str,
  parent_key: str | None,
) -> list[ArchitectureReleaseValidationIssue]:
  issues = []
  normalized_parent_key = _normalize_key(parent_key) if parent_key else None

  if normalized_parent_key in _ALWAYS_FORBIDDEN_CONFIG_KEYS:
    return [ArchitectureReleaseValidationIssue(
      code="runtime_binding_in_ingestion_config",
      message=(
        f"Runtime binding key {parent_key!r} must not be stored in portable "
        "ingestion metadata."
      ),
      object_key=object_key,
      field_path=field_path,
    )]

  if normalized_parent_key in _NESTED_SYMBOLIC_CONFIG_KEYS:
    if not _is_symbolic_structure(value):
      return [ArchitectureReleaseValidationIssue(
        code="concrete_runtime_value_in_ingestion_config",
        message=(
          f"Runtime or credential value {parent_key!r} must contain only "
          "exact ${ENV_VAR} references."
        ),
        object_key=object_key,
        field_path=field_path,
      )]
    return []

  if normalized_parent_key in _SYMBOLIC_VALUE_CONFIG_KEYS:
    if not _is_empty_or_symbolic(value):
      return [ArchitectureReleaseValidationIssue(
        code="concrete_runtime_value_in_ingestion_config",
        message=(
          f"Runtime or credential value {parent_key!r} must use one exact "
          "${ENV_VAR} reference."
        ),
        object_key=object_key,
        field_path=field_path,
      )]
    return []

  if isinstance(value, Mapping):
    for key in sorted(value, key=lambda item: str(item)):
      key_text = str(key)
      issues.extend(_inspect_config_value(
        value=value[key],
        object_key=object_key,
        field_path=f"{field_path}.{key_text}",
        parent_key=key_text,
      ))
    return issues

  if isinstance(value, Sequence) and not isinstance(
    value,
    (str, bytes, bytearray),
  ):
    for index, item in enumerate(value):
      issues.extend(_inspect_config_value(
        value=item,
        object_key=object_key,
        field_path=f"{field_path}[{index}]",
        parent_key=parent_key,
      ))
    return issues

  if not isinstance(value, str):
    return issues

  if "-----BEGIN" in value and "PRIVATE KEY-----" in value:
    issues.append(ArchitectureReleaseValidationIssue(
      code="embedded_private_key",
      message="Private key material must not be stored in portable metadata.",
      object_key=object_key,
      field_path=field_path,
    ))
  if _CREDENTIAL_URL_RE.match(value.strip()):
    issues.append(ArchitectureReleaseValidationIssue(
      code="credential_url_in_ingestion_config",
      message="Connection URLs with embedded credentials are not portable.",
      object_key=object_key,
      field_path=field_path,
    ))
  if "${" in value and not _has_valid_environment_references(value):
    issues.append(ArchitectureReleaseValidationIssue(
      code="invalid_environment_reference",
      message="Environment references must use ${UPPER_SNAKE_CASE} syntax.",
      object_key=object_key,
      field_path=field_path,
    ))
  if (
    normalized_parent_key in _PATH_CONFIG_KEYS
    and not _ENV_REFERENCE_RE.fullmatch(value.strip())
    and _is_absolute_local_path(value)
  ):
    issues.append(ArchitectureReleaseValidationIssue(
      code="absolute_local_path_in_ingestion_config",
      message="Absolute local paths are not portable between environments.",
      object_key=object_key,
      field_path=field_path,
    ))
  return issues


def _normalize_key(value: str | None) -> str:
  if value is None:
    return ""
  return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _is_empty_or_symbolic(value: Any) -> bool:
  if value is None:
    return True
  if not isinstance(value, str):
    return False
  text = value.strip()
  return not text or bool(_ENV_REFERENCE_RE.fullmatch(text))


def _is_symbolic_structure(value: Any) -> bool:
  if isinstance(value, Mapping):
    return all(_is_symbolic_structure(item) for item in value.values())
  if isinstance(value, Sequence) and not isinstance(
    value,
    (str, bytes, bytearray),
  ):
    return all(_is_symbolic_structure(item) for item in value)
  return _is_empty_or_symbolic(value)


def _has_valid_environment_references(value: str) -> bool:
  remaining = _ENV_REFERENCE_TOKEN_RE.sub("", value)
  return "${" not in remaining


def _is_absolute_local_path(value: str) -> bool:
  text = value.strip()
  return (
    text.startswith("/")
    or text.startswith("\\\\")
    or bool(_WINDOWS_ABSOLUTE_PATH_RE.match(text))
  )
