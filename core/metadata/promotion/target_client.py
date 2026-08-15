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

from json import JSONDecodeError
import json
import socket
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import (
  HTTPRedirectHandler,
  Request,
  build_opener,
)

from metadata.promotion.deployment import (
  EnvironmentPromotionDeploymentPackage,
  serialize_environment_promotion_deployment_package,
)
from metadata.promotion.record import (
  EnvironmentPromotionRecord,
  EnvironmentPromotionRecordError,
  deserialize_environment_promotion_record,
)
from metadata.promotion.snapshot import (
  EnvironmentMetadataSnapshot,
  EnvironmentMetadataSnapshotError,
  deserialize_environment_metadata_snapshot,
)
from metadata.promotion.target_registry import EnvironmentPromotionTarget


MAX_TARGET_RESPONSE_BYTES = 64 * 1024 * 1024


class EnvironmentPromotionTargetClientError(RuntimeError):
  """Raised when a configured Promotion Target cannot satisfy its contract."""


class _NoRedirectHandler(HTTPRedirectHandler):
  """Prevent bearer credentials from being forwarded through HTTP redirects."""

  def redirect_request(self, req, fp, code, msg, headers, newurl):
    return None


class EnvironmentPromotionTargetClient:
  """Server-side client for one headless Promotion Target Runner."""

  def __init__(self, target: EnvironmentPromotionTarget, *, opener=None) -> None:
    self.target = target
    self.opener = opener or build_opener(_NoRedirectHandler())

  def health(self) -> dict[str, Any]:
    """Return and validate the runner health contract."""
    payload = self._request_json("promotion-runner/health/")
    if payload.get("status") != "ok":
      raise EnvironmentPromotionTargetClientError(
        f"Promotion target {self.target.environment_label} is not healthy."
      )
    if payload.get("runtime_mode") != "promotion_target":
      raise EnvironmentPromotionTargetClientError(
        f"Promotion target {self.target.environment_label} is not running in "
        "promotion_target mode."
      )
    if payload.get("environment_label") != self.target.environment_label:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target environment identity mismatch: expected "
        f"{self.target.environment_label}, received "
        f"{payload.get('environment_label') or '<empty>'}."
      )
    if payload.get("metadata_database") != "reachable":
      raise EnvironmentPromotionTargetClientError(
        f"Promotion target {self.target.environment_label} metadata database "
        "is not reachable."
      )
    return payload

  def snapshot(self, *, actor: str) -> EnvironmentMetadataSnapshot:
    """Fetch and validate the current portable target snapshot."""
    payload = self._request_text(
      "promotion-runner/snapshot/",
      headers={"X-Elevata-Actor": _require_actor(actor)},
    )
    try:
      snapshot = deserialize_environment_metadata_snapshot(payload)
    except EnvironmentMetadataSnapshotError as exc:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target returned an invalid metadata snapshot."
      ) from exc
    if snapshot.environment_label != self.target.environment_label:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target snapshot environment mismatch: expected "
        f"{self.target.environment_label}, received {snapshot.environment_label}."
      )
    return snapshot

  def history(self) -> dict[str, Any]:
    """Fetch compact immutable promotion history from the target runner."""
    payload = self._request_json("promotion-runner/history/")
    if payload.get("target_environment_label") != self.target.environment_label:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target history environment identity mismatch."
      )
    records = payload.get("records")
    if not isinstance(records, list):
      raise EnvironmentPromotionTargetClientError(
        "Promotion target history response is invalid."
      )
    return payload

  def check(
    self,
    *,
    package: EnvironmentPromotionDeploymentPackage,
    actor: str,
  ) -> dict[str, Any]:
    """Run the runner's exact live-target drift check for one package."""
    payload = self._request_json(
      "promotion-runner/check/",
      method="POST",
      body=serialize_environment_promotion_deployment_package(package).encode(
        "utf-8"
      ),
      headers={
        "Content-Type": "application/json; charset=utf-8",
        "X-Elevata-Actor": _require_actor(actor),
      },
      accepted_http_errors=(409,),
    )
    if payload.get("package_id") != package.package_id:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target drift-check package identity mismatch."
      )
    if payload.get("target_environment_label") != self.target.environment_label:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target drift-check environment identity mismatch."
      )
    status = payload.get("status")
    if status not in {"unchanged", "drift"}:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target drift-check response has an unsupported status."
      )
    if payload.get("is_unchanged") is not (status == "unchanged"):
      raise EnvironmentPromotionTargetClientError(
        "Promotion target drift-check response is internally inconsistent."
      )
    return payload

  def apply(
    self,
    *,
    package: EnvironmentPromotionDeploymentPackage,
    confirm_package_id: str,
    actor: str,
  ) -> EnvironmentPromotionRecord:
    """Apply one exact approved package through the target runner."""
    confirmation = str(confirm_package_id or "").strip()
    if confirmation != package.package_id:
      raise EnvironmentPromotionTargetClientError(
        "Deployment package confirmation does not match the package ID."
      )
    payload = self._request_text(
      "promotion-runner/apply/",
      method="POST",
      body=serialize_environment_promotion_deployment_package(package).encode(
        "utf-8"
      ),
      headers={
        "Content-Type": "application/json; charset=utf-8",
        "X-Elevata-Confirm-Package-ID": confirmation,
        "X-Elevata-Applied-By": _require_actor(actor),
      },
    )
    try:
      record = deserialize_environment_promotion_record(payload)
    except EnvironmentPromotionRecordError as exc:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target returned an invalid promotion record."
      ) from exc

    expected_bindings = {
      "plan ID": (record.plan_id, package.plan.plan_id),
      "plan fingerprint": (
        record.plan_fingerprint,
        package.plan.plan_fingerprint,
      ),
      "approval ID": (record.approval_id, package.approval.approval_id),
      "approval fingerprint": (
        record.approval_fingerprint,
        package.approval.artifact_fingerprint,
      ),
      "release ID": (record.release_id, package.bundle.release_id),
      "bundle fingerprint": (
        record.bundle_fingerprint,
        package.bundle.bundle_fingerprint,
      ),
      "target environment": (
        record.target_environment_label,
        self.target.environment_label,
      ),
      "post-apply metadata fingerprint": (
        record.post_target_metadata_fingerprint,
        package.bundle.metadata_fingerprint,
      ),
    }
    mismatches = [
      label
      for label, (actual, expected) in expected_bindings.items()
      if actual != expected
    ]
    if mismatches:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target apply record does not match the approved package: "
        + ", ".join(sorted(mismatches))
        + "."
      )
    if record.summary["action_count"] != len(package.plan.actions):
      raise EnvironmentPromotionTargetClientError(
        "Promotion target apply record action count does not match the approved plan."
      )
    return record


  def _request_json(
    self,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    method: str = "GET",
    body: bytes | None = None,
    accepted_http_errors: tuple[int, ...] = (),
  ) -> dict[str, Any]:
    payload = self._request_text(
      path,
      headers=headers,
      method=method,
      body=body,
      accepted_http_errors=accepted_http_errors,
    )
    try:
      data = json.loads(payload)
    except JSONDecodeError as exc:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target returned invalid JSON."
      ) from exc
    if not isinstance(data, dict):
      raise EnvironmentPromotionTargetClientError(
        "Promotion target JSON response must be an object."
      )
    return data

  def _request_text(
    self,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    method: str = "GET",
    body: bytes | None = None,
    accepted_http_errors: tuple[int, ...] = (),
  ) -> str:
    request_headers = {
      "Accept": "application/json",
      "Authorization": f"Bearer {self.target.bearer_token}",
      "User-Agent": "elevata-environment-promotion",
    }
    request_headers.update(headers or {})
    request = Request(
      self._url(path),
      data=body,
      headers=request_headers,
      method=method,
    )
    try:
      with self.opener.open(
        request,
        timeout=self.target.timeout_seconds,
      ) as response:
        payload = response.read(MAX_TARGET_RESPONSE_BYTES + 1)
    except HTTPError as exc:
      if exc.code in accepted_http_errors:
        payload = exc.read(MAX_TARGET_RESPONSE_BYTES + 1)
      else:
        detail = _http_error_detail(exc)
        raise EnvironmentPromotionTargetClientError(
          f"Promotion target {self.target.environment_label} rejected the request "
          f"with HTTP {exc.code}{detail}."
        ) from exc
    except (URLError, TimeoutError, socket.timeout, OSError) as exc:
      raise EnvironmentPromotionTargetClientError(
        f"Promotion target {self.target.environment_label} is unreachable: {exc}."
      ) from exc
    if len(payload) > MAX_TARGET_RESPONSE_BYTES:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target response exceeds the maximum accepted size."
      )
    try:
      return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
      raise EnvironmentPromotionTargetClientError(
        "Promotion target response must be UTF-8."
      ) from exc

  def _url(self, path: str) -> str:
    base = self.target.base_url.rstrip("/") + "/"
    return urljoin(base, path.lstrip("/"))


def _require_actor(value: str) -> str:
  actor = str(value or "").strip()
  return actor or "environment-promotion-ui"


def _http_error_detail(exc: HTTPError) -> str:
  try:
    raw = exc.read(8192)
    data = json.loads(raw.decode("utf-8"))
  except (OSError, UnicodeDecodeError, JSONDecodeError, AttributeError):
    return ""
  if not isinstance(data, dict):
    return ""
  message = str(data.get("message") or "").strip()
  return f": {message}" if message else ""
