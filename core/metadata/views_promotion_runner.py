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

import hmac

from django.conf import settings
from django.core.exceptions import RequestDataTooBig
from django.db import DatabaseError, connection
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from metadata.promotion.apply import (
  EnvironmentPromotionApplyError,
  EnvironmentPromotionDriftError,
)
from metadata.promotion.deployment import (
  EnvironmentPromotionDeploymentPackageError,
  deserialize_environment_promotion_deployment_package,
)
from metadata.promotion.record import serialize_environment_promotion_record
from metadata.promotion.record_store import (
  EnvironmentPromotionRecordStore,
  EnvironmentPromotionRecordStoreError,
)
from metadata.promotion.runner import (
  EnvironmentPromotionRunnerError,
  apply_environment_promotion_package,
  build_runner_snapshot,
  check_environment_promotion_target,
)
from metadata.promotion.snapshot import serialize_environment_metadata_snapshot
from metadata.promotion.snapshot_builder import EnvironmentMetadataSnapshotBuildError


@csrf_exempt
@require_GET
def promotion_runner_health(request: HttpRequest) -> HttpResponse:
  denied = _guard_runner_request(request)
  if denied is not None:
    return denied
  try:
    with connection.cursor() as cursor:
      cursor.execute("SELECT 1")
      cursor.fetchone()
  except DatabaseError:
    return _error_response(
      "metadata_database_unavailable",
      "The target metadata database is not reachable.",
      status=503,
    )
  return _json_response({
    "status": "ok",
    "runtime_mode": "promotion_target",
    "environment_label": settings.ELEVATA_ENVIRONMENT,
    "elevata_version": settings.ELEVATA_VERSION,
    "metadata_database": "reachable",
  })


@csrf_exempt
@require_GET
def promotion_runner_snapshot(request: HttpRequest) -> HttpResponse:
  denied = _guard_runner_request(request)
  if denied is not None:
    return denied
  actor = _audit_actor(request, default="promotion-runner-snapshot")
  try:
    snapshot = build_runner_snapshot(
      runtime_environment_label=settings.ELEVATA_ENVIRONMENT,
      created_by=actor,
    )
  except (EnvironmentPromotionRunnerError, EnvironmentMetadataSnapshotBuildError) as exc:
    return _error_response("snapshot_failed", str(exc), status=500)
  return _artifact_response(serialize_environment_metadata_snapshot(snapshot))


@csrf_exempt
@require_POST
def promotion_runner_check(request: HttpRequest) -> HttpResponse:
  denied = _guard_runner_request(request)
  if denied is not None:
    return denied
  try:
    package = _deployment_package_from_request(request)
    result = check_environment_promotion_target(
      package=package,
      runtime_environment_label=settings.ELEVATA_ENVIRONMENT,
      created_by=_audit_actor(request, default="promotion-runner-check"),
    )
  except (
    EnvironmentPromotionDeploymentPackageError,
    EnvironmentPromotionRunnerError,
    EnvironmentMetadataSnapshotBuildError,
  ) as exc:
    return _error_response("invalid_package", str(exc), status=400)
  return _json_response(result.to_dict(), status=200 if result.is_unchanged else 409)


@csrf_exempt
@require_POST
def promotion_runner_apply(request: HttpRequest) -> HttpResponse:
  denied = _guard_runner_request(request)
  if denied is not None:
    return denied
  try:
    package = _deployment_package_from_request(request)
    confirmation = _required_header(request, "X-Elevata-Confirm-Package-ID")
    applied_by = _required_header(request, "X-Elevata-Applied-By")
    record = apply_environment_promotion_package(
      package=package,
      runtime_environment_label=settings.ELEVATA_ENVIRONMENT,
      confirm_package_id=confirmation,
      applied_by=applied_by,
    )
  except EnvironmentPromotionDriftError as exc:
    return _error_response("target_drift", str(exc), status=409)
  except (
    EnvironmentPromotionDeploymentPackageError,
    EnvironmentPromotionRunnerError,
    EnvironmentPromotionApplyError,
    EnvironmentPromotionRecordStoreError,
  ) as exc:
    return _error_response("apply_rejected", str(exc), status=400)
  response = _artifact_response(serialize_environment_promotion_record(record), status=201)
  response["X-Elevata-Promotion-Record-ID"] = record.record_id
  return response


@csrf_exempt
@require_GET
def promotion_runner_history(request: HttpRequest) -> HttpResponse:
  denied = _guard_runner_request(request)
  if denied is not None:
    return denied
  try:
    records = EnvironmentPromotionRecordStore().load_all(
      target_environment_label=settings.ELEVATA_ENVIRONMENT
    )
  except EnvironmentPromotionRecordStoreError as exc:
    return _error_response("history_failed", str(exc), status=500)
  return _json_response({
    "target_environment_label": settings.ELEVATA_ENVIRONMENT,
    "records": [
      {
        "record_id": record.record_id,
        "record_fingerprint": record.record_fingerprint,
        "release_id": record.release_id,
        "plan_id": record.plan_id,
        "approval_id": record.approval_id,
        "source_environment_label": record.source_environment_label,
        "target_environment_label": record.target_environment_label,
        "applied_at": record.applied_at.isoformat(),
        "applied_by": record.applied_by,
        "post_target_metadata_fingerprint": record.post_target_metadata_fingerprint,
        "summary": record.summary,
      }
      for record in reversed(records)
    ],
  })


@csrf_exempt
@require_GET
def promotion_runner_history_detail(
  request: HttpRequest,
  record_id: str,
) -> HttpResponse:
  denied = _guard_runner_request(request)
  if denied is not None:
    return denied
  try:
    record = EnvironmentPromotionRecordStore().load(
      target_environment_label=settings.ELEVATA_ENVIRONMENT,
      record_id=record_id,
    )
  except EnvironmentPromotionRecordStoreError as exc:
    return _error_response("history_failed", str(exc), status=400)
  if record is None:
    return _error_response("record_not_found", "Promotion record was not found.", status=404)
  return _artifact_response(serialize_environment_promotion_record(record))


def _guard_runner_request(request: HttpRequest) -> HttpResponse | None:
  if getattr(settings, "ELEVATA_RUNTIME_MODE", "authoring") != "promotion_target":
    return _error_response("not_found", "Promotion Runner is not enabled.", status=404)
  expected = str(getattr(settings, "ELEVATA_PROMOTION_RUNNER_TOKEN", "") or "")
  provided = request.headers.get("Authorization", "")
  prefix = "Bearer "
  if not provided.startswith(prefix):
    return _unauthorized()
  actual = provided[len(prefix):]
  if not expected or not hmac.compare_digest(actual, expected):
    return _unauthorized()
  return None


def _deployment_package_from_request(request: HttpRequest):
  max_bytes = int(getattr(settings, "ELEVATA_PROMOTION_RUNNER_MAX_PACKAGE_BYTES", 0) or 0)
  content_length = request.META.get("CONTENT_LENGTH")
  if content_length:
    try:
      if max_bytes and int(content_length) > max_bytes:
        raise EnvironmentPromotionRunnerError("Deployment package exceeds the runner size limit.")
    except ValueError as exc:
      raise EnvironmentPromotionRunnerError("Invalid Content-Length header.") from exc
  try:
    payload = request.body
  except RequestDataTooBig as exc:
    raise EnvironmentPromotionRunnerError(
      "Deployment package exceeds the runner size limit."
    ) from exc
  if max_bytes and len(payload) > max_bytes:
    raise EnvironmentPromotionRunnerError("Deployment package exceeds the runner size limit.")
  if not payload:
    raise EnvironmentPromotionRunnerError("Deployment package request body is required.")
  try:
    text = payload.decode("utf-8")
  except UnicodeDecodeError as exc:
    raise EnvironmentPromotionRunnerError("Deployment package must be UTF-8 JSON.") from exc
  return deserialize_environment_promotion_deployment_package(text)


def _audit_actor(request: HttpRequest, *, default: str) -> str:
  return str(request.headers.get("X-Elevata-Actor") or default).strip() or default


def _required_header(request: HttpRequest, name: str) -> str:
  value = str(request.headers.get(name) or "").strip()
  if not value:
    raise EnvironmentPromotionRunnerError(f"{name} header is required.")
  return value


def _artifact_response(payload: str, *, status: int = 200) -> HttpResponse:
  response = HttpResponse(payload, status=status, content_type="application/json; charset=utf-8")
  response["Cache-Control"] = "no-store"
  return response


def _json_response(payload: dict, *, status: int = 200) -> JsonResponse:
  response = JsonResponse(payload, status=status, json_dumps_params={"sort_keys": True})
  response["Cache-Control"] = "no-store"
  return response


def _error_response(code: str, message: str, *, status: int) -> JsonResponse:
  return _json_response({"error": code, "message": message}, status=status)


def _unauthorized() -> JsonResponse:
  response = _error_response(
    "unauthorized",
    "A valid Promotion Runner bearer token is required.",
    status=401,
  )
  response["WWW-Authenticate"] = "Bearer"
  return response
