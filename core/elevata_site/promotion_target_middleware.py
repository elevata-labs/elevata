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

from django.conf import settings
from django.http import JsonResponse
from django.urls import Resolver404, resolve


class PromotionTargetOnlyMiddleware:
  """
  Expose only Promotion Runner endpoints in promotion-target mode.

  The target runtime deliberately has no modeling, catalog, Architecture
  Control, admin or account UI surface. Authentication for the remaining
  machine endpoints is handled by the runner's bearer-token guard.
  """

  def __init__(self, get_response):
    self.get_response = get_response

  def __call__(self, request):
    if getattr(settings, "ELEVATA_RUNTIME_MODE", "authoring") != "promotion_target":
      return self.get_response(request)

    try:
      match = resolve(request.path_info)
    except Resolver404:
      return _not_found()

    if not str(match.url_name or "").startswith("promotion_runner_"):
      return _not_found()

    return self.get_response(request)


def _not_found() -> JsonResponse:
  response = JsonResponse(
    {
      "error": "not_found",
      "message": "This promotion target exposes runner endpoints only.",
    },
    status=404,
  )
  response["Cache-Control"] = "no-store"
  return response
