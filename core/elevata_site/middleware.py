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
from django.shortcuts import redirect
from django.conf import settings

ALLOWLIST = (
  "/accounts/login/",
  "/accounts/logout/",
  "/accounts/password_reset/",
  "/accounts/reset/",
  "/static/",
)

class LoginRequiredAllMiddleware:
  def __init__(self, get_response):
    self.get_response = get_response

  def __call__(self, request):
    path = request.path
    if (not request.user.is_authenticated
        and not any(path.startswith(p) for p in ALLOWLIST)):
      return redirect(f"{settings.LOGIN_URL}?next={path}")
    return self.get_response(request)
