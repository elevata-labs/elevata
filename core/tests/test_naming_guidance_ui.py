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

from types import SimpleNamespace

import metadata.views_inline_api as views_inline_api


def test_targetcolumn_naming_guidance_context_uses_guidance_service(monkeypatch) -> None:
  """
  Verify the inline TargetColumn UI gets advisory naming guidance from the service.
  """
  col = SimpleNamespace(pk=42)
  expected_guidance = (SimpleNamespace(source_column_name="KUNNR"),)

  def fake_build_naming_guidance_for_target_column(target_column):
    """
    Return deterministic guidance for the context helper test.
    """
    assert target_column is col
    return expected_guidance

  monkeypatch.setattr(
    views_inline_api,
    "build_naming_guidance_for_target_column",
    fake_build_naming_guidance_for_target_column,
  )

  context = views_inline_api._targetcolumn_naming_guidance_context(col)

  assert context == {"naming_guidance": expected_guidance}
