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

from types import SimpleNamespace

import elevata_site.context_processors as context_processors


class FakeMeta:
  """
  Django model meta test double.
  """

  def __init__(self, *, model_name: str, verbose_name_plural: str):
    self.model_name = model_name
    self.verbose_name_plural = verbose_name_plural


class TargetSchema:
  """
  TargetSchema model test double.
  """

  _meta = FakeMeta(
    model_name="targetschema",
    verbose_name_plural="Target Schemas",
  )


class TargetDataset:
  """
  TargetDataset model test double.
  """

  _meta = FakeMeta(
    model_name="targetdataset",
    verbose_name_plural="Target Datasets",
  )


class FakeAppConfig:
  """
  Django app config test double.
  """

  def get_models(self):
    """
    Return configured model test doubles.
    """
    return [TargetSchema, TargetDataset]


def test_app_menu_inserts_configured_menu_item_at_end(
  monkeypatch,
) -> None:
  """
  Verify configured menu items can be inserted at the end.
  """
  monkeypatch.setattr(
    context_processors.apps,
    "get_app_config",
    lambda name: FakeAppConfig(),
  )
  monkeypatch.setattr(
    context_processors,
    "_safe_reverse",
    lambda name: f"/{name}/",
  )
  monkeypatch.setattr(
    context_processors.settings,
    "ELEVATA_CRUD",
    {
      "metadata": {
        "order": [
          "TargetSchema",
          "TargetDataset",
        ],
        "descriptions": {
          "TargetSchema": "Model platform layers.",
          "TargetDataset": "Design target datasets.",
        },
        "icons": {
          "TargetSchema": "layers",
          "TargetDataset": "file-check-2",
        },
        "menu_items": [
          {
            "label": "Architecture Control",
            "url_name": "architecture_control",
            "card_text": "Review and approve architecture across controlled scopes.",
            "icon": "shield-check",
            "position": "end",
          },
        ],
      },
    },
  )

  result = context_processors.app_menu(SimpleNamespace())
  items = result["MAIN_MENU"]

  assert [item["label"] for item in items] == [
    "Target Schemas",
    "Target Datasets",
    "Architecture Control",
  ]
  assert items[2]["href"] == "/architecture_control/"
  assert items[2]["icon"] == "shield-check"
  assert items[2]["card_text"] == (
    "Review and approve architecture across controlled scopes."
  )


def test_app_menu_skips_configured_menu_item_without_href(
  monkeypatch,
) -> None:
  """
  Verify configured menu items without resolvable navigation are skipped.
  """
  monkeypatch.setattr(
    context_processors.apps,
    "get_app_config",
    lambda name: FakeAppConfig(),
  )

  def fake_safe_reverse(name: str) -> str:
    """
    Resolve model routes and leave the configured item unresolved.
    """
    if name == "architecture_control":
      return ""

    return f"/{name}/"

  monkeypatch.setattr(
    context_processors,
    "_safe_reverse",
    fake_safe_reverse,
  )
  monkeypatch.setattr(
    context_processors.settings,
    "ELEVATA_CRUD",
    {
      "metadata": {
        "order": [
          "TargetSchema",
          "TargetDataset",
        ],
        "menu_items": [
          {
            "label": "Architecture Control",
            "url_name": "architecture_control",
            "position": "end",
          },
        ],
      },
    },
  )

  result = context_processors.app_menu(SimpleNamespace())
  items = result["MAIN_MENU"]

  assert [item["label"] for item in items] == [
    "Target Schemas",
    "Target Datasets",
  ]