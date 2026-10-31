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

import json

from django import forms
from django.template.loader import get_template
from django.utils.safestring import mark_safe


class PortableExpressionWidget(forms.Widget):
  """Structured editor widget for a persisted portable-expression contract."""

  template_name = "metadata/widgets/portable_expression_editor.html"

  def __init__(
    self,
    attrs=None,
    *,
    editor_kind="expression",
    column_label="Upstream column",
    intro_text="Define the expression using the selected upstream columns.",
    empty_text="No expression defined.",
    runtime_values=(),
  ):
    super().__init__(attrs)
    self.column_names: list[str] = []
    self.editor_kind = editor_kind
    self.column_label = column_label
    self.intro_text = intro_text
    self.empty_text = empty_text
    self.runtime_values = tuple(runtime_values)

  def format_value(self, value):
    if value in (None, ""):
      return ""
    if isinstance(value, str):
      return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

  def get_context(self, name, value, attrs):
    context = super().get_context(name, value, attrs)
    context["widget"]["value"] = self.format_value(value)
    context["widget"]["column_names_json"] = json.dumps(
      self.column_names,
      ensure_ascii=False,
      separators=(",", ":"),
    )
    context["widget"]["editor_kind"] = self.editor_kind
    context["widget"]["column_label"] = self.column_label
    context["widget"]["intro_text"] = self.intro_text
    context["widget"]["empty_text"] = self.empty_text
    context["widget"]["runtime_values_json"] = json.dumps(
      [{"name": name, "label": label} for name, label in self.runtime_values],
      ensure_ascii=False,
      separators=(",", ":"),
    )
    return context

  def render(self, name, value, attrs=None, renderer=None):
    """
    Render the widget through elevata's configured project template loader.

    Django form widgets use a dedicated form renderer by default. elevata keeps
    application templates in the shared project template root, so this widget
    deliberately uses the project's configured Django template engine.
    """
    context = self.get_context(name, value, attrs)
    template = get_template(self.template_name)
    return mark_safe(template.render(context))
