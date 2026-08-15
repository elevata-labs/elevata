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

import hashlib
import json
import os
import re

from django.conf import settings
from django.db import migrations


RUNTIME_PEPPER_TOKEN = "{runtime:pepper}"

COL_PATTERN = re.compile(r'col\(["\']([^"\']+)["\']\)')
EXPR_REF_PATTERN = re.compile(r"\{expr:([^}]+)\}")

LEGACY_PEPPER_BINDING_RE = re.compile(
  r"^(?P<prefix>\s*HASH256\s*\(\s*CONCAT_WS\s*\(.*,\s*)"
  r"'(?P<pepper>(?:''|[^'])*)'"
  r"(?P<suffix>\s*\)\s*\)\s*)$",
  re.IGNORECASE | re.DOTALL,
)


def _canonical_json(value):
  return json.dumps(
    value,
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
  )


def _resolve_runtime_pepper():
  pepper = os.environ.get("ELEVATA_PEPPER")

  if not pepper:
    profile = os.environ.get("ELEVATA_PROFILE", "DEV").upper()
    pepper = os.environ.get(f"SEC_{profile}_PEPPER")

  if not pepper:
    pepper = getattr(settings, "ELEVATA_PEPPER", None)

  if not pepper:
    raise RuntimeError(
      "Cannot migrate legacy generated hash expressions because no runtime "
      "pepper is configured. Set ELEVATA_PEPPER or SEC_<PROFILE>_PEPPER "
      "before running this migration."
    )

  return str(pepper)


def _column_label(column):
  target_dataset = getattr(column, "target_dataset", None)
  target_schema = getattr(target_dataset, "target_schema", None)
  schema_name = str(getattr(target_schema, "short_name", "") or "").strip()
  dataset_name = str(
    getattr(target_dataset, "target_dataset_name", "") or ""
  ).strip()
  column_name = str(getattr(column, "target_column_name", "") or "").strip()
  return f"{schema_name}.{dataset_name}.{column_name} (pk={column.pk})"


def _parse_legacy_pepper_binding(expression):
  match = LEGACY_PEPPER_BINDING_RE.fullmatch(str(expression or ""))
  if not match:
    return None

  embedded_pepper = match.group("pepper").replace("''", "'")
  portable_expression = (
    f"{match.group('prefix')}"
    f"{RUNTIME_PEPPER_TOKEN}"
    f"{match.group('suffix')}"
  )
  return embedded_pepper, portable_expression


def _dataset_transport_key(target_dataset):
  """
  Mirror TargetDatasetReference._dataset_transport_key() without local IDs.
  """
  lineage_key = str(target_dataset.lineage_key or "").strip()
  if lineage_key:
    return lineage_key

  schema_short_name = str(
    target_dataset.target_schema.short_name or ""
  ).strip()
  dataset_name = str(
    target_dataset.target_dataset_name or ""
  ).strip()
  return f"{schema_short_name}.{dataset_name}"


def _reference_fk_lineage_key(reference):
  """
  Mirror the current TargetDatasetReference.transport_key contract exactly.
  """
  payload = {
    "referenced_dataset": _dataset_transport_key(
      reference.referenced_dataset
    ),
    "reference_prefix": str(reference.reference_prefix or "").strip(),
    "referencing_dataset": _dataset_transport_key(
      reference.referencing_dataset
    ),
  }
  digest = hashlib.sha256(
    _canonical_json(payload).encode("utf-8")
  ).hexdigest()
  return f"fk:{digest}"


def _stage_expression_for_column(column, database_alias):
  manual_expression = str(
    getattr(column, "manual_expression", "") or ""
  ).strip()
  if manual_expression:
    return manual_expression

  link = (
    column.input_links
    .using(database_alias)
    .filter(active=True)
    .select_related("upstream_target_column")
    .order_by("ordinal_position", "id")
    .first()
  )
  if link and link.upstream_target_column:
    return f'col("{link.upstream_target_column.target_column_name}")'

  return None


def _rewrite_parent_sk_expression(parent_expression, mapping):
  def _expr_repl(match):
    name = (match.group(1) or "").strip()
    return mapping.get(name, match.group(0))

  rewritten = EXPR_REF_PATTERN.sub(
    _expr_repl,
    str(parent_expression or ""),
  )

  seen_counts = {}
  result = []
  index = 0

  while True:
    match = COL_PATTERN.search(rewritten, index)
    if not match:
      result.append(rewritten[index:])
      break

    result.append(rewritten[index:match.start()])
    column_name = match.group(1)

    if column_name not in mapping:
      replacement = f'col("{column_name}")'
    else:
      count = seen_counts.get(column_name, 0) + 1
      seen_counts[column_name] = count
      replacement = (
        f'col("{column_name}")'
        if count == 1
        else mapping[column_name]
      )

    result.append(replacement)
    index = match.end()

  return "".join(result)


def _build_expected_fk_expression(
  *,
  reference,
  TargetColumn,
  database_alias,
  future_expressions,
):
  parent_sk = (
    TargetColumn.objects
    .using(database_alias)
    .filter(
      target_dataset_id=reference.referenced_dataset_id,
      system_role="surrogate_key",
      active=True,
    )
    .order_by("ordinal_position", "id")
    .first()
  )
  if parent_sk is None:
    raise RuntimeError(
      "referenced dataset has no active surrogate-key column"
    )

  parent_expression = future_expressions.get(
    parent_sk.pk,
    str(parent_sk.surrogate_expression or ""),
  )
  if not parent_expression:
    raise RuntimeError(
      "referenced dataset surrogate key has no expression"
    )

  components = list(
    reference.key_components
    .using(database_alias)
    .select_related("from_column", "to_column")
    .order_by("ordinal_position", "id")
  )
  if not components:
    raise RuntimeError("reference has no key components")

  mapping = {}
  for component in components:
    parent_bk_name = component.to_column.target_column_name
    parent_stage_expression = _stage_expression_for_column(
      component.to_column,
      database_alias,
    )
    if parent_stage_expression:
      match = COL_PATTERN.fullmatch(parent_stage_expression.strip())
      if match:
        parent_bk_name = match.group(1)

    child_stage_expression = _stage_expression_for_column(
      component.from_column,
      database_alias,
    )
    if not child_stage_expression:
      raise RuntimeError(
        "cannot resolve Stage expression for child column "
        f"{component.from_column.target_column_name}"
      )

    mapping[parent_bk_name] = child_stage_expression

  return _rewrite_parent_sk_expression(
    parent_expression,
    mapping,
  )


def migrate_portable_generated_hash_expressions(apps, schema_editor):
  TargetColumn = apps.get_model("metadata", "TargetColumn")
  TargetDatasetReference = apps.get_model(
    "metadata",
    "TargetDatasetReference",
  )
  database_alias = schema_editor.connection.alias

  columns = list(
    TargetColumn.objects
    .using(database_alias)
    .filter(system_role__in=("surrogate_key", "foreign_key"))
    .select_related("target_dataset__target_schema")
    .order_by("pk")
  )

  future_expressions = {}
  planned_updates = {}
  concrete_bindings = []
  errors = []

  for column in columns:
    expression = str(column.surrogate_expression or "")
    if not expression.strip():
      continue

    token_count = expression.count(RUNTIME_PEPPER_TOKEN)
    if token_count == 1:
      future_expressions[column.pk] = expression
      continue

    if token_count > 1:
      errors.append(
        f"{_column_label(column)} contains multiple runtime pepper bindings"
      )
      continue

    parsed = _parse_legacy_pepper_binding(expression)
    if parsed is None:
      errors.append(
        f"{_column_label(column)} has an unrecognized generated hash expression"
      )
      continue

    concrete_bindings.append((column, parsed))

  if errors:
    raise RuntimeError(
      "Cannot migrate generated hash expressions safely: "
      + "; ".join(errors)
    )

  if concrete_bindings:
    runtime_pepper = _resolve_runtime_pepper()
    mismatch_labels = []

    for column, (embedded_pepper, portable_expression) in concrete_bindings:
      if embedded_pepper != runtime_pepper:
        mismatch_labels.append(_column_label(column))
        continue

      planned_updates[column.pk] = portable_expression
      future_expressions[column.pk] = portable_expression

    if mismatch_labels:
      raise RuntimeError(
        "Cannot replace embedded peppers with runtime bindings because doing "
        "so would change hash semantics. The configured runtime pepper does "
        "not match the persisted pepper for: "
        + ", ".join(mismatch_labels)
      )

  references = list(
    TargetDatasetReference.objects
    .using(database_alias)
    .select_related(
      "referencing_dataset__target_schema",
      "referenced_dataset__target_schema",
    )
    .order_by("pk")
  )

  fk_errors = []
  for reference in references:
    fk_lineage_key = _reference_fk_lineage_key(reference)
    fk_candidates = list(
      TargetColumn.objects
      .using(database_alias)
      .filter(
        target_dataset_id=reference.referencing_dataset_id,
        system_role="foreign_key",
        lineage_key=fk_lineage_key,
      )
      .select_related("target_dataset__target_schema")
      .order_by("pk")[:2]
    )

    if len(fk_candidates) > 1:
      fk_errors.append(
        "multiple generated FK columns match TargetDatasetReference "
        f"{reference.pk}"
      )
      continue

    if not fk_candidates:
      continue

    fk_column = fk_candidates[0]
    try:
      expected_expression = _build_expected_fk_expression(
        reference=reference,
        TargetColumn=TargetColumn,
        database_alias=database_alias,
        future_expressions=future_expressions,
      )
    except RuntimeError as exc:
      fk_errors.append(
        f"{_column_label(fk_column)} cannot be reconciled: {exc}"
      )
      continue

    current_future_expression = future_expressions.get(
      fk_column.pk,
      str(fk_column.surrogate_expression or ""),
    )
    if current_future_expression != expected_expression:
      planned_updates[fk_column.pk] = expected_expression
      future_expressions[fk_column.pk] = expected_expression

  if fk_errors:
    raise RuntimeError(
      "Cannot reconcile generated FK expressions safely: "
      + "; ".join(fk_errors)
    )

  for column in columns:
    expression = future_expressions.get(
      column.pk,
      str(column.surrogate_expression or ""),
    )
    if not expression.strip():
      continue
    if expression.count(RUNTIME_PEPPER_TOKEN) != 1:
      raise RuntimeError(
        "Generated hash expression did not converge to exactly one runtime "
        f"pepper binding: {_column_label(column)}"
      )

  for column_pk, expression in sorted(planned_updates.items()):
    TargetColumn.objects.using(database_alias).filter(
      pk=column_pk
    ).update(
      surrogate_expression=expression
    )


class Migration(migrations.Migration):

  dependencies = [
    ("metadata", "0012_portable_generated_lineage_keys"),
  ]

  operations = [
    migrations.RunPython(
      migrate_portable_generated_hash_expressions,
      reverse_code=migrations.RunPython.noop,
    ),
  ]
