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

from django.db.models.signals import post_save
from django.dispatch import receiver
from django.db import transaction
from django.db.models import Q

from metadata.models import TargetDataset, TargetColumn
from metadata.generation.target_generation_service import TargetGenerationService
from metadata.services.rename_common import sync_key_former_names_for_rawcore_dataset

def _merge_former_names(a, b):
  # union, case-insensitive; preserve original casing as inserted first
  out = []
  seen = set()
  for src in (list(a or []), list(b or [])):
    for v in src:
      if not v:
        continue
      k = str(v).lower()
      if k not in seen:
        seen.add(k)
        out.append(v)
  return out


def _sanitize_hist_dataset_former_names(names: list[str] | None) -> list[str]:
  """
  Hist TargetDataset former_names must never contain base dataset names.
  Keep only values that look like hist dataset names (end with '_hist'), case-insensitive de-duped.
  """
  cur = [n for n in (names or []) if isinstance(n, str) and n.strip()]
  out: list[str] = []
  seen = set()
  for n in cur:
    n2 = n.strip()
    if not n2.lower().endswith("_hist"):
      continue
    k = n2.lower()
    if k not in seen:
      seen.add(k)
      out.append(n2)
  return out


@receiver(post_save, sender=TargetColumn)
def sync_hist_on_rawcore_column_change(sender, instance: TargetColumn, **kwargs) -> None:
  """
  Whenever a column on a rawcore dataset changes (rename, datatype, nullable, ...),
  ensure the corresponding *_hist dataset is schema-synced.

  We deliberately do NOT check is_system_managed here:
  rawcore is generator-managed, but some fields (like name and datatype)
  may be unlocked and edited and must be reflected in *_hist.
  """
  td = instance.target_dataset
  schema = td.target_schema

  # Only operate on rawcore layer; hist datasets themselves are skipped
  if schema.short_name != "rawcore":
    return
  if td.target_dataset_name.endswith("_hist"):
    return
  if not td.historize:
    return

  # only run when the SK exists (or this column *is* the SK)
  if getattr(schema, "surrogate_keys_enabled", False):
    if not (
      instance.system_role == "surrogate_key"
      or TargetColumn.objects.filter(target_dataset=td, system_role="surrogate_key").exists()
    ):
      return

  def _run():
    # td must be the base dataset for this column
    td = getattr(instance, "target_dataset", None)
    if not td:
      return    
    TargetGenerationService().ensure_hist_dataset_for_rawcore(td)

    # After ensuring hist dataset exists, propagate former_names to corresponding hist column
    # so materialization can do RENAME_COLUMN instead of ADD_COLUMN.
    hist_td = TargetDataset.objects.filter(
      target_schema=td.target_schema,
      lineage_key=td.lineage_key,
      target_dataset_name__endswith="_hist",
    ).first()
    if not hist_td:
      return

    # Guardrail: hist TargetDataset former_names must never contain base names.
    fn = list(getattr(hist_td, "former_names", None) or [])
    fn_clean = _sanitize_hist_dataset_former_names(fn)
    if fn_clean != fn:
      hist_td.former_names = fn_clean

      hist_td.save(update_fields=["former_names"])

    # Ensure former_names for key columns are synced as well (base SK + hist SK + hist entity key)
    sync_key_former_names_for_rawcore_dataset(base_td=td, hist_td=hist_td)

    old_name = None
    inst_former = list(getattr(instance, "former_names", None) or [])
    if inst_former:
      old_name = (inst_former[-1] or "").strip() or None

    if not getattr(instance, "lineage_key", None):
      # Fallback mapping by name if lineage_key is missing.
      # This is safe for rawcore because hist mirrors base columns (plus SCD fields).
      cand = TargetColumn.objects.filter(
        target_dataset=hist_td,
        target_column_name=instance.target_column_name,
      ).first()

      if cand is None and old_name:
        cand = TargetColumn.objects.filter(
          target_dataset=hist_td,
          target_column_name=old_name,
        ).first()

      if not cand:
        return

      merged = _merge_former_names(getattr(cand, "former_names", None), inst_former + ([old_name] if old_name else []))
      if merged != list(getattr(cand, "former_names", None) or []):
        cand.former_names = merged
        cand.save(update_fields=["former_names"])
      return

    hist_col = TargetColumn.objects.filter(
      target_dataset=hist_td,
      lineage_key=instance.lineage_key,
    ).first()
    if not hist_col:
      return

    merged = _merge_former_names(getattr(hist_col, "former_names", None), getattr(instance, "former_names", None))
    if merged != list(getattr(hist_col, "former_names", None) or []):
      hist_col.former_names = merged
      hist_col.save(update_fields=["former_names"])

  transaction.on_commit(_run)
