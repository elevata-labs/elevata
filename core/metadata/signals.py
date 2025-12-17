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

from metadata.models import TargetDataset, TargetColumn
from metadata.generation.target_generation_service import TargetGenerationService


@receiver(post_save, sender=TargetDataset)
def sync_hist_on_rawcore_dataset_change(sender, instance: TargetDataset, **kwargs) -> None:
  """
  If a rawcore TargetDataset is renamed or its historize-flag changes,
  keep the corresponding *_hist target in sync.
  """
  schema = instance.target_schema

  if schema.short_name != "rawcore":
    return
  if not instance.historize:
    return
  if instance.target_dataset_name.endswith("_hist"):
    return

  # prevent early fire during generation: only run if rawcore has an SK
  if getattr(schema, "surrogate_keys_enabled", False):
    if not TargetColumn.objects.filter(
      target_dataset=instance,
      system_role="surrogate_key",
    ).exists():
      return
  else:
    # if no SK concept, at least require columns
    if not TargetColumn.objects.filter(target_dataset=instance).exists():
      return

  TargetGenerationService().ensure_hist_dataset_for_rawcore(instance)


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
    TargetGenerationService().ensure_hist_dataset_for_rawcore(td)

  transaction.on_commit(_run)
