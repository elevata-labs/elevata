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

from typing import Dict, List

from django.core.management.base import BaseCommand

from metadata.models import TargetDataset
from metadata.generation.validators import (
  validate_all_incremental_targets,
  validate_all_semantic_targets,
  validate_all_materialization,
  validate_all_hist_naming,
  summarize_targetdataset_health,
)


class Command(BaseCommand):
  """
  Run metadata health checks for all target datasets.

  This command aggregates:
    - incremental configuration checks
    - bizcore semantics (if applicable)
    - materialization expectations

  It prints a short summary and detailed issues per dataset.
  Exit code is 0 if no issues are found, 1 otherwise.
  """

  help = "Run metadata health checks for all target datasets."

  def handle(self, *args, **options):
    self.stdout.write("Running elevata metadata health checks…\n")

    def _split_info(issues_by_pk: Dict[int, List[str]]) -> tuple[Dict[int, List[str]], Dict[int, List[str]]]:
      # Return (failing_issues, info_issues) based on "INFO:" prefix.
      failing: Dict[int, List[str]] = {}
      info: Dict[int, List[str]] = {}

      for pk, msgs in issues_by_pk.items():
        for msg in msgs:
          bucket = info if msg.startswith("INFO:") else failing
          bucket.setdefault(pk, []).append(msg)

      return failing, info

    incr_all: Dict[int, List[str]] = validate_all_incremental_targets()
    sem_all: Dict[int, List[str]] = validate_all_semantic_targets()
    mat_all: Dict[int, List[str]] = validate_all_materialization()
    hist_all: Dict[int, List[str]] = validate_all_hist_naming()

    incr_issues, incr_info = _split_info(incr_all)
    sem_issues, sem_info = _split_info(sem_all)
    mat_issues, mat_info = _split_info(mat_all)
    hist_issues, hist_info = _split_info(hist_all)

    # Collect all datasets that have at least one issue in any category
    problematic_ids = (
      set(incr_issues.keys())
      | set(sem_issues.keys())
      | set(mat_issues.keys())
      | set(hist_issues.keys())
    )
    info_only_ids = (
      set(incr_info.keys())
      | set(sem_info.keys())
      | set(mat_info.keys())
      | set(hist_info.keys())
    ) - problematic_ids  

    total_targets = TargetDataset.objects.count()
    total_problematic = len(problematic_ids)
    total_info_only = len(info_only_ids)

    self.stdout.write(f"Total target datasets: {total_targets}")
    self.stdout.write(f"Datasets with issues: {total_problematic}\n")
    if total_info_only:
      self.stdout.write(f"Datasets with INFO only: {total_info_only}\n")    

    if not problematic_ids:
      if info_only_ids:
        self.stdout.write(self.style.SUCCESS("No failing issues found. 🎉"))
        self.stdout.write(self.style.WARNING("Some datasets have INFO notices:\n"))
        for pk in sorted(info_only_ids):
          td = TargetDataset.objects.select_related("target_schema").get(pk=pk)
          schema = td.target_schema.short_name if td.target_schema else "<?>"

          self.stdout.write("")
          self.stdout.write(self.style.WARNING(
            f"[{pk}] {td.target_dataset_name} (schema={schema})"
          ))

          if pk in incr_info:
            for msg in incr_info[pk]:
              self.stdout.write(f"  - Incremental: {msg}")
          if pk in sem_info:
            for msg in sem_info[pk]:
              self.stdout.write(f"  - Semantic: {msg}")
          if pk in mat_info:
            for msg in mat_info[pk]:
              self.stdout.write(f"  - Materialization: {msg}")
          if pk in hist_info:
            for msg in hist_info[pk]:
              self.stdout.write(f"  - Naming: {msg}")

          level, _ = summarize_targetdataset_health(td)
          self.stdout.write(f"  -> Health level: {level}")

        return

      self.stdout.write(self.style.SUCCESS("All target datasets look healthy. 🎉"))
      return

    # Print details grouped by dataset
    for pk in sorted(problematic_ids):
      td = TargetDataset.objects.select_related("target_schema").get(pk=pk)
      schema = td.target_schema.short_name if td.target_schema else "<?>"

      self.stdout.write("")
      self.stdout.write(self.style.WARNING(
        f"[{pk}] {td.target_dataset_name} (schema={schema})"
      ))

      # Incremental issues
      if pk in incr_issues:
        for msg in incr_issues[pk]:
          self.stdout.write(f"  - Incremental: {msg}")
      if pk in incr_info:
        for msg in incr_info[pk]:
          self.stdout.write(f"  - Incremental: {msg}")

      # BizCore issues
      if pk in sem_issues:
        for msg in sem_issues[pk]:
          self.stdout.write(f"  - Semantic: {msg}")
      if pk in sem_info:
        for msg in sem_info[pk]:
          self.stdout.write(f"  - Semantic: {msg}")

      # Materialization issues
      if pk in mat_issues:
        for msg in mat_issues[pk]:
          self.stdout.write(f"  - Materialization: {msg}")
      if pk in mat_info:
        for msg in mat_info[pk]:
          self.stdout.write(f"  - Materialization: {msg}")

      # Naming / historize advisory issues
      if pk in hist_issues:
        for msg in hist_issues[pk]:
          self.stdout.write(f"  - Naming: {msg}")
      if pk in hist_info:
        for msg in hist_info[pk]:
          self.stdout.write(f"  - Naming: {msg}")

      # Optional: aggregated health level (mainly for curiosity / future extension)
      level, _ = summarize_targetdataset_health(td)
      self.stdout.write(f"  -> Health level: {level}")

    self.stdout.write("")
    self.stdout.write(self.style.ERROR("Metadata health check found issues."))
    # Non-zero exit code so this can be used in CI
    raise SystemExit(1)
