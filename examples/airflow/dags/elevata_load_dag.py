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


import json
import os
from datetime import datetime
from pathlib import Path
import hashlib
from typing import Iterable

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


ELEVATA_CMD = os.environ.get("ELEVATA_CMD", "python manage.py")
PROFILE = os.environ.get("ELEVATA_PROFILE", "prod")
TARGET_SYSTEM = os.environ.get("ELEVATA_TARGET_SYSTEM", "dwh")

DEFAULT_MANIFEST_PATH = f"/opt/elevata/core/.artifacts/elevata/manifest_{PROFILE}_{TARGET_SYSTEM}.json"
MANIFEST_PATH = os.environ.get("ELEVATA_MANIFEST_PATH", DEFAULT_MANIFEST_PATH)
MAX_AGE_HOURS = int(os.environ.get("ELEVATA_MANIFEST_MAX_AGE_HOURS", "24"))


def _load_manifest_if_present(path: str) -> dict | None:
  p = Path(path)
  if not p.exists():
    return None
  with open(path, "r", encoding="utf-8") as f:
    return json.load(f)


def _task_id(node_id: str) -> str:
  return node_id.replace(".", "__").replace(" ", "_")


manifest = _load_manifest_if_present(MANIFEST_PATH)

with DAG(
  dag_id="elevata_load",
  start_date=datetime(2026, 1, 1),
  schedule=None,
  catchup=False,
  max_active_runs=1,
) as dag:

  # Always generate/refresh manifest on each run (explicit relationship, no separate DAG needed)
  generate_manifest = BashOperator(
    task_id="generate_manifest",
    bash_command=(
      "bash -lc 'set -euo pipefail; "
      f"{ELEVATA_CMD} elevata_manifest "
      f"--profile {PROFILE} "
      f"--target-system {TARGET_SYSTEM}"
      "'"
    ),
  )

  if not manifest:
    # Make the DAG visible even before first manifest exists.
    # User triggers the DAG once -> generate_manifest writes manifest -> next parse shows full graph.
    missing = BashOperator(
      task_id="missing_manifest",
      bash_command=(
        "echo 'Manifest file not found. Run this DAG once to generate it, "
        "then refresh the DAG to see the full task graph.' && exit 1"
      ),
    )
    generate_manifest >> missing
  else:
    manifest_generated_at = manifest.get("generated_at", "<?>")

    manifest_info = BashOperator(
      task_id="manifest_info",
      bash_command=f"echo 'Using elevata manifest generated_at={manifest_generated_at} path={MANIFEST_PATH}'",
    )

    @task(task_id="check_manifest_age")
    def check_manifest_age(generated_at: str, max_age_hours: int) -> None:
      # Non-blocking warning only.
      from datetime import datetime, timezone

      if not generated_at or generated_at == "<?>":
        print("WARNING: Manifest has no generated_at timestamp.")
        return

      ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
      age = datetime.now(timezone.utc) - ts
      age_hours = age.total_seconds() / 3600.0

      if age_hours > float(max_age_hours):
        print(f"WARNING: Manifest is stale ({age_hours:.1f}h old; threshold={max_age_hours}h).")
      else:
        print(f"Manifest age: {age_hours:.1f}h (threshold={max_age_hours}h).")

    nodes = {n["id"]: n for n in manifest.get("nodes", [])}

    start = EmptyOperator(task_id="start")
    generate_manifest >> manifest_info >> check_manifest_age(manifest_generated_at, MAX_AGE_HOURS) >> start

    # Build a lookup by dataset name too (some manifests reference datasets instead of node ids)
    id_by_dataset: dict[str, str] = {}
    for nid, n in nodes.items():
      ds = (n or {}).get("dataset")
      if ds:
        id_by_dataset[str(ds)] = nid
    
    def _iter_upstreams(node: dict) -> list[str]:
      v = (node or {}).get("deps")
      if isinstance(v, list):
        return [str(x) for x in v if x]
      return []

    # Only create Airflow tasks for "target" nodes.
    target_node_ids = [
      nid for nid, n in nodes.items()
      if (n or {}).get("type") == "target"
    ]

    tasks_by_id: dict[str, BashOperator] = {}

    with TaskGroup(group_id="load_targets") as tg:
      # 1) Create all tasks
      for node_id in target_node_ids:
        dataset_name = (nodes.get(node_id) or {}).get("dataset") or node_id
        tasks_by_id[node_id] = BashOperator(
          task_id=_task_id(node_id),
          bash_command=(
            f"{ELEVATA_CMD} elevata_load '{dataset_name}' "
            f"--execute --no-deps "
            f"--target-system {TARGET_SYSTEM}"
          ),
          env={
            # Make profile explicit for the subprocess (optional, but nice)
            "ELEVATA_PROFILE": PROFILE,
            "ELEVATA_TARGET_SYSTEM": TARGET_SYSTEM,
          },
        )

      # 2) Wire dependencies purely by lineage
      for node_id in target_node_ids:
        node = nodes.get(node_id) or {}
        ups = _iter_upstreams(node)
        for up in ups:
          # Resolve upstream reference to a target node id if possible
          up_id = None
          if up in tasks_by_id:
            up_id = up
          elif up in id_by_dataset and id_by_dataset[up] in tasks_by_id:
            up_id = id_by_dataset[up]

          if up_id:
            tasks_by_id[up_id] >> tasks_by_id[node_id]

    # Phase gate: load_targets starts only after manifest generation + checks.
    start >> tg

    # Optional UI cleanup: connect start only to lineage roots as well
    # (does not change scheduling semantics; only reduces visual fan-out in some UIs).
    for node_id in target_node_ids:
      node = nodes.get(node_id) or {}
      if not _iter_upstreams(node):
        start >> tasks_by_id[node_id]
