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
from datetime import datetime, timedelta
from pathlib import Path
import hashlib

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


ELEVATA_CMD = os.environ.get("ELEVATA_CMD", "python manage.py")
PROFILE = os.environ.get("ELEVATA_PROFILE", "prod")
TARGET_SYSTEM = os.environ.get("ELEVATA_TARGET_SYSTEM", "dwh")

DEFAULT_MANIFEST_PATH = f"/opt/elevata/core/.artifacts/elevata/manifest_{PROFILE}_{TARGET_SYSTEM}.json"
MANIFEST_PATH = os.environ.get("ELEVATA_MANIFEST_PATH", DEFAULT_MANIFEST_PATH)
MAX_AGE_HOURS = int(os.environ.get("ELEVATA_MANIFEST_MAX_AGE_HOURS", "24"))

TASK_RETRIES = int(
  os.environ.get(
    "ELEVATA_AIRFLOW_TASK_RETRIES",
    "3",
  )
)
TASK_RETRY_DELAY_SECONDS = int(
  os.environ.get(
    "ELEVATA_AIRFLOW_RETRY_DELAY_SECONDS",
    "60",
  )
)
TASK_MAX_RETRY_DELAY_SECONDS = int(
  os.environ.get(
    "ELEVATA_AIRFLOW_MAX_RETRY_DELAY_SECONDS",
    "600",
  )
)

DEFAULT_RUN_PLAN_DIR = (
  "/opt/elevata/core/.artifacts/elevata/airflow_run_plans"
)
RUN_PLAN_DIR = os.environ.get(
  "ELEVATA_RUN_PLAN_DIR",
  DEFAULT_RUN_PLAN_DIR,
)
RUN_PLAN_XCOM_PATH = (
  "{{ ti.xcom_pull(task_ids='create_execution_run_plan') }}"
)


def _load_manifest_if_present(path: str) -> dict | None:
  p = Path(path)
  if not p.exists():
    return None
  with open(path, "r", encoding="utf-8") as f:
    return json.load(f)


def _iter_upstreams(node: dict) -> list[str]:
  """
  Return execution-upstream references from one manifest node.
  """
  execution_deps = (node or {}).get("execution_deps")
  if isinstance(execution_deps, list):
    upstreams: list[str] = []
    for dep in execution_deps:
      if isinstance(dep, dict) and dep.get("id"):
        upstreams.append(str(dep["id"]))
      elif isinstance(dep, str):
        upstreams.append(dep)
    if upstreams:
      return upstreams

  deps = (node or {}).get("deps")
  if isinstance(deps, list):
    return [str(value) for value in deps if value]
  return []


def _target_graph_contract(payload: dict) -> dict:
  """
  Return the effective target-task graph represented by one manifest.

  Source nodes and explanatory dependency metadata are intentionally excluded:
  they do not create Airflow tasks or change target-task scheduling.
  """
  nodes = {
    str(node["id"]): node
    for node in payload.get("nodes", [])
    if isinstance(node, dict) and node.get("id")
  }
  target_node_ids = {
    node_id
    for node_id, node in nodes.items()
    if (node or {}).get("type") == "target"
  }
  id_by_dataset = {
    str(node.get("dataset")): node_id
    for node_id, node in nodes.items()
    if node_id in target_node_ids and node.get("dataset")
  }

  target_nodes: list[dict[str, object]] = []
  for node_id in sorted(target_node_ids):
    node = nodes[node_id]
    upstream_ids: set[str] = set()

    for upstream in _iter_upstreams(node):
      if upstream in target_node_ids:
        upstream_ids.add(upstream)
        continue

      resolved_id = id_by_dataset.get(upstream)
      if resolved_id in target_node_ids:
        upstream_ids.add(resolved_id)

    target_nodes.append({
      "id": node_id,
      "dataset": str(node.get("dataset") or node_id),
      "upstream_ids": sorted(upstream_ids),
    })

  return {
    "target_nodes": target_nodes,
  }


def _manifest_graph_fingerprint(payload: dict) -> str:
  """
  Return a deterministic fingerprint of the effective Airflow task graph.
  """
  canonical = json.dumps(
    _target_graph_contract(payload),
    sort_keys=True,
    separators=(",", ":"),
    ensure_ascii=False,
  )
  return hashlib.sha256(
    canonical.encode("utf-8")
  ).hexdigest()


def _execution_run_plan_identity(
  *,
  dag_id: str,
  run_id: str,
  run_plan_dir: str,
) -> tuple[Path, str, str]:
  """
  Return deterministic artifact and identifier values for one Airflow run.

  The opaque SHA-256 token keeps paths shell-safe while preserving a stable
  one-to-one relationship with the Airflow DAG run identifier.
  """
  token_payload = f"{dag_id}\n{run_id}"
  run_token = hashlib.sha256(
    token_payload.encode("utf-8")
  ).hexdigest()

  plan_path = (
    Path(run_plan_dir)
    / str(dag_id)
    / f"{run_token}.json"
  )

  return (
    plan_path,
    f"airflow-run-plan-{dag_id}-{run_token}",
    f"airflow-batch-{dag_id}-{run_token}",
  )


def _validate_execution_run_plan_payload(
  payload: dict,
  *,
  expected_run_plan_id: str,
  expected_batch_run_id: str,
  expected_profile_name: str,
  expected_target_system_short: str,
  expected_dataset_keys: list[str],
) -> None:
  """
  Validate the public Run Plan against the parsed Airflow target graph.
  """
  if not isinstance(payload, dict):
    raise RuntimeError(
      "Execution Run Plan artifact must contain a JSON object."
    )

  expected_values = {
    "artifact_type": "execution_run_plan",
    "artifact_version": 1,
    "run_plan_id": expected_run_plan_id,
    "batch_run_id": expected_batch_run_id,
    "profile_name": expected_profile_name,
    "target_system_short": expected_target_system_short,
    "scope_mode": "all",
    "dependency_mode": "with_dependencies",
  }

  mismatches = [
    f"{field}={payload.get(field)!r}, expected={expected!r}"
    for field, expected in expected_values.items()
    if payload.get(field) != expected
  ]
  if mismatches:
    raise RuntimeError(
      "Execution Run Plan contract mismatch: "
      + "; ".join(mismatches)
    )

  decisions = payload.get("dataset_decisions")
  if not isinstance(decisions, list):
    raise RuntimeError(
      "Execution Run Plan dataset_decisions must be a list."
    )

  allowed_decisions = {
    "REUSE",
    "INCREMENTAL_EXECUTE",
    "FULL_REBUILD",
  }
  actual_dataset_keys: list[str] = []

  for index, item in enumerate(decisions):
    if not isinstance(item, dict):
      raise RuntimeError(
        "Execution Run Plan dataset decision "
        f"at index {index} must be an object."
      )

    dataset_key = str(
      item.get("dataset_key") or ""
    ).strip()
    decision = str(
      item.get("decision") or ""
    ).strip()

    if not dataset_key:
      raise RuntimeError(
        "Execution Run Plan dataset decision "
        f"at index {index} has no dataset key."
      )
    if decision not in allowed_decisions:
      raise RuntimeError(
        "Execution Run Plan dataset decision "
        f"for {dataset_key} is not executable: {decision!r}."
      )

    actual_dataset_keys.append(dataset_key)

  if len(actual_dataset_keys) != len(set(actual_dataset_keys)):
    raise RuntimeError(
      "Execution Run Plan contains duplicate dataset keys."
    )

  expected_key_set = set(expected_dataset_keys)
  if len(expected_key_set) != len(expected_dataset_keys):
    raise RuntimeError(
      "Parsed Airflow manifest contains duplicate target dataset keys."
    )

  actual_key_set = set(actual_dataset_keys)
  if actual_key_set != expected_key_set:
    missing = sorted(expected_key_set - actual_key_set)
    unexpected = sorted(actual_key_set - expected_key_set)
    details: list[str] = []

    if missing:
      details.append(
        "missing from Run Plan: " + ", ".join(missing)
      )
    if unexpected:
      details.append(
        "not represented by Airflow tasks: "
        + ", ".join(unexpected)
      )

    raise RuntimeError(
      "Execution Run Plan dataset scope does not match the parsed "
      "Airflow target graph: "
      + "; ".join(details)
    )

  if payload.get("dataset_count") != len(actual_dataset_keys):
    raise RuntimeError(
      "Execution Run Plan dataset_count does not match "
      "dataset_decisions."
    )

  if not str(
    payload.get("run_plan_fingerprint") or ""
  ).strip():
    raise RuntimeError(
      "Execution Run Plan has no run_plan_fingerprint."
    )


def _task_id(node_id: str) -> str:
  return node_id.replace(".", "__").replace(" ", "_")


manifest = _load_manifest_if_present(MANIFEST_PATH)
parsed_manifest_graph_fingerprint = (
  _manifest_graph_fingerprint(manifest)
  if manifest is not None
  else None
)

with DAG(
  dag_id="elevata_load",
  start_date=datetime(2026, 1, 1),
  schedule=None,
  catchup=False,
  max_active_runs=1,
  default_args={
    "retries": TASK_RETRIES,
    "retry_delay": timedelta(
      seconds=TASK_RETRY_DELAY_SECONDS,
    ),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(
      seconds=TASK_MAX_RETRY_DELAY_SECONDS,
    ),
  },
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
    # Safety fallback. airflow-init normally creates the initial manifest before
    # the DAG processor and scheduler start.
    missing = BashOperator(
      task_id="missing_manifest",
      retries=0,
      bash_command=(
        "echo 'Manifest file was missing when the DAG was parsed. "
        "Run airflow-init or elevata_manifest before DAG parsing, then wait "
        "for Airflow to reparse the DAG.' && exit 1"
      ),
    )
    generate_manifest >> missing
  else:
    @task(
      task_id="validate_manifest_contract",
      retries=0,
    )
    def validate_manifest_contract(
      path: str,
      expected_graph_fingerprint: str,
      max_age_hours: int,
    ) -> None:
      """
      Validate the refreshed manifest against the graph parsed by Airflow.
      """
      from datetime import datetime, timezone

      current_manifest = _load_manifest_if_present(path)
      if current_manifest is None:
        raise RuntimeError(
          f"Refreshed elevata manifest was not found: {path}"
        )

      actual_graph_fingerprint = _manifest_graph_fingerprint(
        current_manifest
      )
      generated_at = current_manifest.get("generated_at", "<?>")

      print(
        "Using elevata manifest "
        f"generated_at={generated_at} "
        f"path={path} "
        f"graph_fingerprint={actual_graph_fingerprint}"
      )

      if actual_graph_fingerprint != expected_graph_fingerprint:
        raise RuntimeError(
          "The refreshed elevata manifest describes a different target-task "
          "graph than the manifest used to parse this DAG. No dataset tasks "
          "were started. Wait for Airflow to reparse the updated manifest, "
          "then trigger a new DAG run. "
          f"parsed={expected_graph_fingerprint} "
          f"refreshed={actual_graph_fingerprint}"
        )

      if not generated_at or generated_at == "<?>":
        print("WARNING: Manifest has no generated_at timestamp.")
        return

      try:
        ts = datetime.fromisoformat(
          str(generated_at).replace("Z", "+00:00")
        )
      except ValueError:
        print(
          "WARNING: Manifest generated_at timestamp is invalid: "
          f"{generated_at}"
        )
        return

      age = datetime.now(timezone.utc) - ts
      age_hours = age.total_seconds() / 3600.0

      if age_hours > float(max_age_hours):
        print(f"WARNING: Manifest is stale ({age_hours:.1f}h old; threshold={max_age_hours}h).")
      else:
        print(f"Manifest age: {age_hours:.1f}h (threshold={max_age_hours}h).")

    @task(task_id="create_execution_run_plan")
    def create_execution_run_plan(
      *,
      dag_id: str,
      run_id: str,
      elevata_cmd: str,
      profile_name: str,
      target_system_short: str,
      run_plan_dir: str,
      expected_dataset_keys: list[str],
    ) -> str:
      """
      Create, reuse and validate one immutable plan for this DAG run.
      """
      import shlex
      import subprocess

      (
        plan_path,
        run_plan_id,
        batch_run_id,
      ) = _execution_run_plan_identity(
        dag_id=dag_id,
        run_id=run_id,
        run_plan_dir=run_plan_dir,
      )
      plan_path.parent.mkdir(
        parents=True,
        exist_ok=True,
      )

      command = shlex.split(str(elevata_cmd))
      if not command:
        raise RuntimeError(
          "ELEVATA_CMD does not contain an executable command."
        )

      command.extend([
        "elevata_run_plan",
        "--all-datasets",
        "--profile",
        profile_name,
        "--target-system",
        target_system_short,
        "--run-plan-id",
        run_plan_id,
        "--batch-run-id",
        batch_run_id,
        "--output",
        str(plan_path),
        "--reuse-existing",
      ])

      print(
        "Creating immutable elevata Execution Run Plan: "
        f"path={plan_path} "
        f"airflow_run_id={run_id}"
      )
      subprocess.run(
        command,
        cwd="/opt/elevata/core",
        check=True,
      )

      try:
        payload = json.loads(
          plan_path.read_text(encoding="utf-8")
        )
      except OSError as exc:
        raise RuntimeError(
          "Execution Run Plan artifact could not be read: "
          f"{plan_path}"
        ) from exc
      except json.JSONDecodeError as exc:
        raise RuntimeError(
          "Execution Run Plan artifact contains invalid JSON: "
          f"{plan_path}"
        ) from exc

      _validate_execution_run_plan_payload(
        payload,
        expected_run_plan_id=run_plan_id,
        expected_batch_run_id=batch_run_id,
        expected_profile_name=profile_name,
        expected_target_system_short=target_system_short,
        expected_dataset_keys=expected_dataset_keys,
      )

      print(
        "Execution Run Plan ready: "
        f"run_plan_id={run_plan_id} "
        f"batch_run_id={batch_run_id} "
        f"datasets={payload['dataset_count']} "
        f"decisions={payload.get('decision_counts')} "
        "fingerprint="
        f"{payload['run_plan_fingerprint']}"
      )

      return str(plan_path)

    nodes = {n["id"]: n for n in manifest.get("nodes", [])}

    # Only target nodes become executable Airflow tasks.
    target_node_ids = [
      nid for nid, n in nodes.items()
      if (n or {}).get("type") == "target"
    ]
    target_dataset_keys = sorted(
      str(node_id)
      for node_id in target_node_ids
    )

    start = EmptyOperator(task_id="start")
    manifest_guard = validate_manifest_contract(
      MANIFEST_PATH,
      parsed_manifest_graph_fingerprint,
      MAX_AGE_HOURS,
    )
    run_plan_path = create_execution_run_plan(
      dag_id=dag.dag_id,
      run_id="{{ run_id }}",
      elevata_cmd=ELEVATA_CMD,
      profile_name=PROFILE,
      target_system_short=TARGET_SYSTEM,
      run_plan_dir=RUN_PLAN_DIR,
      expected_dataset_keys=target_dataset_keys,
    )
    generate_manifest >> manifest_guard >> run_plan_path >> start

    # Build a lookup by dataset name too (some manifests reference datasets instead of node ids)
    id_by_dataset: dict[str, str] = {}
    for nid, n in nodes.items():
      ds = (n or {}).get("dataset")
      if ds:
        id_by_dataset[str(ds)] = nid

    tasks_by_id: dict[str, BashOperator] = {}

    with TaskGroup(group_id="load_targets") as tg:
      # 1) Create all tasks
      for node_id in target_node_ids:
        node = nodes.get(node_id) or {}
        dataset_name = node.get("dataset") or node_id
        schema_short = node.get("schema")

        schema_option = (
          f" --schema '{schema_short}'"
          if schema_short
          else ""
        )

        tasks_by_id[node_id] = BashOperator(
          task_id=_task_id(node_id),
          bash_command=(
            f"{ELEVATA_CMD} elevata_load '{dataset_name}' "
            f"{schema_option} "
            f"--execute --no-deps "
            f"--target-system {TARGET_SYSTEM} "
            '--run-plan "${ELEVATA_RUN_PLAN_PATH}"'
          ),
          env={
            # Explicit runtime binding plus the shared immutable plan path.
            "ELEVATA_PROFILE": PROFILE,
            "ELEVATA_TARGET_SYSTEM": TARGET_SYSTEM,
            "ELEVATA_RUN_PLAN_PATH": RUN_PLAN_XCOM_PATH,
          },
          append_env=True,
        )

      # 2) Wire dependencies by manifest execution dependencies.
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

    finalize_run_plan = BashOperator(
      task_id="finalize_execution_run_plan",
      trigger_rule=TriggerRule.ALL_SUCCESS,
      bash_command=(
        f"{ELEVATA_CMD} elevata_finalize_run_plan "
        '"${ELEVATA_RUN_PLAN_PATH}"'
      ),
      env={
        # The finalizer consumes the same immutable Run Plan that was used by
        # every dataset task and persists the runtime-scoped Architecture State.
        "ELEVATA_PROFILE": PROFILE,
        "ELEVATA_TARGET_SYSTEM": TARGET_SYSTEM,
        "ELEVATA_RUN_PLAN_PATH": RUN_PLAN_XCOM_PATH,
      },
      append_env=True,
    )

    # Phase gate: load_targets starts only after manifest generation + checks.
    start >> tg >> finalize_run_plan

    # Optional UI cleanup: connect start only to execution roots as well
    # (does not change scheduling semantics; only reduces visual fan-out in some UIs).
    for node_id in target_node_ids:
      node = nodes.get(node_id) or {}
      if not _iter_upstreams(node):
        start >> tasks_by_id[node_id]
