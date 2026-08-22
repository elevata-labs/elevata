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
import re
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

# Scheduler-local overrides only. Every manifest load scope becomes a DAG
# automatically. Missing entries default safely to schedule=None.
#
# Example:
# SCHEDULES = {
#   "full": "0 2 * * *",
#   "sales": "0 * * * *",
# }
SCHEDULES: dict[str, str | None] = {
  "full": None,
}


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


def _manifest_load_scopes(payload: dict) -> list[dict]:
  """
  Return validated load-scope definitions from one manifest v3+ payload.
  """
  try:
    manifest_version = int(payload.get("manifest_version") or 0)
  except (TypeError, ValueError):
    manifest_version = 0

  if manifest_version < 3:
    raise RuntimeError(
      "The Airflow example requires manifest_version >= 3 with load_scopes. "
      "Regenerate the manifest and wait for Airflow to reparse the DAG."
    )

  raw_scopes = payload.get("load_scopes")
  if not isinstance(raw_scopes, list) or not raw_scopes:
    raise RuntimeError("The elevata manifest contains no load_scopes.")

  scopes: list[dict] = []
  names: set[str] = set()

  for raw_scope in raw_scopes:
    if not isinstance(raw_scope, dict):
      raise RuntimeError("Manifest load scope entries must be objects.")

    name = str(raw_scope.get("name") or "").strip()
    scope_mode = str(raw_scope.get("scope_mode") or "").strip()
    roots = raw_scope.get("root_dataset_ids")
    datasets = raw_scope.get("dataset_ids")

    if not name or name in names:
      raise RuntimeError(
        f"Manifest load scope name is missing or duplicated: {name!r}."
      )
    if scope_mode not in {"all", "partial_load"}:
      raise RuntimeError(
        f"Manifest load scope {name!r} has unsupported mode {scope_mode!r}."
      )
    if not isinstance(roots, list) or not isinstance(datasets, list) or not datasets:
      raise RuntimeError(
        f"Manifest load scope {name!r} has an invalid root/dataset contract."
      )

    scope = {
      "name": name,
      "scope_mode": scope_mode,
      "root_dataset_ids": [str(value) for value in roots],
      "dataset_ids": [str(value) for value in datasets],
    }

    if len(scope["root_dataset_ids"]) != len(set(scope["root_dataset_ids"])):
      raise RuntimeError(f"Manifest load scope {name!r} contains duplicate roots.")
    if len(scope["dataset_ids"]) != len(set(scope["dataset_ids"])):
      raise RuntimeError(f"Manifest load scope {name!r} contains duplicate datasets.")
    if name == "full" and scope_mode != "all":
      raise RuntimeError("Reserved load scope 'full' must use scope_mode='all'.")
    if name != "full" and scope_mode != "partial_load":
      raise RuntimeError(
        f"Non-full load scope {name!r} must use scope_mode='partial_load'."
      )
    if scope_mode == "partial_load" and not scope["root_dataset_ids"]:
      raise RuntimeError(f"Partial load scope {name!r} has no roots.")

    names.add(name)
    scopes.append(scope)

  if "full" not in names:
    raise RuntimeError("The manifest does not contain the implicit 'full' load scope.")

  return sorted(
    scopes,
    key=lambda scope: (
      0 if scope["name"] == "full" else 1,
      scope["name"],
    ),
  )


def _load_scope_by_name(payload: dict, load_scope_name: str) -> dict:
  """
  Resolve one exact manifest load scope or fail closed.
  """
  for scope in _manifest_load_scopes(payload):
    if scope["name"] == load_scope_name:
      return scope
  raise RuntimeError(
    f"Load scope {load_scope_name!r} is missing from the refreshed manifest."
  )


def _target_graph_contract(payload: dict, *, load_scope_name: str) -> dict:
  """
  Return the effective target-task graph represented by one load scope.

  Source nodes and explanatory dependency metadata are intentionally excluded:
  they do not create Airflow tasks or change target-task scheduling.
  """
  scope = _load_scope_by_name(payload, load_scope_name)
  nodes = {
    str(node["id"]): node
    for node in payload.get("nodes", [])
    if isinstance(node, dict) and node.get("id")
  }
  all_target_node_ids = {
    node_id
    for node_id, node in nodes.items()
    if (node or {}).get("type") == "target"
  }
  target_node_ids = set(scope["dataset_ids"])

  missing = sorted(target_node_ids - all_target_node_ids)
  if missing:
    raise RuntimeError(
      f"Load scope {load_scope_name!r} references target nodes missing from "
      f"the manifest: {', '.join(missing)}."
    )

  roots = set(scope["root_dataset_ids"])
  if not roots.issubset(target_node_ids):
    raise RuntimeError(
      f"Load scope {load_scope_name!r} contains roots outside its resolved scope."
    )

  id_by_dataset = {
    str(node.get("dataset")): node_id
    for node_id, node in nodes.items()
    if node_id in all_target_node_ids and node.get("dataset")
  }

  target_nodes: list[dict[str, object]] = []
  for node_id in sorted(target_node_ids):
    node = nodes[node_id]
    upstream_ids: set[str] = set()

    for upstream in _iter_upstreams(node):
      up_id = upstream if upstream in all_target_node_ids else id_by_dataset.get(upstream)
      if up_id not in all_target_node_ids:
        continue  # SourceDataset dependency: no Airflow task.
      if up_id not in target_node_ids:
        raise RuntimeError(
          f"Load scope {load_scope_name!r} is not execution-closed: "
          f"{node_id} depends on {up_id} outside the scope."
        )
      upstream_ids.add(up_id)

    target_nodes.append({
      "id": node_id,
      "dataset": str(node.get("dataset") or node_id),
      "upstream_ids": sorted(upstream_ids),
    })

  return {
    "load_scope": {
      "name": scope["name"],
      "scope_mode": scope["scope_mode"],
      "root_dataset_ids": sorted(scope["root_dataset_ids"]),
      "dataset_ids": sorted(scope["dataset_ids"]),
    },
    "target_nodes": target_nodes,
  }


def _manifest_scope_fingerprint(payload: dict, *, load_scope_name: str) -> str:
  """
  Return a deterministic fingerprint of one effective Airflow load-scope graph.
  """
  canonical = json.dumps(
    _target_graph_contract(payload, load_scope_name=load_scope_name),
    sort_keys=True,
    separators=(",", ":"),
    ensure_ascii=False,
  )
  return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
  expected_scope_mode: str,
  expected_scope_key: str,
  expected_dataset_keys: list[str],
  expected_root_dataset_keys: list[str] | None,
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
    "scope_mode": expected_scope_mode,
    "scope_key": expected_scope_key,
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

  if expected_root_dataset_keys is not None:
    actual_roots = payload.get("root_dataset_keys")
    if not isinstance(actual_roots, list):
      raise RuntimeError(
        "Execution Run Plan root_dataset_keys must be a list."
      )
    if sorted(str(value) for value in actual_roots) != sorted(expected_root_dataset_keys):
      raise RuntimeError(
        "Execution Run Plan roots do not match the parsed Partial Load."
      )

  if not str(
    payload.get("run_plan_fingerprint") or ""
  ).strip():
    raise RuntimeError(
      "Execution Run Plan has no run_plan_fingerprint."
    )


def _task_id(node_id: str) -> str:
  return node_id.replace(".", "__").replace(" ", "_")


def _dag_id_for_load_scope(load_scope_name: str) -> str:
  """
  Return the stable Airflow DAG id for one manifest load scope.
  """
  if load_scope_name == "full":
    return "elevata_load"

  suffix = re.sub(r"[^A-Za-z0-9_.-]+", "_", load_scope_name).strip("._-")
  if not suffix:
    raise RuntimeError(
      f"Load scope {load_scope_name!r} cannot be converted to an Airflow DAG id."
    )
  return f"elevata_load_{suffix}"


def _run_plan_scope_args(load_scope: dict) -> tuple[list[str], str, str, list[str] | None]:
  """
  Return CLI arguments plus expected immutable scope identity.
  """
  name = str(load_scope["name"])
  if load_scope["scope_mode"] == "all":
    return ["--all-datasets"], "all", "all", None
  return (
    ["--partial-load", name],
    "partial_load",
    f"partial_load:{name}",
    list(load_scope["root_dataset_ids"]),
  )


def _dag_default_args() -> dict:
  return {
    "retries": TASK_RETRIES,
    "retry_delay": timedelta(seconds=TASK_RETRY_DELAY_SECONDS),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=TASK_MAX_RETRY_DELAY_SECONDS),
  }


def _build_missing_manifest_dag() -> DAG:
  """
  Preserve the stable full-load placeholder before the first manifest exists.
  """
  with DAG(
    dag_id="elevata_load",
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULES.get("full"),
    catchup=False,
    max_active_runs=1,
    default_args=_dag_default_args(),
  ) as dag:
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
  return dag


def _build_elevata_load_dag(*, manifest: dict, load_scope: dict) -> DAG:
  """
  Build one Airflow DAG from one resolved manifest load scope.
  """
  load_scope_name = str(load_scope["name"])
  dag_id = _dag_id_for_load_scope(load_scope_name)
  parsed_scope_fingerprint = _manifest_scope_fingerprint(
    manifest,
    load_scope_name=load_scope_name,
  )
  nodes = {n["id"]: n for n in manifest.get("nodes", [])}
  target_node_ids = list(load_scope["dataset_ids"])
  target_dataset_keys = sorted(str(node_id) for node_id in target_node_ids)
  (
    run_plan_scope_args,
    expected_scope_mode,
    expected_scope_key,
    expected_root_dataset_keys,
  ) = _run_plan_scope_args(load_scope)

  with DAG(
    dag_id=dag_id,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULES.get(load_scope_name),
    catchup=False,
    max_active_runs=1,
    default_args=_dag_default_args(),
    tags=["elevata", "full" if load_scope_name == "full" else "partial-load"],
  ) as dag:

    # Refresh the canonical manifest on every run.
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

    @task(task_id="validate_manifest_contract", retries=0)
    def validate_manifest_contract(
      path: str,
      load_scope_name: str,
      expected_scope_fingerprint: str,
      max_age_hours: int,
    ) -> None:
      """
      Validate the refreshed manifest against this parsed load scope.
      """
      from datetime import datetime, timezone

      current_manifest = _load_manifest_if_present(path)
      if current_manifest is None:
        raise RuntimeError(
          f"Refreshed elevata manifest was not found: {path}"
        )

      actual_scope_fingerprint = _manifest_scope_fingerprint(
        current_manifest,
        load_scope_name=load_scope_name,
      )
      generated_at = current_manifest.get("generated_at", "<?>")

      print(
        "Using elevata manifest "
        f"generated_at={generated_at} "
        f"path={path} "
        f"load_scope={load_scope_name} "
        f"scope_fingerprint={actual_scope_fingerprint}"
      )

      if actual_scope_fingerprint != expected_scope_fingerprint:
        raise RuntimeError(
          "The refreshed elevata manifest describes a different load-scope "
          "task graph than the manifest used to parse this DAG. No dataset "
          "tasks were started. Wait for Airflow to reparse the updated "
          "manifest, then trigger a new DAG run. "
          f"load_scope={load_scope_name} "
          f"parsed={expected_scope_fingerprint} "
          f"refreshed={actual_scope_fingerprint}"
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
      run_plan_scope_args: list[str],
      expected_scope_mode: str,
      expected_scope_key: str,
      expected_dataset_keys: list[str],
      expected_root_dataset_keys: list[str] | None,
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
      plan_path.parent.mkdir(parents=True, exist_ok=True)

      command = shlex.split(str(elevata_cmd))
      if not command:
        raise RuntimeError(
          "ELEVATA_CMD does not contain an executable command."
        )

      command.extend([
        "elevata_run_plan",
        *run_plan_scope_args,
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
        f"airflow_run_id={run_id} "
        f"scope={expected_scope_key}"
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
        expected_scope_mode=expected_scope_mode,
        expected_scope_key=expected_scope_key,
        expected_dataset_keys=expected_dataset_keys,
        expected_root_dataset_keys=expected_root_dataset_keys,
      )

      print(
        "Execution Run Plan ready: "
        f"run_plan_id={run_plan_id} "
        f"batch_run_id={batch_run_id} "
        f"scope={expected_scope_key} "
        f"datasets={payload['dataset_count']} "
        f"decisions={payload.get('decision_counts')} "
        "fingerprint="
        f"{payload['run_plan_fingerprint']}"
      )

      return str(plan_path)

    start = EmptyOperator(task_id="start")
    manifest_guard = validate_manifest_contract(
      MANIFEST_PATH,
      load_scope_name,
      parsed_scope_fingerprint,
      MAX_AGE_HOURS,
    )
    run_plan_path = create_execution_run_plan(
      dag_id=dag.dag_id,
      run_id="{{ run_id }}",
      elevata_cmd=ELEVATA_CMD,
      profile_name=PROFILE,
      target_system_short=TARGET_SYSTEM,
      run_plan_dir=RUN_PLAN_DIR,
      run_plan_scope_args=run_plan_scope_args,
      expected_scope_mode=expected_scope_mode,
      expected_scope_key=expected_scope_key,
      expected_dataset_keys=target_dataset_keys,
      expected_root_dataset_keys=expected_root_dataset_keys,
    )
    generate_manifest >> manifest_guard >> run_plan_path >> start

    # Build a lookup by dataset name too (compatibility with older dependency refs).
    id_by_dataset: dict[str, str] = {}
    for nid, n in nodes.items():
      ds = (n or {}).get("dataset")
      if ds:
        id_by_dataset[str(ds)] = nid

    tasks_by_id: dict[str, BashOperator] = {}

    with TaskGroup(group_id="load_targets") as tg:
      # 1) Create only tasks from this resolved load scope.
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
            "ELEVATA_PROFILE": PROFILE,
            "ELEVATA_TARGET_SYSTEM": TARGET_SYSTEM,
            "ELEVATA_RUN_PLAN_PATH": RUN_PLAN_XCOM_PATH,
          },
          append_env=True,
        )

      # 2) Wire the same manifest execution dependencies for every load scope.
      for node_id in target_node_ids:
        node = nodes.get(node_id) or {}
        for up in _iter_upstreams(node):
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
        "ELEVATA_PROFILE": PROFILE,
        "ELEVATA_TARGET_SYSTEM": TARGET_SYSTEM,
        "ELEVATA_RUN_PLAN_PATH": RUN_PLAN_XCOM_PATH,
      },
      append_env=True,
    )

    start >> tg >> finalize_run_plan

  return dag


manifest = _load_manifest_if_present(MANIFEST_PATH)

if manifest is None:
  globals()["elevata_load"] = _build_missing_manifest_dag()
else:
  seen_dag_ids: set[str] = set()

  for load_scope in _manifest_load_scopes(manifest):
    dag_id = _dag_id_for_load_scope(str(load_scope["name"]))
    if dag_id in seen_dag_ids:
      raise RuntimeError(
        f"Multiple load scopes map to Airflow DAG id {dag_id!r}."
      )
    seen_dag_ids.add(dag_id)
    globals()[dag_id] = _build_elevata_load_dag(
      manifest=manifest,
      load_scope=load_scope,
    )
