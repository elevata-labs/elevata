# ⚙️ Load Execution & Orchestration Architecture

This document describes how elevata executes load operations: from dependency resolution and execution planning to orchestration, failure semantics, retries, and observability.

While the SQL generation pipeline focuses on *what* SQL is produced, the execution architecture focuses on *how* and *in which order* datasets are loaded, and how this process is observed and explained.

---

## 🔧 1. Overview

Load execution in elevata is a **first-class architectural concern**.

Key design goals:

- Deterministic execution order  
- Explicit separation of planning and execution  
- Clear failure and retry semantics  
- Non-destructive, best-effort behavior  
- Metadata-first observability  
- Dialect-agnostic orchestration logic

Execution is **not** embedded in SQL generation and **not** dialect-specific. Dialects remain adapters, never logic carriers.

---

## 🔧 2. Execution Model: Planning, Binding, and Execution

Execution in elevata is separated into explicit semantic stages.

### 🧩 2.1 Execution Planning

An **ExecutionPlan** is a deterministic, declarative description of *what should be executed* and *in which dependency order*.

The plan contains:

- a stable `batch_run_id`  
- an ordered list of execution steps  
- dataset-level dependencies

The plan is derived from metadata only. No SQL is rendered and no execution happens at this stage.

### 🧩 2.2 Execution

Execution consumes an ExecutionPlan and applies:

- execution policies (fail-fast vs continue-on-error)  
- retry semantics  
- dependency blocking rules

Execution produces **results**, not SQL: status, timing, attempts, and failure reasons.

### 🧩 2.3 Execution Impact Plan

Architecture Control evaluates the active execution scope before a controlled or scheduler-managed run starts.

The read-only **Execution Impact Plan** classifies every active dataset as:

- `REUSE`  
- `INCREMENTAL_EXECUTE`  
- `FULL_REBUILD`  
- `REVALIDATE`  
- `BLOCKED`

Impact decisions are derived from current Architecture State, baseline state, Architecture Change Report, review status, previous execution evidence, materialization semantics, and dependency propagation.

`REVALIDATE` and `BLOCKED` are closed gates. They cannot become executable scheduler decisions.

### 🧩 2.4 Immutable Execution Run Plan

An **Execution Run Plan** is the public scheduler contract for one exact run.

It binds:

- profile and target system  
- Architecture Control scope and dependency mode  
- review status and approval identifier  
- Architecture State and report fingerprints  
- Execution Impact, Execution Preview, and ExecutionPlan fingerprints  
- exact active root datasets  
- exact ordered dataset decisions

The Run Plan contains only active TargetDatasets and executable decisions. Its identifier may be reused only when the canonical fingerprint is unchanged; another payload cannot silently replace it.

The internal ExecutionPlan answers how datasets are ordered. The Execution Run Plan additionally answers which reviewed architecture, impact decisions, and runtime context authorized that exact scheduler run.

### 🧩 2.5 Planned Architecture State and Finalization

Every newly created Run Plan is stored together with a matching **Planned Architecture State** snapshot.

Before a scheduler step executes, elevata validates:

- Run Plan runtime context  
- dataset membership and decision  
- dataset metadata against the planned snapshot

Each step writes one structured outcome artifact. Finalization requires one semantically successful outcome for every planned dataset and then persists exactly the Planned Architecture State applied by that run.

Metadata changes made after Run Plan creation are reported as post-plan drift. They do not redefine the active run and do not block successful finalization of the older architecture that was actually applied. The changed metadata belongs to a new Run Plan and a new scheduler run.

When no recorded state exists, a first state can be established only after full-scope physical discovery verifies an empty managed target. Legacy interrupted initial deployments without a Planned Architecture State snapshot can be recovered only through an explicit, strictly validated recovery path.

### 🧩 2.6 Controlled Target Generation Boundary

Controlled Target Generation occurs before execution planning. It creates and applies TargetDataset and TargetColumn metadata through immutable schema-scoped plans, Source-to-Target reviews, optional or required Generation Approval, and drift-guarded apply.

Target Generation Plans do not contain load steps and do not authorize physical execution. The generated-layer sequence must converge before the resulting Architecture State and Architecture Change Report are treated as the current execution-review basis.

Generation Approval is not carried into an Execution Run Plan. Only the later Architecture Approval associated with the resulting Architecture Change Report can become part of execution review context.

### 🧩 2.7 Architecture Control Plane

The Architecture Control Plane provides controlled target generation, architecture review, approval, Execution Impact, execution preview, immutable scheduler planning, finalization, and execution audit workflows around architecture state and schema evolution intent.

Execution remains delegated to the load runner. The Architecture Control UI and scheduler commands do not bypass preflight validation, materialization policy checks, or Architecture Guard enforcement.

The control plane commands are:

| Command | Purpose |
|---|---|
| `generate_targets --dry-run` | Render a Target Generation Plan and Review before metadata mutation |
| `elevata_generation_approve` | Approve one exact Target Generation Review |
| `generate_targets --plan-file` | Apply one exact Target Generation Plan with drift guards |
| `elevata_state` | Render the metadata-defined architecture state |
| `elevata_plan` | Render an architecture change report |
| `elevata_promote` | Compare two architecture state artifacts |
| `elevata_approve` | Create an approval artifact |
| `elevata_approval_check` | Verify an approval artifact |
| `elevata_run_plan` | Create an immutable scheduler Run Plan and Planned Architecture State |
| `elevata_finalize_run_plan` | Validate scheduler outcomes and persist the planned applied state |

The load runner remains responsible for execution safety. Before executing load SQL, it performs preflight validation, derives materialization steps from the MigrationPlan, applies policy checks, and blocks unsafe execution.

This separation allows architecture review and operational execution to use the same semantic basis while keeping execution guardrails inside the load runner.

Architecture Control Plane flow:

```text
Architecture State
  ↓
Architecture Diff
  ↓
MigrationPlan
  ↓
Policy Decisions
  ↓
Architecture Report
  ↓
Approval Artifact
  ↓
Execution Impact Plan
  ↓
Execution Run Plan + Planned Architecture State
  ↓
Structured Dataset Outcomes
  ↓
Finalization
  ↓
Applied Architecture State + Architecture Execution Record
```

---

## 🔧 3. Materialization Preflight Stage

In elevata, materialization is split into two distinct phases:

1. Planning (Preflight)  
2. Application (Execution)

### 🧩 Planning Phase

During planning, elevata:

- introspects the existing table structure  
- compares desired and actual schemas  
- derives deterministic schema evolution steps from the Architecture MigrationPlan  
- classifies schema differences  
- produces deterministic drift findings and validates schema evolution intent (MigrationPlan)

The plan contains:

- ordered materialization steps  
- warnings  
- blocking errors

No changes are applied during this phase.

### 🧩 Application Phase

Only after successful preflight validation:

- deterministic schema evolution steps (derived from the Architecture MigrationPlan) are executed  
- schema changes are applied via dialect-rendered DDL  
- execution continues to load SQL

This separation ensures deterministic execution behavior across platforms.

### 🧩 Hist Dataset Synchronization

For rawcore datasets with historization enabled:

- base dataset materialization is planned first  
- corresponding `_hist` dataset schema is synchronized afterwards  
- synchronization is best-effort and does not block base execution

The `_hist` synchronization uses the same schema evolution intent (MigrationPlan) so base and history schemas stay consistent and lineage-safe.

---

## 🔧 4. Dependency Graph & Ordering

Dataset dependencies are resolved into a directed acyclic graph (DAG).

From this graph, elevata derives a **deterministic execution order**:

- Upstream datasets are always executed before downstream datasets  
- Independent branches may be executed in parallel in the future  
- The same metadata state always yields the same order

Dependency resolution errors are treated as **best-effort warnings** and never block execution planning.

---

## 🔧 5. Execution Policies

Execution behavior is controlled by an explicit **ExecutionPolicy**.

Core policy parameters:

- `continue_on_error`  
- `max_retries`

Policies apply globally to a run and are evaluated consistently for all datasets.

There is no implicit behavior. All execution semantics are explicit and predictable.

---

## 🔧 6. Retry & Failure Semantics

### 🧩 6.1 Retries

Retries apply **only in execute mode** (`--execute`).

- Dry-run failures are surfaced immediately  
- Retries are counted per dataset  
- `attempt_no` starts at 1 and is propagated to execution logic

Retries are **never hidden**: each attempt is observable and logged.

### 🧩 6.2 Failure Handling

When a dataset fails after all retries:

- Its status becomes `error`  
- Downstream behavior depends on the execution policy

---

## 🔧 7. Blocked vs Aborted

elevata distinguishes two fundamentally different non-success outcomes.

### 🧩 7.1 Blocked

A dataset is **blocked** if:

- One of its upstream dependencies failed  
- The dataset itself was never attempted

Blocked datasets are reported as:

- `status = skipped`  
- `kind = blocked`  
- `blocked_by = <upstream dataset>`

This represents *dependency-based non-execution*.

### 🧩 7.2 Aborted (Fail-Fast)

A dataset is **aborted** if:

- Execution stops early due to `continue_on_error = false`  
- The dataset was not attempted due to fail-fast semantics

Aborted datasets are reported as:

- `status = skipped`  
- `kind = aborted`  
- `status_reason = fail_fast_abort`

This represents *policy-based non-execution*.

Blocked and aborted are intentionally distinct and never conflated.

---

## 🔧 8. Load Run Log (`meta.load_run_log`)

The **load run log** is an append-only, event-level record of execution.

Characteristics:

- Dataset- and attempt-granular  
- One row per execution attempt or orchestration event  
- Operational and time-oriented

Typical events:

- Successful dataset execution  
- Failed attempts  
- Blocked datasets  
- Aborted datasets

The log answers the question:

> *What happened, step by step, during this load run?*

---

## 🔧 9. Load Run Snapshot (`meta.load_run_snapshot`)

The **load run snapshot** captures the declarative state of a load run.

Characteristics:

- One row per batch run  
- JSON-based snapshot document  
- Explains *why* execution behaved the way it did

The snapshot includes:

- Execution plan  
- Execution policy  
- Dependency structure  
- Aggregated outcomes  
- Failure reasons and counts

The snapshot answers the question:

> *What did this load run look like as a whole?*

### 🧩 9.1 Event vs State

| Aspect | Load Run Log | Load Run Snapshot |
|------|-------------|-------------------|
| Granularity | Dataset / Attempt | Batch Run |
| Nature | Event stream | State document |
| Purpose | Monitoring, auditing | Explainability, debugging |

Both are complementary and intentionally distinct.

---

## 🔧 10. Batch Runs & Multi-Dataset Loads

A single invocation of `elevata_load` may execute multiple datasets.

All datasets executed in one invocation share:

- the same `batch_run_id`  
- the same execution policy  
- the same snapshot

For scheduler-managed execution, every dataset step also shares one immutable Execution Run Plan and Planned Architecture State. The scheduler writes one structured outcome per planned dataset, and finalization closes the batch only when the complete expected outcome set is valid.

Execution manifests and Run Plans are active-only. Inactive generated TargetDatasets may remain visible in review history, but they are not scheduler tasks and cannot be executed implicitly.

This enables:

- consistent failure semantics  
- cross-dataset observability  
- batch-level governance rules  
- exact architecture-to-run binding  
- deterministic state advancement

---

## 🔧 11. Best-Effort Guarantees

Execution observability is **best-effort by design**.

- Logging and snapshot persistence must never block execution  
- Meta-schema evolution is additive only  
- Failures in observability are swallowed, not propagated

Execution correctness always takes precedence over observability.

### 🧩 11.1 Batch-scoped Runtime State

The load runner keeps batch-scoped runtime state for best-effort provisioning and observability helpers.

This state records which schema, `meta.load_run_log`, and `meta.load_run_snapshot` ensure operations were already requested for the current batch context. It reduces repeated ensure calls across SQL loads, RAW ingestion, orchestration-only log rows, and snapshot persistence without changing load semantics.

The runtime state does not decide what should be executed. Execution order, retry behavior, dependency blocking, load logging granularity, and snapshot content remain derived from the execution plan and policy.

Snapshot file persistence, warehouse snapshot persistence, and orchestration-only log rows are handled through best-effort helpers so observability failures stay isolated from load correctness.

---

## 🔧 12. CLI Integration (`elevata_load`)

The execution architecture is exposed through the CLI:

- `--execute` enables real execution  
- `--continue-on-error` controls fail-fast behavior  
- `--max-retries` controls retry behavior  
- `--debug-execution` prints execution snapshots  
- `--write-execution-snapshot` persists snapshots to disk  
- `--run-plan` binds one dataset task to an immutable scheduler Run Plan and requires `--execute --no-deps`

Scheduler-facing commands:

```bash
python manage.py elevata_run_plan --all-datasets
python manage.py elevata_finalize_run_plan <RUN_PLAN_PATH>
```

`elevata_run_plan --reuse-existing` is intended for scheduler retries and succeeds only when the existing immutable artifact matches the requested runtime and scope. Metadata corrections require a new Run Plan and a new scheduler run.

The CLI is an adapter. All execution logic lives in the execution core.

---

## 🔧 13. Architecture Control Execution

Architecture Control execution is a constrained UI path into the same load runner. Controlled Target Generation is the preceding metadata-control phase and must converge separately before physical execution review.

It provides:

- scope-aware Execution Impact  
- scope-aware execution preview  
- approval-aware execution gating  
- Architecture Guard enforcement  
- controlled execution output capture  
- Architecture Execution Record creation  
- Architecture Execution Record history and retention

Architecture Control supports the following execution scopes:

| Scope | Dependency behavior |
|---|---|
| All datasets | Executes all active target datasets with dependency ordering |
| Schema | Executes selected active schema roots with dependency ordering |
| TargetDataset | Executes the selected active TargetDataset with dependency ordering |
| TargetDataset, target-only | Executes only the selected active TargetDataset |

Schema review may include a previous-state dataset that has just been retired so the metadata-only removal remains visible and approvable. Execution resolution remains active-only, and inactive datasets are excluded from Execution Impact, preview steps, manifests, and Run Plans.

Target-only execution is restricted to TargetDataset scopes. It supports focused iteration while keeping the default execution path lineage-aware.

### 🧩 13.1 Architecture Execution Record

An Architecture Execution Record captures the audit context of a controlled UI execution.

It contains:

- execution identifier  
- operator  
- timestamps and duration  
- status and message  
- Architecture Control scope  
- dependency mode  
- report fingerprint  
- approval identifier  
- preview fingerprint  
- command invocation metadata  
- output and error tails  
- output and error line counts  
- record fingerprint

Architecture Execution Records are stored as JSON artifacts under:

```bash
ELEVATA_ARCH_EXECUTION_DIR=.elevata/executions
```

Architecture Control can list stored records, filter them by scope, status, date range and dependency mode, show record details, download record JSON, and remove older stored records through retention cleanup.

They complement the load run log and load run snapshot:

| Artifact | Granularity | Purpose |
|---|---|---|
| Load Run Log | Dataset / attempt | Operational event stream |
| Load Run Snapshot | Batch run | Execution state and outcome summary |
| Architecture Execution Record | Architecture Control execution | Review, approval, scope, command and audit reference |

---

## 🔧 14. Design Summary

The execution architecture of elevata is:

- explicit, not implicit  
- deterministic, not heuristic  
- metadata-driven, not SQL-driven  
- bound to immutable scheduler contracts  
- active-only at execution time  
- observable by default  
- explicit about batch-scoped runtime state  
- exact about applied-state finalization  
- extensible without breaking changes

This provides a robust foundation for orchestration integrations, governance rules, execution analytics, and recoverable scheduler operation.

---

© 2025-2026 elevata - Technical Documentation