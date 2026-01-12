# ⚙️ Load Execution & Orchestration Architecture

This document describes how elevata executes load operations:  
from dependency resolution and execution planning to orchestration, failure semantics,  
retries, and observability.

While the SQL generation pipeline focuses on *what* SQL is produced,  
the execution architecture focuses on *how* and *in which order* datasets are loaded,  
and how this process is observed and explained.

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

Execution is **not** embedded in SQL generation and **not** dialect-specific.  
Dialects remain adapters, never logic carriers.

---

## 🔧 2. Execution Model: Plan vs Execution

Execution in elevata is split into two explicit phases:

### 🧩 2.1 Execution Planning

An **ExecutionPlan** is a deterministic, declarative description of
*what should be executed* and *in which dependency order*.

The plan contains:

- A stable `batch_run_id`
- An ordered list of execution steps
- Dataset-level dependencies (upstream relationships)

The plan is derived from metadata only.
No SQL is rendered and no execution happens at this stage.

### 🧩 2.2 Execution

Execution consumes an ExecutionPlan and applies:

- Execution policies (fail-fast vs continue-on-error)
- Retry semantics
- Dependency blocking rules

Execution produces **results**, not SQL:
status, timing, attempts, and failure reasons.

---

## 🔧 3. Dependency Graph & Ordering

Dataset dependencies are resolved into a directed acyclic graph (DAG).

From this graph, elevata derives a **deterministic execution order**:

- Upstream datasets are always executed before downstream datasets
- Independent branches may be executed in parallel in the future
- The same metadata state always yields the same order

Dependency resolution errors are treated as **best-effort warnings**
and never block execution planning.

---

## 🔧 4. Execution Policies

Execution behavior is controlled by an explicit **ExecutionPolicy**.

Core policy parameters:

- `continue_on_error`
- `max_retries`

Policies apply globally to a run and are evaluated consistently
for all datasets.

There is no implicit behavior.
All execution semantics are explicit and predictable.

---

## 🔧 5. Retry & Failure Semantics

### 🧩 5.1 Retries

Retries apply **only in execute mode** (`--execute`).

- Dry-run failures are surfaced immediately
- Retries are counted per dataset
- `attempt_no` starts at 1 and is propagated to execution logic

Retries are **never hidden**:
each attempt is observable and logged.

### 🧩 5.2 Failure Handling

When a dataset fails after all retries:

- Its status becomes `error`
- Downstream behavior depends on the execution policy

---

## 🔧 6. Blocked vs Aborted

elevata distinguishes two fundamentally different non-success outcomes.

### 🧩 6.1 Blocked

A dataset is **blocked** if:

- One of its upstream dependencies failed
- The dataset itself was never attempted

Blocked datasets are reported as:

- `status = skipped`
- `kind = blocked`
- `blocked_by = <upstream dataset>`

This represents *dependency-based non-execution*.

### 🧩 6.2 Aborted (Fail-Fast)

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

## 🔧 7. Load Run Log (`meta.load_run_log`)

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

## 🔧 8. Load Run Snapshot (`meta.load_run_snapshot`)

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

### 🧩 8.1 Event vs State

| Aspect | Load Run Log | Load Run Snapshot |
|------|-------------|-------------------|
| Granularity | Dataset / Attempt | Batch Run |
| Nature | Event stream | State document |
| Purpose | Monitoring, auditing | Explainability, debugging |

Both are complementary and intentionally distinct.

---

## 🔧 9. Batch Runs & Multi-Dataset Loads

A single invocation of `elevata_load` may execute multiple datasets.

All datasets executed in one invocation share:

- The same `batch_run_id`
- The same execution policy
- The same snapshot

This enables:

- Consistent failure semantics
- Cross-dataset observability
- Future batch-level governance rules

---

## 🔧 10. Best-Effort Guarantees

Execution observability is **best-effort by design**.

- Logging and snapshot persistence must never block execution
- Meta-schema evolution is additive only
- Failures in observability are swallowed, not propagated

Execution correctness always takes precedence over observability.

---

## 🔧 11. CLI Integration (`elevata_load`)

The execution architecture is exposed through the CLI:

- `--execute` enables real execution
- `--continue-on-error` controls fail-fast behavior
- `--max-retries` controls retry behavior
- `--debug-execution` prints execution snapshots
- `--write-execution-snapshot` persists snapshots to disk

The CLI is an adapter.
All execution logic lives in the execution core.

---

## 🔧 12. Design Summary

The execution architecture of elevata is:

- Explicit, not implicit
- Deterministic, not heuristic
- Metadata-driven, not SQL-driven
- Observable by default
- Extensible without breaking changes

This provides a robust foundation for:
orchestration integrations, governance rules,
and execution analytics in future versions.

---

© 2025-2026 elevata Labs — Internal Technical Documentation