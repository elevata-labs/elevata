# ⚙️ Determinism & Execution Semantics

This document defines elevata’s rules for **deterministic SQL generation and execution**.  
It applies to both standard generation and custom query logic (Query Trees).

---

## 🔧 1. Why determinism matters in elevata

elevata is built for reproducibility:

- SQL previews must match executed SQL  
- CI checks must be stable  
- the same metadata must produce the same output across runs  
- architecture reports must produce stable fingerprints  
- architecture execution records must preserve stable audit references  
- multi-dialect rendering must not introduce semantic drift

Determinism is therefore treated as a correctness requirement, not a “best practice”.

---

## 🔧 2. Determinism model: errors vs warnings

elevata classifies findings as:

- **ERROR (blocking):** execution is ambiguous or unsafe  
- **WARNING (advisory):** execution is valid, but quality or semantics may be degraded

The Query Builder UI surfaces this via:

- deterministic / needs ordering badges  
- error/warning counts

---

## 🔧 3. Preflight Validation Phase

elevata includes a preflight validation phase executed before any DDL or DML statements are applied.

The preflight phase guarantees that execution behavior is fully predictable.

### 🧩 Responsibilities

The preflight phase performs:

- schema introspection  
- schema evolution planning (MigrationPlan → deterministic DDL steps)  
- type drift detection  
- validation of blocking conditions  
- execution safety checks

No SQL affecting data or schema is executed before preflight completes successfully.

### 🧩 Deterministic Failure Modes

Execution may fail during preflight when:

- unsafe schema evolution is required  
- narrowing or incompatible type drift is detected  
- required dialect capabilities are missing  
- metadata inconsistencies are found

Failures always occur before execution starts.

This guarantees:

- no partially applied schema changes  
- no partial data loads  
- reproducible execution behavior.

### 🧩 Full Refresh Exception

Datasets using full refresh materialization are exempt from type drift blocking  
because the table is recreated during execution.

Type drift warnings may still be emitted for visibility.

---

## 🔧 4. Architecture Report Determinism

Architecture reports and approval artifacts are deterministic artifacts.

The same architecture state, scope, migration intent, and policy configuration  
produce the same report fingerprint.

Deterministic report artifacts include:

- Architecture State fingerprint  
- Architecture Change Report fingerprint  
- Architecture Promotion Report fingerprint  
- Architecture Approval Artifact fingerprint  
- Architecture Execution Record fingerprint

These fingerprints are derived from canonical JSON representations.

Approval artifact fingerprints are derived from the approval payload and review decision.  
They bind an approval to one exact Architecture Change Report reference.

Report JSON uses stable ordering and contains semantic architecture information:

- scope  
- architecture state fingerprints  
- dataset changes  
- column changes  
- MigrationPlan actions  
- policy decisions

The selected report scope is part of the deterministic artifact contract.  
For scoped reports, dataset changes, column changes, MigrationPlan actions, policy decisions  
and summary counts are restricted to the selected scope.

The reports do not require SQL rendering, warehouse introspection, or execution
engines.

Approval artifacts do not alter execution policy. A matching approval confirms that an Architecture Change Report  
was reviewed, while load execution remains protected by preflight checks, policy decisions and materialization guardrails.

Architecture Control displays the resulting review state, execution readiness, dependency mode,  
controlled execution output, and Architecture Execution Record.

---

## 🔧 5. Architecture Control Execution Semantics

Architecture Control provides a constrained UI path into the same deterministic load runner.

It does not bypass:

- preflight validation  
- schema evolution policy checks  
- Architecture Guard enforcement  
- approval matching  
- dialect-owned SQL rendering

The Architecture Control UI exposes controlled execution scopes:

| Scope | Dependency behavior |
|---|---|
| All datasets | Executes all active target datasets with dependency ordering |
| Schema | Executes selected schema roots with dependency ordering |
| TargetDataset | Executes the selected TargetDataset with dependency ordering |
| TargetDataset, target-only | Executes only the selected TargetDataset |

The default execution path remains lineage-aware.

Target-only execution is restricted to TargetDataset scopes and is intended for focused iteration  
when upstream data is already available.

Controlled execution produces an Architecture Execution Record.

The record captures:

- execution identifier  
- operator  
- timestamps and duration  
- execution status  
- Architecture Control scope  
- dependency mode  
- report fingerprint  
- approval identifier  
- preview fingerprint  
- command invocation metadata  
- output and error tails  
- output and error line counts  
- deterministic record fingerprint

Architecture Execution Records are deterministic audit artifacts. They answer:

```text
Who executed which controlled architecture scope, under which review and dependency context, and what happened?
```

The record fingerprint is derived from the canonical JSON representation of the execution record.

---

## 🔧 6. Window functions

Some window functions are inherently nondeterministic without ordering.

Rule:

- If a window function requires ordering, an ORDER BY clause is mandatory.
  Missing ORDER BY → **ERROR**

Examples of functions requiring ORDER BY:

- ROW_NUMBER, RANK, DENSE_RANK  
- LAG, LEAD  
- FIRST_VALUE, LAST_VALUE, NTH_VALUE  
- NTILE

Windowed aggregates (SUM/AVG/…) may not require ORDER BY:

- missing ORDER BY is usually ok → optional warning depending on policy

---

## 🔧 7. Aggregation determinism

Aggregations can become nondeterministic if result ordering is undefined in the aggregation semantics.

Rule patterns:

- Ordered aggregates (e.g. STRING_AGG) require explicit ORDER BY inside the function.
  Missing ordering → **ERROR** (or strict WARNING, depending on policy)

Other aggregates (SUM, COUNT, MIN, MAX, AVG) are deterministic without ordering.

---

## 🔧 8. Contract stability and collisions

The output contract must be stable and unambiguous.

Rules:

- Output column name collisions → **ERROR**  
- Missing inputs / disconnected tree → **ERROR**  
- Cycles in the Query Tree → **ERROR**

---

## 🔧 9. Why elevata is not a semantic layer

elevata does not implement query-time semantics (like BI semantic layers or metric stores).  
Instead, elevata materializes semantics into datasets deterministically:

- business logic belongs in bizcore  
- consumer shaping belongs in serving  
- execution is metadata-native and explainable via lineage + query contract

This avoids tool-specific logic and ensures reproducible pipelines.

---

## 🔧 10. References

- [Query Tree & Query Builder](query_builder_and_query_tree.md)    
- [Lineage Model & Logical Plan](logical_plan.md)   
- [Dialect System](dialect_system.md)

---

© 2025-2026 elevata Labs — Internal Technical Documentation
