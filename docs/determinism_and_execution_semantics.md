# ⚙️ Determinism & Execution Semantics

This document defines elevata’s rules for **deterministic SQL generation and execution**.  
It applies to both standard generation and custom query logic (Query Trees).

---

## 🔧 1. Why determinism matters in elevata

elevata is built for reproducibility:

- SQL previews must match executed SQL  
- CI checks must be stable  
- the same metadata must produce the same output across runs  
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
- materialization planning  
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

## 🔧 4. Window functions

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

## 🔧 5. Aggregation determinism

Aggregations can become nondeterministic if result ordering is undefined in the aggregation semantics.

Rule patterns:

- Ordered aggregates (e.g. STRING_AGG) require explicit ORDER BY inside the function.
  Missing ordering → **ERROR** (or strict WARNING, depending on policy)

Other aggregates (SUM, COUNT, MIN, MAX, AVG) are deterministic without ordering.

---

## 🔧 6. Contract stability and collisions

The output contract must be stable and unambiguous.

Rules:

- Output column name collisions → **ERROR**  
- Missing inputs / disconnected tree → **ERROR**  
- Cycles in the Query Tree → **ERROR**

---

## 🔧 7. Why elevata is not a semantic layer

elevata does not implement query-time semantics (like BI semantic layers or metric stores).  
Instead, elevata materializes semantics into datasets deterministically:

- business logic belongs in bizcore  
- consumer shaping belongs in serving  
- execution is metadata-native and explainable via lineage + query contract

This avoids tool-specific logic and ensures reproducible pipelines.

---

## 🔧 8. References

- [Query Tree & Query Builder](query_builder_and_query_tree.md)    
- [Lineage Model & Logical Plan](logical_plan.md)   
- [Dialect System](dialect_system.md)

---

© 2025-2026 elevata Labs — Internal Technical Documentation
