# ⚙️ Load SQL Architecture

This document describes how elevata transforms metadata into executable SQL for load operations.

---

## 🔧 1. Overview

The load SQL pipeline turns metadata into **complete SQL statements** suitable for execution on analytical backends.  

High‑level flow:

```
Metadata → Logical Plan → Expression AST → Dialect Rendering → SQL Load Statement → Execution
```

The system is designed so that:  
- metadata remains backend‑agnostic  
- logical plans describe *what* is required, not *how* it is written  
- dialects encapsulate syntactic differences  
- execution engines are decoupled from SQL generation

---

## 🔧 2. Logical Plans for Load Operations

elevata represents load operations using SQL primitives:  

- `LogicalSelect` – core building block  
- `LogicalUnion` – multi-source resolution  
- `SubquerySource` – ranking, filtering, pre‑aggregation  

Logical plans are intentionally **dialect‑neutral**.

---

## 🔧 3. Expression AST in Load SQL

Every column expression is represented as an AST node during load generation.  

Supported node types include:  
- `ColumnRef`  
- `Literal`  
- `ExprRef`  
- `ConcatExpr`  
- `ConcatWsExpr`  
- `CoalesceExpr`  
- `WindowFunctionExpr`  
- `Hash256Expr`  

The AST guarantees that:  
- hashing is consistent across dialects  
- CONCAT / COALESCE behave uniformly  
- window functions are structured, not string‑built  
- SQL remains predictable and comparable

---

## 🔧 4. Dialect Rendering

After a logical plan is constructed, the selected dialect renders the plan and its AST into concrete SQL.

```
sql = dialect.render_select(plan)
```

Each dialect implements:  
- identifier quoting  
- literal rendering  
- hashing functions  
- CONCAT / CONCAT_WS  
- COALESCE  
- window functions  
- subqueries and unions  

This ensures consistent semantics while using native SQL syntax per backend.

---

## 🔧 5. Load Runner

The **Load Runner CLI** (`elevata_load`) orchestrates SQL generation and execution.  

It:  
- resolves the active profile and target system  
- reads target dataset metadata  
- constructs the logical plan  
- renders SQL via the active dialect  
- optionally executes SQL in the target warehouse  

The same pipeline is used for SQL preview and execution.

---

## 🔧 6. Deterministic Generation

The SQL generation pipeline is fully deterministic:  
- stable business-key ordering  
- stable hashing patterns  
- stable helper column naming  
- stable logical plan structure  

This guarantees reproducible SQL and predictable diffs.

---

## 🔧 7. Merge‑based Incremental SQL Generation (Rawcore)

This section documents how merge‑based incremental loads are implemented for Rawcore targets.

### 🧩 7.1 Source Resolution

For targets using `incremental_strategy = "merge"`, the SQL layer resolves the Stage upstream dataset as the merge source:  

- source: `stage.<table> AS s`  
- target: `rawcore.<table> AS t`  

Lineage metadata guarantees compatible natural keys and attribute sets.

### 🧩 7.2 Natural Key Join

Natural key fields define:  
- the merge join condition  
- identification of new vs. existing rows  
- delete‑detection scope  

If no natural key is defined, SQL generation fails.

### 🧩 7.3 Logical Plan Reuse

All column expressions used in UPDATE and INSERT branches are reused from the logical plan.  

Business logic is defined once and rendered consistently.

### 🧩 7.4 Dialect‑dependent Strategy

Dialects choose between:  
- native `MERGE` statements  
- fallback `UPDATE` + `INSERT ... WHERE NOT EXISTS` patterns  

Both paths reuse the same logical plan expressions.

### 🧩 7.5 Delete Detection

Delete detection is implemented as a separate anti‑join statement that runs before the merge.  

The SQL layer translates incremental scope filters from source lineage into target column expressions.

---

## 🔧 8. Execution Semantics

Execution semantics are defined by target layer:  

| Layer     | Behaviour |
|----------|-----------|
| `raw`    | Replace when loaded or seeded |
| `stage`  | Always replace (truncate before insert) |
| `rawcore`| Replace only when `mode = full`; incremental runs never truncate |
| `*_hist` | Never truncate; versioned updates only |

Execution always runs **inside the target system**.

---

## 🔧 9. Execution, Auto‑Provisioning & Warehouse Logging

### 🧩 9.1 Execution Modes

`elevata_load` supports:  

- **Dry‑run**: render SQL without executing it  
- **Execute** (`--execute`): render and execute SQL in the target warehouse

### 🧩 9.2 Auto‑Provisioning

When enabled, execution automatically provisions:  
- target schemas  
- the meta schema  
- the `load_run_log` table  

All DDL is idempotent.

### 🧩 9.3 Warehouse‑level Load Run Log

Each executed load writes a row into `meta.load_run_log`, capturing:  
- batch and load run IDs  
- target dataset and system  
- load mode and flags  
- timestamps and durations  
- execution status and error details  

This enables warehouse‑native observability and auditing.

---

## 🔧 10. Load Observability & Debugging

Load runs expose structured summaries, batch grouping, and CLI‑level logging to support debugging and monitoring.

---

## 🔧 11. CLI Usage

The `elevata_load` command supports preview, debugging, batch execution, and warehouse execution.

---

## 🔧 12. Execute Mode

The `--execute` flag enables direct execution of load SQL in the target warehouse via dialect‑specific execution engines.

---

© 2025 elevata Labs — Internal Technical Documentation
