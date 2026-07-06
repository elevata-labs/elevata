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

> **Note**  
> This document focuses exclusively on SQL generation and rendering.  
> Execution order, orchestration semantics, retries, failure handling, and execution observability are described separately in [Load Execution & Orchestration Architecture](load_execution_architecture.md).

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

## 🔧 5. Intent layer (generation + ingestion)

elevata uses a small "intent layer" to keep core decisions consistent across target generation, ingestion, drift detection and quality checks.

### 🧩 Landing intent
The function `landing_required(SourceDataset)` determines whether a dataset conceptually requires a RAW landing object.

Decision rules:

- `SourceDataset.integrate` must be True (hard gate).  
- `SourceDataset.generate_raw_table` overrides the system default.  
- If unset, inherit `System.generate_raw_tables`.  

This decision is shared across:

- RAW target generation  
- ingestion execution planning  
- future drift detection and quality checks

### 🧩 Ingestion mode
The function `resolve_ingest_mode(SourceDataset)` determines how RAW is populated:

- `native`: elevata extracts and loads  
- `external`: an external tool populates RAW; elevata validates + continues  
- `none`: no landing ingestion (federated/virtual access)  

If RAW landing is required but `include_ingest='none'`, configuration is inconsistent and must fail fast.

### 🧩 Allowed states (RAW landing × include_ingest)

The decision whether a RAW landing exists is driven by `landing_required(SourceDataset)`. The execution mode is driven by `System.include_ingest`, but only becomes relevant when RAW landing is required.

| landing_required | include_ingest | Result / behavior |
|-----------------|----------------|-------------------|
| false           | none           | ✅ Valid. No RAW landing. Ingestion is skipped (federated/virtual or intentionally no landing). |
| false           | external       | ✅ Valid (ignored). No RAW landing, so ingestion is skipped. |
| false           | native         | ✅ Valid (ignored). No RAW landing, so ingestion is skipped. |
| true            | native         | ✅ Valid. elevata performs native ingestion (extract + load into RAW). |
| true            | external       | ✅ Valid. RAW is expected to be populated by an external tool; elevata validates RAW existence and continues with downstream steps (drift/quality/etc.). |
| true            | none           | ❌ Invalid. Configuration inconsistency: RAW landing is required but no ingestion mode is enabled. Must fail fast. |

Notes:

- `include_ingest` is only actionable when `landing_required=True`.  
- For `include_ingest=external`, elevata does not extract data but still logs runs and can run drift/quality checks on RAW.

---

## 🔧 6. Load Runner

The **Load Runner CLI** (`elevata_load`) orchestrates SQL generation and execution.

It:

- resolves the active profile and target system  
- reads target dataset metadata  
- constructs the logical plan  
- renders SQL via the active dialect  
- optionally executes SQL in the target warehouse  

The same pipeline is used for SQL preview and execution.

---

## 🔧 7. Deterministic Generation

The SQL generation pipeline is fully deterministic:

- stable business-key ordering  
- stable hashing patterns  
- stable helper column naming  
- stable logical plan structure  

This guarantees reproducible SQL and predictable diffs.

---

## 🔧 8. Merge‑based Incremental SQL Generation (Rawcore)

This section documents how merge‑based incremental loads are implemented for Rawcore targets.

### 🧩 8.1 Source Resolution

For targets using `incremental_strategy = "merge"`, the SQL layer resolves the Stage upstream dataset as the merge source:

- source: `stage.<table> AS s`  
- target: `rawcore.<table> AS t`  

Lineage metadata guarantees compatible natural keys and attribute sets.

### 🧩 8.2 Natural Key Join

Natural key fields define:

- the merge join condition  
- identification of new vs. existing rows  
- delete‑detection scope  

If no natural key is defined, SQL generation fails.

### 🧩 8.3 Logical Plan Reuse

All column expressions used in UPDATE and INSERT branches are reused from the logical plan.

Business logic is defined once and rendered consistently.

### 🧩 8.4 Dialect‑dependent Strategy

Dialects choose between:

- native `MERGE` statements  
- fallback `UPDATE` + `INSERT ... WHERE NOT EXISTS` patterns  

Both paths reuse the same logical plan expressions.

### 🧩 8.5 Delete Detection

Delete detection is implemented as a separate anti‑join statement that runs before the merge.

The SQL layer translates incremental scope filters from source lineage into target column expressions.

The incremental scope used for delete detection is derived from the SourceDataset.increment_filter and translated via lineage into target column references.


---

## 🔧 9. Execution Semantics

Execution semantics are defined by target layer:

| Layer     | Behaviour |
|----------|-----------|
| `raw`    | Replace when loaded or seeded |
| `stage`  | Always replace (truncate before insert) |
| `rawcore`| Replace only when `mode = full`; incremental runs never truncate |
| `*_hist` | Never truncate; versioned updates only |

Execution always runs **inside the target system**.

---

## 🔧 10. Controlled Reference Completion

Controlled Reference Completion is rendered as deterministic load-time SQL for modeled rawcore references.

The execution order is intentionally stable:

```text
1. Ensure the loaded dataset's own default member where applicable  
2. Ensure referenced parent default members for enabled fallback references  
3. Insert inferred parent members for complete missing parent keys where enabled  
4. Map still-unresolved child reference keys to the parent default member where enabled
```

This order preserves the reference priority:

```text
real parent > inferred parent > default member > unresolved
```

Default Member Fallback is controlled by `default_member_fallback_enabled` on the modeled `TargetDatasetReference`. Enabling `inferred_members_enabled` automatically enables the fallback as a modeling default, but users can disable the fallback explicitly afterwards.

Fallback updates do not create parent rows. They update child reference keys only when no matching parent row exists after real and inferred parents have been considered. Null, empty or blank values in any modeled reference component make a composite reference incomplete for fallback purposes unless the calculated child reference key still matches an existing parent row. Business-invalid but technically complete values are not interpreted by this runtime feature; they remain candidates for inferred members when enabled or for separate quality checks.

Controlled reference completion also contributes runtime execution dependencies. When inferred members or Default Member Fallback are enabled for a reference, the referenced parent dataset is scheduled before the child dataset. This strengthens execution order without turning the reference into semantic lineage.

---

## 🔧 11. Execution, Auto‑Provisioning & Warehouse Logging

### 🧩 11.1 Execution Modes

`elevata_load` supports:

- **Dry‑run**: render SQL without executing it  
- **Execute** (`--execute`): render and execute SQL in the target warehouse

### 🧩 11.2 Layer-aware execution (RAW = ingestion)

`elevata_load --execute` is intentionally **layer-aware**:

- For `raw` targets, `--execute` does not produce SQL but triggers **ingestion** logic instead. The same execution command therefore has different semantics depending on the target layer.
- For downstream layers (`stage`, `rawcore`, `*_hist`), `--execute` renders and executes **warehouse-native SQL** as usual.

Why this matters:

- elevata treats **ingestion as a first-class citizen** of the pipeline.  
- The same lineage metadata that drives target generation also drives ingestion planning.  
- This closes an important gap in dbt-style stacks: dbt excels at transformations but does not provide ingestion as part of its core execution model.

Practical rule:

- If you can `--execute` a RAW table, elevata will bring the data in (native/external mode).  
- If you `--execute` a Stage model, elevata assumes that its upstream exists inside the target execution context  
  (RAW landing or federated/external availability), and will fail fast otherwise.

#### 🔎 Ingestion configuration (`ingestion_config`)

For RAW ingestion, source connection details and behavior are defined via `SourceDataset.ingestion_config`.

- For relational sources, scoping is controlled via `static_filter` and `increment_filter`.  
- For non-relational sources (Files / REST), `ingestion_config` contains connector-specific parameters (e.g. `uri` or `url`).

Regardless of source type, RAW ingestion is always executed as **Full Replace**:

- Drop (if supported)  
- Create  
- Truncate  
- Insert

### 🧩 11.3 Auto‑Provisioning

When enabled, execution automatically provisions:

- target schemas  
- the meta schema  
- the `load_run_log` table  

All DDL is idempotent.

### 🧩 11.4 Warehouse‑level Load Run Log

Each executed load writes a row into `meta.load_run_log`, capturing:

- batch and load run IDs  
- target dataset and system  
- load mode and flags  
- timestamps and durations  
- execution status and error details  

This enables warehouse‑native observability and auditing.

### 🧩 11.5 Schema Evolution (MigrationPlan-driven)

Before executing generated DML, elevata applies deterministic **schema evolution steps** that align physical target tables with metadata-defined schemas:

- `ENSURE_SCHEMA` (idempotent)  
- `RENAME TABLE` (dataset rename via `former_names`)  
- `RENAME COLUMN` (column rename via `former_names`)  
- `ADD COLUMN` (when safe and supported)  
- `DROP COLUMN` (policy-gated; disabled by default)  
    - Base tables: enabled via `ELEVATA_ALLOW_AUTO_DROP_COLUMNS=true`  
    - `_hist` tables: enabled via `ELEVATA_ALLOW_AUTO_DROP_HIST_COLUMNS=true`  

Schema evolution steps are derived from the **Architecture MigrationPlan** and executed by the applier. Destructive operations remain policy-gated and are only executed when explicitly enabled.

Table creation remains the responsibility of the load runner via `ensure_target_table(...)`.

---

## 🔧 12. Load Observability & Debugging

Load runs expose structured summaries, batch grouping, and CLI‑level logging to support debugging and monitoring.

---

## 🔧 13. CLI Usage

The `elevata_load` command supports preview, debugging, batch execution, and warehouse execution.

---

## 🔧 14. Execute Mode

The `--execute` flag enables direct execution of load SQL in the target warehouse via dialect‑specific execution engines.

---

© 2025-2026 elevata - Technical Documentation
