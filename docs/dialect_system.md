# ⚙️ SQL Dialect System

This document describes the **SQL Dialect System**, including:  

- the unified `SqlDialect` base class  
- the dialect registry & factory  
- runtime dialect selection (profiles, environment, UI)  
- DuckDB, Postgres, and MSSQL implementations  
- rendering rules for identifiers, literals, expressions, window functions, and subqueries  
- hashing and string operations via the Expression DSL & AST

---

## 🔧 1. Role of the Dialect System

The Dialect System abstracts all SQL-vendor differences.  

High-level pipeline:

```text
LogicalPlan  →  Expression AST  →  SqlDialect renderer  →  final SQL string
```

Each dialect is responsible for:  

- identifier quoting  - literal rendering  
- expression rendering (DSL AST)  
- function names and argument patterns  
- window functions  
- subqueries and UNIONs  

The **LogicalPlan** and **Expression AST** are vendor-neutral. Only `SqlDialect` knows how to turn them into concrete SQL.

---

## 🔧 2. `SqlDialect` Base Class

All dialects extend a shared base class, conceptually:

```python
class SqlDialect:
    DIALECT_NAME: str

    def render_expr(self, expr: Expr) -> str:
        ...

    def render_select(self, select: LogicalSelect) -> str:
        ...

    def cast_expression(self, expr: Expr, target_type: str) -> str:
        ...
```

Every dialect must implement at least:  

- `render_expr(expr)` – render Expression AST nodes  
- `render_select(select)` – render LogicalSelect / LogicalUnion / SubquerySource  
- `cast_expression(expr, target_type)` – render dialect-specific CAST/CONVERT syntax  

Helper methods inside dialects (e.g. `quote_ident`, `_render_literal`, `_render_function_call`) are built on top of these primitives.

---

## 🔧 3. Dialect Registry & Factory

Dialects are registered via a central registry defined in `dialect_factory.py`.  

Each dialect module defines a class with a unique `DIALECT_NAME`, for example:

```python
class PostgresDialect(SqlDialect):
    DIALECT_NAME = "postgres"
```

When the package is imported, dialect classes are discovered and registered into an internal dictionary:

```python
REGISTRY = {
    "duckdb": DuckDbDialect,
    "postgres": PostgresDialect,
    "mssql": MssqlDialect,
}
```

Public factory functions:  

- `get_dialect(name: str) -> type[SqlDialect]`  
- `get_active_dialect(optional_override: str | None = None) -> SqlDialect`  
- `available_dialects() -> dict[str, type[SqlDialect]]`  

This allows:  

- explicit dialect selection by name  
- defaulting from environment or profile  
- future dynamic extension without touching core code  

---

## 🔧 4. Runtime Dialect Selection

Dialect selection follows a layered strategy:  

1. **Explicit override** (e.g. from UI query param):  
   - `?dialect=postgres` → PostgresDialect  
2. **Profile / configuration** (if set)  
3. **Environment variable**:  
   - `ELEVATA_SQL_DIALECT=duckdb`  
4. Fallback: a default dialect (usually DuckDB)  

In the **SQL Preview UI**, the user selects a dialect from a dropdown. The selection is passed as `?dialect=...` to the preview endpoint, which then calls `get_active_dialect()` with that override.

---

## 🔧 5. Identifier Rendering

All dialects receive **unquoted** identifiers from the LogicalPlan / AST and apply quoting rules themselves.  


- DuckDB: `"identifier"`  
- Postgres: `"identifier"`  
- MSSQL: `"identifier"` (aligned with dbt behaviour)  

This avoids confusion around vendor-specific quoting (e.g. `[]` vs `""`) and supports consistent behaviour across dialects.  

Rules:  
- identifiers that contain uppercase letters, spaces, or reserved words are always quoted  
- schema and table are rendered as `"schema"."table" AS "alias"`  

---

## 🔧 6. Literal Rendering

Literals are rendered via dialect helpers, typically:

```python
def render_literal(self, value: Any) -> str:
    ...
```

General rules:  

- strings → `'value'` with proper escaping  
- integers / decimals → as-is  
- booleans → `TRUE` / `FALSE` (or `1`/`0` for engines that require it)  
- `None` → `NULL`  

There is **no dialect-specific SQL** stored in metadata; only `SqlDialect` decides how a literal appears in the final SQL.

---

## 🔧 7. Expression Rendering (DSL AST)

All expressions in the LogicalPlan are built from the **Expression DSL & AST**.  

Key node types include:  

- `Literal`  
- `ColumnRef`  
- `ExprRef`  
- `ConcatExpr`  
- `ConcatWsExpr`  
- `CoalesceExpr`  
- `Hash256Expr`  
- `WindowFunctionExpr`  

The dialect implements `render_expr(expr)` by pattern-matching on node types.  

### 🧩 Example: CONCAT and CONCAT_WS

DSL:
```text
CONCAT_WS('|', CONCAT('bk1', '~', COALESCE({expr:bk1}, 'null_replaced')), 'pepper')
```

Rendered:  

- DuckDB: `CONCAT_WS('|', CONCAT('bk1', '~', COALESCE(r."bk1", 'null_replaced')), 'pepper')`  
- Postgres: `CONCAT_WS('|', CONCAT('bk1', '~', COALESCE(r."bk1", 'null_replaced')), 'pepper')`  
- MSSQL: `CONCAT_WS('|', CONCAT('bk1', '~', COALESCE(r."bk1", 'null_replaced')), 'pepper')`  

All dialects share the same function names here; differences emerge mainly around hashing, casting, and sometimes booleans.

---

## 🔧 8. Hashing (HASH256)

`Hash256Expr(inner)` is the dialect-neutral representation of SHA-256 hashing.  

Each dialect chooses how to implement it:  

### 🧩 DuckDB
```sql
SHA256(<expr>)
```

### 🧩 Postgres
```sql
ENCODE(DIGEST(<expr>, 'sha256'), 'hex')
```

### 🧩 MSSQL
```sql
CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', <expr>), 2)
```

All dialects yield **hex-encoded 64-character hashes**, so SK/FK values are comparable across engines.  

The inner `<expr>` itself is typically a `CONCAT_WS('|', ...)` expression built from BK components and a pepper.

---

## 🔧 9. Window Functions

Window functions such as `ROW_NUMBER()` are represented at the AST level by `WindowFunctionExpr`.  

Example:

```text
ROW_NUMBER() OVER (
  PARTITION BY bk
  ORDER BY updated_at DESC
)
```

Rendered in all dialects using ANSI SQL syntax. Dialects only differ in quoting of identifiers.  

These are heavily used in **multi-source Stage non-identity mode** to compute `__src_rank_ord`.  

---

## 🔧 10. Subqueries & UNION Rendering

### 🧩 Subqueries

Represented by `SubquerySource(select, alias)` in the LogicalPlan.  

Rendered as:  

```sql
(
  SELECT ...
) AS alias
```

Dialects can apply their own line-breaking / formatting rules, but the structure is identical.

### 🧩 UNION / UNION ALL

Represented by `LogicalUnion(selects=[...], union_type='ALL'|'DISTINCT')`.  

Rendered as:

```sql
SELECT ...
UNION ALL
SELECT ...
UNION ALL
SELECT ...
```

Again, only identifier/literal rendering and optional formatting differ by dialect.

---

## 🔧 11. Dialect Feature Summary

| Feature              | DuckDB   | Postgres | MSSQL   |  
|----------------------|----------|----------|---------|  
| Identifier quoting   | `"..."` | `"..."` | `"..."` |  
| HASH256              | `SHA256` | `DIGEST`+`ENCODE` | `HASHBYTES`+`CONVERT` |  
| CONCAT / CONCAT_WS   | ✓        | ✓        | ✓       |  
| COALESCE             | ✓        | ✓        | ✓       |  
| Window functions     | ✓        | ✓        | ✓       |  
| Subqueries in FROM   | ✓        | ✓        | ✓       |  
| UNION / UNION ALL    | ✓        | ✓        | ✓       |  


---

## 🔧 12. Adding a New Dialect

To add a new dialect:  

1. Create a module under `metadata/rendering/dialects/`, e.g. `snowflake.py`.  
2. Implement a class:  

```python
class SnowflakeDialect(SqlDialect):
    DIALECT_NAME = "snowflake"

    def render_expr(self, expr: Expr) -> str:
        ...

    def render_select(self, select: LogicalSelect) -> str:
        ...

    def cast_expression(self, expr: Expr, target_type: str) -> str:
        ...
```

3. Ensure the module is imported (so the class registers itself).  
4. Add tests in `tests/test_dialect_snowflake.py`.  
5. Optionally expose it in the SQL preview dropdown.  

No changes to metadata are required — all dialect logic is encapsulated.  

---

## 🔧 13. Identifier vs. Table Identifier Rendering

Dialects implement two distinct methods for rendering identifiers:

| Method | Responsibility | Example Output |
|--------|----------------|----------------|
| `render_identifier(name: str) -> str` | Renders a single identifier only (e.g., column name) | `"customer_id"` |
| `render_table_identifier(schema: str \| None, name: str) -> str` | Renders a schema-qualified table reference | `"rawcore"."rc_customer"` |

### Usage in SQL Generation

```python
col_sql = dialect.render_identifier(col_name)
tbl_sql = dialect.render_table_identifier(schema_name, table_name)
```

This separation ensures that:
- table-level quoting rules do not affect column expressions
- engines without schemas can pass `None` for `schema`
- identifiers remain valid for cross-schema and cross-dialect SQL

### Dialect Support Summary

| Feature | DuckDB | Postgres | MSSQL |
|--------|--------|----------|------|
| Identifier quoting | `"..."` | `"..."` | `"..."` |
| Schema-qualified tables via `render_table_identifier` | ✓ | ✓ | ✓ |

---

## 🔧 14. Testing Strategy

The Dialect System is validated via:  

- unit tests for each dialect  
- cross-dialect hashing tests (SK/FK equality)  
- SQL preview tests  
- LogicalPlan → SQL snapshot tests  

Focus areas:  

- identifier quoting correctness  
- literal escaping  
- HASH256 implementation parity  
- CONCAT / CONCAT_WS behavior  
- window function correctness  
- subquery and UNION formatting  

If all dialect tests pass, the multi-dialect engine behaves consistently.  

---

## 🔧 15. Summary

The Dialect System is the backbone of elevata’s multi-backend strategy:  

- SQL is generated from a vendor-neutral LogicalPlan + AST  
- dialects implement only the final rendering  
- SK/FK hashing is cross-dialect identical  
- adding new engines is straightforward  

This architecture ensures that elevata can support more backends (Snowflake, BigQuery, Databricks, …) without changing core metadata or generation logic.

---

## 🔧 16. Dialect diagnostics & health check

The Dialect System exposes a lightweight diagnostics layer to verify that all
registered SQL dialects behave as expected.

Diagnostics can be accessed in two ways:

1. **Programmatic API**  
2. **CLI command** via `manage.py elevata_dialect_check`

### 🧩 16.1 Programmatic diagnostics

The module `metadata.rendering.dialects.diagnostics` provides convenience
functions to inspect all dialects at once:

- `collect_dialect_diagnostics(dialect)`  
- `snapshot_all_dialects()`

Each snapshot contains:

- `name` / `class_name`
- `supports_merge`
- `supports_delete_detection`
- `supports_hash_expression`
- example literal renderings (TRUE/FALSE/NULL/date)
- example expressions for CONCAT and HASH256

This is useful for debugging and for asserting capabilities in tests, without
having to introspect each dialect manually.:contentReference[oaicite:0]{index=0}

### 🧩 16.2 CLI: `elevata_dialect_check`

For a quick end-to-end smoke test of all registered SQL dialects, use:

```bash
python manage.py elevata_dialect_check
```

This command:  
- discovers all registered dialects (DuckDB, Postgres, MSSQL, …),  
- prints basic capabilities:  
  - `supports_merge`  
  - `supports_delete_detection`  
- runs a set of small self-checks per dialect:  
  - identifier quoting (`quote_ident`)  
  - literal rendering for strings, numbers, booleans, dates, datetimes, decimals  
  - `concat_expression(...)`  
  - `hash_expression(...)`  
  - (optionally) `render_create_replace_table(...)`  
  - (optionally) `render_insert_into_table(...)`  
  - (optionally) `render_merge_statement(...)`  
  - (optionally) `render_delete_detection_statement(...)`  
- reports each check as:  
  - OK → check succeeded  
  - N/I → NotImplementedError (feature not implemented yet)  
  - FAIL → any other exception  

Example usage:

```python
# Run diagnostics for all dialects
python manage.py elevata_dialect_check

# Restrict diagnostics to a single dialect
python manage.py elevata_dialect_check --dialect duckdb
python manage.py elevata_dialect_check --dialect postgres
```

This is intentionally non-invasive: it only renders SQL; it does not execute it  
against a live database. The command is meant as a quick guardrail during  
development and CI to detect regressions in dialect implementations early.

---

## 🔧 17. Execution engines

Each SQL dialect can optionally provide an execution engine that knows how to  
run SQL statements against a concrete target system.  

The base interface lives in `rendering/dialects/base.py`:  

- `BaseExecutionEngine.execute(sql: str) -> int | None`  
- `SqlDialect.get_execution_engine(system) -> BaseExecutionEngine`  

Concrete dialects (e.g. `DuckDBDialect`) implement their own execution engine
in the same module:  

- `DuckDBDialect.get_execution_engine(system)` returns a `DuckDbExecutionEngine`  
- `DuckDbExecutionEngine` implements `execute(sql: str)`  

---

## 🔧 18. Dialect Parity Checklist

> **Purpose**  
> This checklist defines the *mandatory contract* every officially supported dialect must fulfill in order to support `--execute`, auto‑provisioning, and observability consistently across DuckDB, PostgreSQL, and MSSQL.  
>
> The goal is **behavioral parity**, not identical SQL syntax.

### 🧩 18.1 Core Rendering Contract (SQL Preview & Generation)

Every dialect **must implement** the core SQL rendering primitives so that SQL preview and generated SQL behave consistently.  

**Required methods:**  
- `render_identifier(name: str) -> str`  
- `render_table_identifier(schema: str | None, name: str) -> str`  
- `render_literal(value: Any) -> str`  
- `render_expr(expr: Expr) -> str`  
- `render_select(select: LogicalSelect | LogicalUnion | ...) -> str`  
- `cast_expression(expr: Expr, target_type: str) -> str`  

**Required parity guarantees (v0.6.x):**  
- Deterministic `HASH256` rendering (hex‑encoded, 64 characters)  
- Consistent `CONCAT` / `CONCAT_WS` semantics  
- Window functions parity (`ROW_NUMBER`, partitioning, ordering)  
- Nested subqueries and `UNION` rendering parity  

### 🧩 18.2 Execution Contract (`--execute`)

Every dialect that is considered *supported* **must provide a working execution engine**.  

**Required methods:**  
- `get_execution_engine(system) -> BaseExecutionEngine`  
- `BaseExecutionEngine.execute(sql: str) -> int | None`  

**Execution expectations:**   
- Executes multi‑statement SQL safely  
- Uses the resolved target connection (`system.security["connection_string"]`)  
- Raises clear, actionable exceptions on connection or SQL errors  
- Returns affected row counts where the backend supports it (optional)  

> Dialects without a working execution engine are considered *render‑only* and must not be advertised as fully supported targets.  

### 🧩 18.3 Auto‑Provisioning Contract (Warehouse‑Side DDL)

All supported dialects must support *idempotent* warehouse provisioning.  

**Required methods:**  
- `render_create_schema_if_not_exists(schema: str) -> str`  
- `render_create_table_if_not_exists(td: TargetDataset) -> str`  
- `render_create_load_run_log_if_not_exists(meta_schema: str) -> str`  

**Rules:**  
- DDL must be safe to execute multiple times  
- No destructive operations (no DROP)  
- Target table DDL must be derived from `TargetColumn` metadata  

**Important clarification (RAW ingestion semantics):**  
The "no destructive operations" rule applies **only** to *auto-provisioning*  
of warehouse-managed objects (schemas, tables, columns).

RAW datasets follow **ingestion semantics** and may legitimately use  
`DROP TABLE` + `CREATE TABLE` during full refreshes to realign the physical  
landing table with the current source schema.

This behavior is intentional and handled outside of auto-provisioning.  

**Meta logging table provisioning (meta.load_run_log):**  
Warehouse-level logging table provisioning is **centralized** and **registry-driven**.  
Dialects must not hardcode the physical schema of `meta.load_run_log` via a dedicated  
`render_create_load_run_log_if_not_exists(...)` helper anymore.

Instead, `ensure_load_run_log_table(...)` provisions the table and missing columns using:  
- `LOAD_RUN_LOG_REGISTRY` as the canonical schema definition  
- `Dialect.LOAD_RUN_LOG_TYPE_MAP` (+ optional `Dialect.map_load_run_log_type(...)`) for type mapping  
- `Dialect.render_create_table_if_not_exists_from_columns(...)`  
- `Dialect.render_add_column(...)`

**Dialect contract for registry-driven meta logging:**  
- Dialects must provide `LOAD_RUN_LOG_TYPE_MAP` (canonical → physical type mapping).  
- Dialects may optionally provide `map_load_run_log_type(col_name, canonical_type)` for special cases.  
- Dialects must support `render_create_table_if_not_exists_from_columns(...)` (override if needed).  
- Dialects must support `render_add_column(...)` (override if needed; e.g. MSSQL syntax).

**Dialect notes:**  
- DuckDB: avoid unsupported constraints (PRIMARY KEY / IDENTITY may fail)  
- MSSQL: use `IF OBJECT_ID(...) IS NULL` patterns  
- PostgreSQL: `CREATE TABLE IF NOT EXISTS` is supported  

### 🧩 18.4 Observability & Run Logging Contract

Every supported dialect must support warehouse‑level execution logging.  

**Required method:**  
- `render_insert_load_run_log(*, meta_schema: str, values: dict[str, object]) -> str | None`  

**Contract:**  
- `values` is a fully normalized canonical row for `meta.load_run_log`  
- Keys and order must match the canonical schema defined in `LOAD_RUN_LOG_REGISTRY`  
- Dialects must only render identifiers and literals safely; no hardcoded column lists   

**Canonical log fields:**  
- `batch_run_id`, `load_run_id`  
- `target_schema`, `target_dataset`, `target_dataset_id`  
- `target_system`, `target_system_type`, `profile`, `dialect`  
- `load_mode`, `handle_deletes`, `historize`  
- `started_at`, `finished_at`  
- `render_ms`, `execution_ms`  
- `sql_length`, `rows_affected`  
- `load_status`, `error_message`  

`rows_affected` is best-effort and may be `-1` if not reported by the execution engine.

**Canonical schema source of truth:**  
The physical schema of `meta.load_run_log` must match `LOAD_RUN_LOG_REGISTRY` exactly.

**Dialect type mapping requirements for logging:**  
- `LOAD_RUN_LOG_TYPE_MAP: dict[str, str]` (canonical → physical type)  
- Optional: `map_load_run_log_type(col_name: str, canonical_type: str) -> str | None`  
  for per-column overrides (e.g. MSSQL `error_message` length).

### 🧩 18.5 Incremental & Historization Contract

Dialects that support incremental pipelines (Rawcore / History) must be able to execute:  

- New‑row inserts  
- Changed‑row handling (close previous version + insert new version)  
- Delete detection SQL  
- Delete marking in history (`version_state = 'deleted'`)  

Dialect implementations must render the required SQL primitives consistently; architectural details are specified in:  
- [Load SQL Architecture](load_sql_architecture.md)  
- [Historization Architecture](historization_architecture.md) 

### 🧩 18.6 Diagnostics & Parity Validation

Every supported dialect must pass a minimal diagnostic suite:  

- Render‑only smoke tests  
- Identifier quoting tests  
- Literal escaping tests  
- HASH256 output consistency tests  
- Minimal merge / historization render tests (where applicable)  

Execution parity is validated by:  
- Running `--execute` on all supported targets  
- Verifying schema provisioning, table provisioning, and run logging  

---

> **Summary**  
> A dialect is only considered *fully supported* in elevata when it satisfies **all sections 18.1 – 18.6**.  
> Partial implementations must be clearly marked as *render‑only* or *experimental*.

---

## 🔧 Related Documents

- [Logical Plan & Lineage](logical_plan.md)  
- [SQL Rendering & Conventions](sql_rendering_conventions.md)  
- [Load SQL Architecture](load_sql_architecture.md)  
- [Historization Architecture](historization_architecture.md)  
- [SQL Preview Pipeline](sql_preview_pipeline.md)  
- [Source Backends](source_backends.md)  
- [Target Backends](target_backends.md)  

---

© 2025 elevata Labs — Internal Technical Documentation