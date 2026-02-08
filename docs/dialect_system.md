# ⚙️ SQL Dialect System

The elevata dialect system is the **execution abstraction layer** that makes the platform  
vendor-neutral, deterministic, and production-ready across modern data platforms.

A dialect is responsible for translating a **logical, metadata-driven query plan** into  
**concrete, executable SQL** for a specific target engine — while preserving:

- semantic correctness  
- deterministic behavior  
- schema and contract guarantees

This means:

> **The same logical dataset definition produces the same semantic result —  
> regardless of whether it runs on BigQuery, Databricks, DuckDB, Fabric Warehouse, MSSQL, Postgres   
> or Snowflake.**

---

## 🔧 1. Role of the Dialect System

The Dialect System abstracts all SQL-vendor differences.

High-level pipeline:

```text
LogicalPlan  →  Expression AST  →  SqlDialect renderer  →  final SQL string
```

Each dialect is responsible for:

- identifier quoting  
- literal rendering  
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
    "bigquery": BigQueryDialect,
    "databricks": DatabricksDialect,
    "duckdb": DuckDbDialect,
    "fabric_warehouse": FabricWarehouseDialect,
    "mssql": MssqlDialect,
    "postgres": PostgresDialect,
    "snowflake": SnowflakeDialect,
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

In the **SQL Preview UI**, the user selects a dialect from a dropdown.  
The selection is passed as `?dialect=...` to the preview endpoint, which then calls  
`get_active_dialect()` with that override.

---

## 🔧 5. Identifier Rendering

All dialects receive **unquoted** identifiers from the LogicalPlan / AST and apply quoting rules themselves.

- DuckDB: `"identifier"`  
- Postgres: `"identifier"`  
- MSSQL: `"identifier"`  
- ...

This avoids confusion around vendor-specific quoting and supports consistent behaviour across dialects.

Rules:  
- identifiers that contain uppercase letters, spaces, or reserved words are always quoted  
- schema and table are rendered as `"schema"."table" AS "alias"`

---

## 🔧 6. Literal Rendering

Literals are rendered via dialect helpers.

General rules:

- strings → `'value'` with proper escaping  
- integers / decimals → as-is  
- booleans → `TRUE` / `FALSE` (or `1`/`0`)  
- `None` → `NULL`

There is **no dialect-specific SQL** stored in metadata.

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

---

## 🔧 8. Hashing (HASH256)

```text
HASH256(<expr>)
```

All dialects must return a **hex-encoded, 64-character, lowercase** string.

### 🧩 BigQuery
```sql
TO_HEX(SHA256(<expr>))
```

### 🧩 Databricks
```sql
sha2(<expr>, 256)
```

### 🧩 DuckDB
```sql
sha256(<expr>)
```

### 🧩 Fabric Warehouse
```sql
CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CAST(<expr> AS VARCHAR(4000))), 2)
```

### 🧩 MSSQL
```sql
CONVERT(VARCHAR(64), HASHBYTES('sha2_256', <expr>), 2)
```

### 🧩 Postgres
```sql
encode(digest(<expr>, 'sha256'), 'hex')
```

### 🧩 Snowflake
```sql
LOWER(TO_HEX(SHA2(<expr>, 256)))
```

---

| Feature | BigQuery | Databricks | DuckDB | Fabric Warehouse | MSSQL | Postgres | Snowflake |
|--------|----------|------------|--------|------------------|-------|----------|-----------|
| Identifier quoting | `...` | `...` | "..." | "..." | "..." | "..." | "..." |
| HASH256 implementation | SHA256+TO_HEX | SHA2 | SHA256 | HASHBYTES+CONVERT | HASHBYTES+CONVERT | DIGEST+ENCODE | SHA2+TO_HEX |
| CONCAT / CONCAT_WS | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| COALESCE | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Window functions | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Subqueries in FROM | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| UNION / UNION ALL | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

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
| `render_table_identifier(schema: str | None, name: str) -> str` | Renders a schema-qualified table reference | `"rawcore"."rc_customer"` |

### 🧩 Usage in SQL Generation

```python
col_sql = dialect.render_identifier(col_name)
tbl_sql = dialect.render_table_identifier(schema_name, table_name)
```

This separation ensures that:
- table-level quoting rules do not affect column expressions
- engines without schemas can pass `None` for `schema`
- identifiers remain valid for cross-schema and cross-dialect SQL

### 🧩 Dialect Support Summary

| Feature | BigQuery | Databricks | DuckDB | Fabric Warehouse | MSSQL | Postgres | Snowflake |
|--------|----------|------------|--------|------------------|-------|----------|-----------|
| Identifier quoting | `...` | `...` | "..." | "..." | "..." | "..." | "..." |
| Schema-qualified tables via `render_table_identifier` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

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
having to introspect each dialect manually.

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
    - (optionally) `render_insert_into_table(...)`  
    - (optionally) `render_merge_statement(...)`  
    - (optionally) `render_delete_detection_statement(...)`  
- reports each check as:  
    - OK → check succeeded  
    - N/I → NotImplementedError (feature not implemented yet)  
    - FAIL → any other exception

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

### 🧩 Execution Engine vs SQLAlchemy Engine

In elevata, SQL execution and metadata introspection are intentionally separated concerns.

Two different connection mechanisms may exist for the same target system:

| Component | Purpose |
|-----------|---------|
| Execution Engine | Executes rendered SQL statements (`--execute`) |
| SQLAlchemy Engine | Used for introspection, materialization planning, and schema inspection |

In many databases (e.g. Postgres, DuckDB, Snowflake) both roles may use the same
underlying connection mechanism.

However, analytical platforms such as BigQuery or Databricks may require different
technical connectors:

- Execution via native API or warehouse connector  
- Introspection via SQLAlchemy-compatible drivers

This separation is intentional and allows elevata to support execution environments  
that do not expose a full SQLAlchemy-compatible runtime.

---

## 🔧 18. Dialect Parity Checklist

> **Purpose**
> This checklist defines the *mandatory contract* every officially supported dialect must fulfill  
in order to support `--execute`, auto‑provisioning, and observability consistently across BigQuery, Databricks, DuckDB, Fabric Warehouse, MSSQL, PostgreSQL and Snowflake.
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
- `render_create_table_if_not_exists_from_columns(schema: str, table: str, columns: list[dict[str, object]]) -> str`  
- `render_add_column(schema: str, table: str, column: str, column_type: str | None) -> str`

**Rules:**  
- DDL must be safe to execute multiple times  
- No destructive operations (no DROP)  
- Target table DDL must be derived from `TargetColumn` metadata

### 🧩 18.4 Observability & Run Logging Contract

Every supported dialect must support warehouse‑level execution logging.

**Required behavior:**  
- Logging is emitted into `meta.load_run_log` with a canonical schema (registry-driven).  
- The INSERT logic is centralized (dialects must not each hardcode their own log INSERT).

---

### 🧩 18.5 Incremental & Historization Contract

Dialects that support incremental pipelines (Rawcore / History) must be able to execute:

- New-row inserts  
- Changed-row handling (close previous version + insert new version)  
- Delete detection SQL  
- Delete marking in history (`version_state = 'deleted'`)

Dialect implementations must render the required SQL primitives consistently; architectural details are specified in:  
- [Load SQL Architecture](load_sql_architecture.md)  
- [Historization Architecture](historization_architecture.md)

### 🧩 18.6 Diagnostics & Parity Validation

Every supported dialect must pass a minimal diagnostic suite:

- Render-only smoke tests  
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
> Partial implementations must be clearly marked as *render-only* or *experimental*.

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

© 2025-2026 elevata Labs — Internal Technical Documentation
