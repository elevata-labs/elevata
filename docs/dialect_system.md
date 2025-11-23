# 🧠 SQL Dialect System

> This document describes how *elevata* abstracts SQL dialects across different database engines.  
> Goal: **vendor-neutral metadata**, **dialect-aware SQL generation**.

---

## 🎯 1. Motivation & Scope

elevata generates SQL from metadata models and must support multiple target platforms  
(e.g. DuckDB, MSSQL, Postgres, BigQuery, Snowflake).

Instead of hardcoding SQL syntax in many places, elevata uses a **dialect abstraction**:  

- Metadata & logical plans are **vendor-neutral**  
- Rendering is delegated to a **SqlDialect** implementation  
- Load SQL (full / merge / delete detection) is dialect-specific, but **behind a stable interface**  
- Expressions (concat, hash, casts, literals) are dialect-aware  

The dialect system makes elevata predictable, extendable, and portable across engines.

---

## 🧩 2. Architecture Overview

The dialect system consists of:

1. **Base interface**  
   `SqlDialect` in  
   `metadata.rendering.dialects.base`  
   Defines the contract for rendering identifiers, expressions, SELECTs and Load SQL.

2. **Concrete implementations**  
   Example: `DuckDBDialect` in  
   `metadata.rendering.dialects.duckdb`  
   Implements the interface for a specific engine.

3. **Dialect resolution**  
   `get_active_dialect()`  
   Chooses the dialect instance for the current context based on 
   environment and profile.

4. **Callers**  
   - SQL preview (`preview.py`)  
   - Load SQL (`load_sql.py`)  
   - Renderer (`renderer.py`)  
   - Future: CLI/runner, Data Quality, DB-specific utilities  

The architecture enforces a clean separation:  

- Logical plan & metadata → vendor-neutral  
- Rendering → dialect-specific

---

## 🎛️ 3. Dialect Selection (`get_active_dialect()`)

Centralised resolution:

```python
from metadata.rendering.dialects import get_active_dialect
dialect = get_active_dialect()
```

Resolution order:

### 1. Environment variable  
- `ELEVATA_SQL_DIALECT`  

### 2. Active profile  
`load_profile()` may define:

```yaml
default_dialect: duckdb
```

### 3. Hard fallback  
If nothing is configured → **DuckDBDialect**.

### Error Handling  
Unsupported dialect names raise:

```
ValueError: Unknown dialect: <name>
```

---

## 🧱 4. `SqlDialect` Core Interface

### 4.1 Identifier Rendering

- `quote_ident(name: str) -> str`  
- `quote_table(schema: str, table: str) -> str`  

Example (DuckDB):

```
"rawcore"."rc_customer"
```

---

### 4.2 Expression Rendering

- `render_expr(expr: Expr) -> str`  
- `render_select(select: LogicalSelect) -> str`  

Logical plans remain dialect-neutral.  
Only the dialect implementation knows about:  

- Boolean operators  
- Concat operator (`||` vs `CONCAT()`)  
- Hash functions  
- CAST syntax  
- NULL semantics  
- Timestamp literals  

Renderer uses:

```python
render_sql(plan, dialect)
```

The logical → physical translation is 100% dialect-driven.

---

## 🧪 5. Literals & Type Casting

To keep type handling consistent across engines, the dialect provides:

### `render_literal(value) -> str`

DuckDB examples:

| Python value | SQL literal |
|--------------|-------------|
| `None` | `NULL` |
| `True` | `TRUE` |
| `"abc"` | `'abc'` |
| `"O'Malley"` | `'O''Malley'` |
| `date(2024,5,17)` | `DATE '2024-05-17'` |
| `datetime` | `TIMESTAMP '...'` |

Other dialects may use different forms:  

- Postgres: `'2024-05-17'::date`  
- Snowflake: `TO_DATE('2024-05-17')`  
- BigQuery: `DATE '2024-05-17'`  

### `cast_expression(expr, target_type) -> str`

- DuckDB: `CAST(expr AS TYPE)`  
- BigQuery: same  
- Snowflake: `TO_TYPE(expr)`  

These hooks ensure type behavior is portable.

---

## 🧩 6. Expression Helpers (Concat, Hash, Coalesce)

These common expression patterns are dialect-specific:

### `concat_expression(parts: Sequence[str]) -> str`

- DuckDB: `('A' || 'B' || 'C')`  
- Snowflake: `CONCAT('A','B','C')`  
- MSSQL: `'A' + 'B' + 'C'`  

### `hash_expression(expr: str, algo='sha256') -> str`

- DuckDB: `SHA256(expr)`  
- MSSQL: `HASHBYTES('SHA2_256', expr)`  
- BigQuery: `TO_HEX(SHA256(expr))`  

These helpers are used inside `render_expr`, so all hashing & concatenation  
across the renderer becomes dialect-clean.

---

## 🚚 7. Load SQL in the Dialect

Full-load, merge and delete-detection SQL use dialect hooks:

### `render_create_replace_table(schema, table, select_sql)`  
### `render_insert_into_table(schema, table, select_sql)`  
### `render_merge_statement(...)`  
### `render_delete_detection_statement(...)`

The load planner computes strategy (full vs merge, delete detection),  
and the dialect renders the exact SQL syntax.

Examples:

- DuckDB `MERGE INTO` for merge loads  
- DuckDB `DELETE … WHERE NOT EXISTS` for delete detection  
- DuckDB `CREATE OR REPLACE TABLE … AS SELECT …` for full loads  

Other dialects can:

- emulate merge via `UPDATE + INSERT`  
- use `DELETE USING` (Postgres, MSSQL)  
- require aliasing rules (Snowflake)

---

## ⚙️ 8. Dialect Capabilities

Dialect methods:

```python
@property
def supports_merge(self) -> bool:
    ...

@property
def supports_delete_detection(self) -> bool:
    ...
```

Load SQL uses these flags:

- If `incremental_strategy=merge` but `supports_merge=False`  
  → clear `NotImplementedError`.  

- If `handle_deletes=True` but `supports_delete_detection=False`  
  → clear error.  

Capabilities reflect **implemented semantics**, not theoretical ability.

---

## 🦆 9. DuckDBDialect (reference implementation)

DuckDBDialect implements:

### ✔ Identifier quoting  
`"schema"."table"`

### ✔ Expression rendering  
Columns, functions, concat, hash, casts, literals

### ✔ Load SQL  
- Full load  
- Merge  
- Delete detection  

### ✔ Capabilities  
- `supports_merge = True`  
- `supports_delete_detection = True`  

DuckDB serves as blueprint for all future dialects.

---

## 🧪 10. Testing

Dialects are covered by:  

- Expression tests (`concat`, `hash`, literal rendering, casts)  
- Load SQL tests (`merge`, `delete detection`)  
- Preview SQL tests  
- Dialect resolution tests  

When adding a new dialect:  

- Copy existing DuckDB tests  
- Adjust expected SQL syntax  
- Assert capability flags are correct  
- Run full test suite  

---

## 🧷 11. Adding a New Dialect (Checklist)

1. Create file:  
   `metadata/rendering/dialects/<name>.py`

2. Implement:  
   - `render_literal`  
   - `cast_expression`  
   - `concat_expression`  
   - `hash_expression`  
   - `render_expr`  
   - `render_select`  
   - Load hooks (`render_merge_statement`, `render_delete_detection_statement`, …)

3. Add to dialect registry  
   in `get_active_dialect()`

4. Update profile schema (optional):

```yaml
default_dialect: <name>
```

5. Add tests mirroring DuckDBDialect.

---

## 📚 Related Documents

- [Logical Plan & Lineage](lineage_and_logical_plan.md)  
- [SQL Rendering & Conventions](sql_rendering_conventions.md)  
- [Load SQL Architecture](load_sql_architecture.md)  
- [SQL Preview Pipeline](sql_preview_pipeline.md)  
- [Source Backends](source_backends.md)  
- [Target Backends](target_backends.md)  

---

© 2025 elevata Labs — Internal Technical Documentation