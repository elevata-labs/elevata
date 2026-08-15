# ⚙️ Hashing Architecture

This document describes elevata’s **surrogate-key (SK)** and **foreign-key (FK)** hashing architecture.

The hashing engine is:

- **deterministic** (stable outputs for identical metadata)  
- **dialect-neutral** (AST → rendered per SQL dialect)  
- **metadata-safe** (no vendor SQL stored in the database)  
- **cross-dialect identical** (BigQuery, Databricks, DuckDB, Fabric Warehouse, MSSQL, Postgres, Snowflake produce the same hash)  
- **testable** (AST inspections instead of string asserts)  

Hashing is standardized as **hex-encoded, 64-character, lowercase SHA-256** across all dialects.

---

## 🔧 1. Goals of the Architecture

1. a vendor-neutral **Hashing DSL**  
2. a **DSL Parser** → AST  
3. a structured **Expression AST**  
4. dialect-specific SQL renderers  
5. deterministic ordering for all BK components  

---

## 🔧 2. SK/FK Hashing Requirements

### 🧩 2.1 Surrogate Key (SK) must be:
- deterministic  
- reproducible across dialects  
- independent of physical column order  
- sensitive only to BK semantics  

#### 🔎 SK derivation inputs:
- ordered list of business key columns  
- literal separators  
    - `~` between field name & value  
    - `|` between BK pairs  
- null replacement literal: `'null_replaced'`  
- symbolic runtime pepper binding: `{runtime:pepper}`

### 🧩 2.3 History Surrogate Keys
- History SK reuses the parent rawcore SK structure:  
    - natural key columns: [rawcore_sk_column, "version_started_at"].  
    - same hashing DSL & AST (CONCAT_WS, COALESCE, HASH256, `{runtime:pepper}`).  
- This ensures:  
    - BK for history = rawcore SK + version_started_at.  
    - joinability between rawcore and history later (if needed).  
    - _hist never defines its own FK hashes; all FK semantics stay in the rawcore layer.  

---

### 🧩 2.2 Foreign Key (FK) must:
- follow *parent’s* SK structure **exactly**  
- replace parent column references with child references  
- maintain ordering  
- use the same runtime-resolved pepper within one environment  
- remain cross-dialect consistent  

FK structure for two BK columns looks like:
```
CONCAT_WS('|',
  CONCAT('bk1', '~', COALESCE(parent_child_bk1, 'null_replaced')),
  CONCAT('bk2', '~', COALESCE(parent_child_bk2, 'null_replaced')),
  {runtime:pepper}
)
```

---

## 🔧 3. Hashing DSL (Human-Readable)

Surrogate keys and foreign keys are generated via a safe declarative DSL:

```
HASH256(
  CONCAT_WS('|',
    CONCAT('productid', '~', COALESCE({expr:productid}, 'null_replaced')),
    {runtime:pepper}
  )
)
```

Characteristics:

- **No vendor SQL** inside the DSL  
- All logic expressible via `COL()`, `CONCAT()`, `CONCAT_WS()`, `COALESCE()`, `HASH256()`  
- Designed to be a *serialization format* for metadata  

---

## 🔧 4. DSL Parser → AST

The DSL parser (in `dsl.py`) converts the DSL into a structured AST composed of:

- `Literal`  
- `ColumnRef`  
- `ExprRef`  
- `ConcatExpr`  
- `ConcatWsExpr`  
- `CoalesceExpr`  
- `Hash256Expr`  

Example AST snippet:
```
Hash256Expr(
  ConcatWsExpr('|', [
    ConcatExpr([
      Literal('productid'),
      Literal('~'),
      CoalesceExpr(ColumnRef('productid'), Literal('null_replaced'))
    ]),
    Literal(<runtime pepper>)
  ])
)
```

`{runtime:pepper}` is persisted in the DSL. During DSL parsing, elevata resolves that token through `get_runtime_pepper()` and represents the resolved value as a normal `Literal` in the runtime AST. The concrete secret is therefore never persisted in portable metadata.

---

## 🔧 5. Deterministic Ordering Rules (Critical)

BK components are sorted **lexicographically by BK name**.

For BKs: `[bk2, bk1, bk10]` → sorted → `[bk1, bk10, bk2]`.

This ensures:

- identical hashes regardless of metadata ordering  
- stable lineage comparisons  
- deterministic FK reconstruction  

### 🧩 Ordering within pair:
```
CONCAT("bk1", '~', COALESCE(expr, 'null_replaced'))
```

### 🧩 Ordering of pairs:
Joined using `CONCAT_WS('|', ...)`.

---

## 🔧 6. Foreign Key Hashing

FK hashing mirrors SK hashing **exactly**, except column references point to *child* columns.

Example:
```
CONCAT_WS('|',
  CONCAT('bk1', '~', COALESCE(child.bk1, 'null_replaced')),
  CONCAT('bk2', '~', COALESCE(child.bk2, 'null_replaced')),
  {runtime:pepper}
)
```

This is guaranteed by using `ExprRef` and standard BK ordering.

The dialect renderer never needs to know whether it’s SK or FK.

---

## 🔧 7. Dialect Rendering of HASH256

The AST-based hash expression is rendered differently per dialect.

### 🧩 BigQuery
```
TO_HEX(SHA256(CONCAT_WS('|', ...)))
```

### 🧩 Databricks
```
SHA2(CONCAT_WS('|', ...), 256)
```

### 🧩 DuckDB
```
SHA256(CONCAT_WS('|', ...))
```

### 🧩 Fabric Warehouse
```
CONVERT(VARCHAR(64),
HASHBYTES('SHA2_256', CAST(CONCAT_WS('|', ...) AS VARCHAR(4000))),
2)
```

### 🧩 MSSQL
```
CONVERT(VARCHAR(64),
  HASHBYTES('SHA2_256', CONCAT_WS('|', ...)),
  2)
```

### 🧩 Postgres

> On PostgreSQL, SHA256 hashing relies on the pgcrypto extension.

```
ENCODE(
  DIGEST(CONCAT_WS('|', ...), 'sha256'),
  'hex'
)
```

### 🧩 Snowflake
```
TO_HEX(SHA2(TO_VARCHAR(CONCAT_WS('|', ...)), 256))
```

All platforms produce **byte-identical SHA-256 outputs**.

---

## 🔧 8. Null Semantics

All BK values are wrapped in:
```
COALESCE(value, 'null_replaced')
```

This avoids platform-specific differences:

- `NULL || 'x'` vs `CONCAT(NULL, 'x')`  
- Postgres treating empty strings differently  
- MSSQL `+` operator behavior  


---

## 🔧 9. Pepper Semantics

The symbolic token `{runtime:pepper}` is appended as the last argument of the portable `CONCAT_WS` expression. The concrete secret is resolved from the current runtime when the DSL is parsed for SQL rendering.

Purpose:

- prevent predictable hashes  
- add stability across dialects  
- disable hash attacks on BKs  

Pepper is **constant within one runtime context** and not column-dependent. Different environments may use different concrete pepper values; the persisted portable DSL remains identical because it stores `{runtime:pepper}` rather than the secret.

---

## 🔧 10. How FK hashing mirrors parent SK logic

FK hashing logic:

1. Retrieve parent SK structure (BK names, ordering)  
2. Inject child columns into the same structure  
3. Reconstruct the same AST pattern  
4. Apply `Hash256Expr`  

This guarantees:

- referential equality  
- stable lineage  
- consistent join keys  

---

## 🔧 11. Advantages of the New Hashing Engine

- cross-dialect identical hashing  
- no SQL inside metadata  
- fully testable (AST-level tests)  
- deterministic BK ordering  
- safe DSL (no SQL injection)  
- shared logic for SKs and FKs  
- consistent subquery + expression behavior  

---

## 🔧 12. Testing Strategy

Tests cover:

- DSL → AST correctness  
- dialect rendering for SK & FK  
- BigQuery/Databricks/DuckDB/Fabric Warehouse/MSSQL/Postgres/Snowflake hash equivalence  
- BK ordering rules  
- null-coalescing behavior  
- pepper correctness  

All tests pass when AST rendering is correct.

---

© 2025-2026 elevata - Technical Documentation