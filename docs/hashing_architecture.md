# Hashing Architecture

This document describes elevata’s fully redesigned **surrogate-key (SK)** and **foreign-key (FK)** hashing architecture introduced in **v0.5.0**.

The new hashing engine is:  
- **deterministic** (stable outputs for identical metadata)  
- **dialect-neutral** (AST → rendered per SQL dialect)  
- **metadata-safe** (no vendor SQL stored in the database)  
- **cross-dialect identical** (DuckDB, Postgres, MSSQL produce the same hash)  
- **testable** (AST inspections instead of string asserts)  

This replaces all legacy string-based hashing logic.  

---

# 1. Goals of the New Architecture

Before v0.5.x, hashing logic was embedded as strings inside:  
- builder code  
- mappers  
- templates  

This caused:  
- vendor-specific SQL in metadata  
- inconsistent NULL replacement logic  
- non-deterministic ordering  
- difficult testability  
- different hash outputs across engines  

**v0.5.x introduces:**

1. a vendor-neutral **Hashing DSL**  
2. a **DSL Parser** → AST  
3. a structured **Expression AST**  
4. dialect-specific SQL renderers  
5. deterministic ordering for all BK components  

---

# 2. SK/FK Hashing Requirements

## 2.1 Surrogate Key (SK) must be:  
- deterministic  
- reproducible across dialects  
- independent of physical column order  
- sensitive only to BK semantics  

### SK derivation inputs:
- ordered list of business key columns  
- literal separators  
  - `~` between field name & value  
  - `|` between BK pairs  
- null replacement literal: `'null_replaced'`  
- system-wide pepper: e.g. `'pepper'`  

---

## 2.2 Foreign Key (FK) must:
- follow *parent’s* SK structure **exactly**  
- replace parent column references with child references  
- maintain ordering  
- use the same pepper  
- remain cross-dialect consistent  

FK structure for two BK columns looks like:
```
CONCAT_WS('|',
  CONCAT('bk1', '~', COALESCE(parent_child_bk1, 'null_replaced')),
  CONCAT('bk2', '~', COALESCE(parent_child_bk2, 'null_replaced')),
  'pepper'
)
```

---

# 3. Hashing DSL (Human-Readable)

Surrogate keys and foreign keys are generated via a safe declarative DSL:

```
HASH256(
  CONCAT_WS('|',
    CONCAT('productid', '~', COALESCE({expr:productid}, 'null_replaced')),
    'pepper'
  )
)
```

Characteristics:  
- **No vendor SQL** inside the DSL  
- All logic expressible via `COL()`, `CONCAT()`, `CONCAT_WS()`, `COALESCE()`, `HASH256()`  
- Designed to be a *serialization format* for metadata  

---

# 4. DSL Parser → AST

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
    Literal('pepper')
  ])
)
```

---

# 5. Deterministic Ordering Rules (Critical)

BK components are sorted **lexicographically by BK name**.  

For BKs: `[bk2, bk1, bk10]` → sorted → `[bk1, bk10, bk2]`.  

This ensures:  
- identical hashes regardless of metadata ordering  
- stable lineage comparisons  
- deterministic FK reconstruction  

### Ordering within pair:
```
CONCAT("bk1", '~', COALESCE(expr, 'null_replaced'))
```

### Ordering of pairs:
Joined using `CONCAT_WS('|', ...)`.

---

# 6. Foreign Key Hashing

FK hashing mirrors SK hashing **exactly**, except column references point to *child* columns.

Example:
```
CONCAT_WS('|',
  CONCAT('bk1', '~', COALESCE(child.bk1, 'null_replaced')),
  CONCAT('bk2', '~', COALESCE(child.bk2, 'null_replaced')),
  'pepper'
)
```

This is guaranteed by using `ExprRef` and standard BK ordering.  

The dialect renderer never needs to know whether it’s SK or FK.

---

# 7. Dialect Rendering of HASH256

The AST-based hash expression is rendered differently per dialect.

## DuckDB
```
SHA256(CONCAT_WS('|', ...))
```

## Postgres
```
ENCODE(
  DIGEST(CONCAT_WS('|', ...), 'sha256'),
  'hex'
)
```

## MSSQL
```
CONVERT(VARCHAR(64),
  HASHBYTES('SHA2_256', CONCAT_WS('|', ...)),
  2)
```

All platforms produce **byte-identical SHA-256 outputs**.

---

# 8. Null Semantics

All BK values are wrapped in:
```
COALESCE(value, 'null_replaced')
```

This avoids platform-specific differences:  
- `NULL || 'x'` vs `CONCAT(NULL, 'x')`  
- Postgres treating empty strings differently  
- MSSQL `+` operator behavior  


---

# 9. Pepper Semantics

A global pepper (e.g. `'pepper'`) is appended as the last argument of the `CONCAT_WS` call.  

Purpose:  
- prevent predictable hashes  
- add stability across dialects  
- disable hash attacks on BKs  

Pepper is **constant** and not column-dependent.

---

# 10. How FK hashing mirrors parent SK logic

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

# 11. Advantages of the New Hashing Engine

- cross-dialect identical hashing  
- no SQL inside metadata  
- fully testable (AST-level tests)  
- deterministic BK ordering  
- safe DSL (no SQL injection)  
- shared logic for SKs and FKs  
- consistent subquery + expression behavior  

---

# 12. Testing Strategy

Tests cover:  
- DSL → AST correctness  
- dialect rendering for SK & FK  
- DuckDB/Postgres/MSSQL hash equivalence  
- BK ordering rules  
- null-coalescing behavior  
- pepper correctness  

All tests pass when AST rendering is correct.

---

# 13. Planned Extensions

Future improvements:  
- support for CASE expressions  
- configurable null replacement tokens  
- support for salted hashing per dataset  
- cryptographic hash plug-ins  
- optional binary hash storage format  

---

© 2025 elevata Labs — Internal Technical Documentation