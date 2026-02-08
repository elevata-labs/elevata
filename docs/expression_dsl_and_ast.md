# ⚙️ Expression DSL & AST

This document describes elevata’s vendor-neutral **Expression DSL** (Domain Specific Language) and its corresponding **AST** (Expression Abstract Syntax Tree).  

It forms the foundation of the multi-dialect SQL engine and powers:  
- surrogate-key hashing  
- foreign-key lineage hashing  
- CONCAT/COALESCE operations  
- window functions (ROW_NUMBER, etc.)  
- subqueries in the LogicalPlan  
- deterministic SQL generation across dialects  

---

## 🔧 1. Purpose of the DSL & AST

**The architecture introduces:**  
- a safe, declarative **Expression DSL** stored in metadata  
- a **parser** converting DSL → AST  
- a **vendor-neutral AST** describing expressions  
- **dialect renderers** (BigQuery, Databricks, DuckDB, Fabric Warehouse, MSSQL, Postgres, Snowflake) that emit actual SQL  

This ensures:  
- deterministic SQL generation  
- cross-dialect reproducibility  
- fully testable and composable expression logic  
- consistent hashing on all platforms  

---

## 🔧 2. DSL → AST → SQL Rendering Pipeline

```
DSL string  →  DSL Parser  →  Expression AST  →  Dialect Renderer  →  Final SQL
```

Example DSL:
```
HASH256(
  CONCAT_WS('|',
    CONCAT('productid', '~', COALESCE({expr:productid}, 'null_replaced')),
    'pepper'
  )
)
```

AST (conceptual):
```
Hash256(
  ConcatWs('|', [
    Concat([
      Literal('productid'),
      Literal('~'),
      Coalesce(ColumnRef('productid'), Literal('null_replaced'))
    ]),
    Literal('pepper')
  ])
)
```

Dialect renderings:  
- **BigQuery** → `TO_HEX(SHA256(CONCAT_WS('|', ...)))`  
- **Databricks** → `SHA2(CONCAT_WS('|', ...), 256)`  
- **DuckDB** → `SHA256(CONCAT_WS('|', ...))`  
- **Fabric Warehouse** → `CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CAST(CONCAT_WS('|', ...) AS VARCHAR(4000))), 2)`  
- **MSSQL** → `CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT_WS('|', ...)), 2)`  
- **Postgres** → `ENCODE(DIGEST(CONCAT_WS('|', ...), 'sha256'), 'hex')`  
- **Snowflake** → `LOWER(TO_HEX(SHA2(CONCAT_WS('|', ...), 256)))`  

---

## 🔧 3. DSL Syntax

### 🧩 3.1 Supported core functions

| DSL Function | Description |  
|--------------|-------------|  
| `HASH256(expr)` | Vendor-neutral SHA‑256 hash wrapper |  
| `CONCAT(a,b,...)` | Null-propagating concatenation |  
| `CONCAT_WS(sep,a,b,...)` | Null-safe concatenation with separator |  
| `COALESCE(a,b)` | Standard SQL coalesce |  
| `COL(name)` | Column reference |  
| `{expr:column}` | Reference to upstream expression column |  

The DSL is intentionally minimal and safe.

### 🧩 3.2 Identifiers
- `COL(bk1)` and `COL("bk1")` behave equivalently.  
- Dialects re-apply proper quoting.  

### 🧩 3.3 Literals
String literals may be defined using `'...'` or `"..."`.

### 🧩 3.4 Upstream Expression References
Syntax:
```
{expr:column_name}
```
This refers to an upstream expression already defined in the execution graph.

---

## 🔧 4. DSL Parser

Located in: `metadata/rendering/dsl.py`

Responsibilities:  
1. Normalize input  
2. Detect function calls  
3. Parse nested expressions  
4. Parse literals  
5. Split arguments respecting parentheses  
6. Convert to AST nodes  

Specialized rules:  
- `COL(name)` → `ColumnRef`  
- `'literal'` → `Literal`  
- `{expr:x}` → `ExprRef`  

---

## 🔧 5. Expression AST Nodes

All expression classes derive from a common base.

### 🧩 5.1 Primitive Nodes
#### 🔎 `Literal(value)`
Represents a literal value in SQL.

#### 🔎 `ColumnRef(column_name, table_alias=None)`
Represents a reference to a column.

#### 🔎 `ExprRef(name)`
References an upstream-generated expression.

---

### 🧩 5.2 Function Expression Nodes
#### 🔎 `ConcatExpr(args)`
Represents `CONCAT(a,b,...)`.

#### 🔎 `ConcatWsExpr(separator, args)`
Represents `CONCAT_WS(sep, ...)`.

#### 🔎 `CoalesceExpr(a,b)`
Represents `COALESCE(a,b)`.

#### 🔎 `Hash256Expr(expr)`
Vendor-neutral representation of SHA‑256 hashing.

Each dialect chooses its own SQL form.

---

## 🔧 6. Window Functions

Represented by:
```
WindowFunctionExpr(
  function_name,
  partition_by=[...],
  order_by=[OrderItem(expr, direction)]
)
```

Example:
```
ROW_NUMBER() OVER (PARTITION BY src ORDER BY updated_at DESC)
```

Used primarily in multi-source Stage ranking mode.

---

## 🔧 7. Subqueries in the AST

Subqueries are modeled using:
```
SubquerySource(select, alias)
```

Example pattern:
```
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (...) AS rn
  FROM union_all
) AS ranked
WHERE rn = 1
```

The dialect handles parenthesis placement and alias rendering.

---

## 🔧 8. Dialect Rendering Responsibilities

Each SQL dialect must render:  
- literals  
- identifiers  
- CONCAT / CONCAT_WS  
- COALESCE  
- HASH256  
- window functions  
- subqueries  

Consistency across dialects is ensured because all begin from the same AST.

Examples:  
- **BigQuery**: `TO_HEX(SHA256(...))`  
- **Databricks**: `SHA2(..., 256)`  
- **DuckDB**: `SHA256(...)`  
- **Fabric Warehouse**: `CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', ...), 2)`  
- **MSSQL**: `HASHBYTES('SHA2_256', ...)`  
- **Postgres**: `ENCODE(DIGEST(...), 'hex')`  
- **Snowflake**: `LOWER(TO_HEX(SHA2(..., 256)))`  

---

## 🔧 9. Use in Surrogate & Foreign Keys

The SK/FK hashing pipeline uses the DSL and AST exclusively.

Guarantees:  
- deterministic key generation  
- lexicographically ordered BK parts  
- proper literal separators: `'~'` (pair) and `'|'` (between pairs)  
- null-protection using `COALESCE`  
- dialect-consistent hashing algorithms  

---

## 🔧 10. Benefits of the DSL & AST

- deterministic and reproducible SQL  
- clean abstraction from vendor SQL  
- first-class testability  
- enables multi-dialect rendering  
- no SQL in metadata  
- simple addition of new dialects  
- supports window functions and subqueries  

---

© 2025-2026 elevata Labs — Internal Technical Documentation
