# ⚙️ SQL Rendering Conventions

This document describes how elevata renders SQL from the Logical Plan and Expression AST, independently of any specific dialect or version.  

The goal is to:  
- produce readable, reviewable SQL  
- keep formatting predictable  
- minimise dialect differences  
- support automated testing and diffing  

The actual syntax details (quoting, function names, hashing) are handled by the **dialect layer**.  
This document focuses on structure and layout.

---

## 🔧 1. General Principles

1. **Determinism** – the same Logical Plan and AST always produce the same SQL string (for a given dialect).  
2. **Readability** – SQL should be easy to understand for humans.  
3. **Stability** – small metadata changes should not cause large SQL diffs.  
4. **Abstraction** – Logical Plan and AST are vendor-neutral; dialects only handle surface syntax.  

---

## 🔧 2. Statement Structure

All SELECT statements follow the standard order of clauses:

```sql
SELECT <select_list>
FROM <source>
[WHERE <predicate>]
[GROUP BY <grouping_exprs>]
[HAVING <predicate>]
[ORDER BY <order_items>]
```

### 🧩 2.1 SELECT list

- One column/expression per line where practical.  
- Aliases are always rendered using `AS <alias>`.  
- Hidden technical columns (e.g. ranking ordinals) use a leading `__` prefix, e.g. `__src_rank_ord`.  

Example:

```sql
SELECT
  s."product_id" AS "product_id",
  s."product_name" AS "product_name",
  s."_load_ts" AS "_load_ts"
FROM ...
```

---

## 🔧 3. Identifier Conventions

Identifiers are stored **unquoted** in metadata and AST.  

Dialect-specific rules decide how they are quoted, but the conventions are:  

- table and column names retain their logical casing  
- aliases are always explicit  
- schema-qualified names are rendered as `schema.table AS alias` (with dialect quoting applied)  

Examples (conceptual):  

```sql
"schema"."table" AS "t"
"t"."column_name"
```

No dialect-specific quoting rules are embedded in the Logical Plan; the dialect decides the exact quoting syntax.

---

## 🔧 4. Literal Conventions

Literals are represented as `Literal(value)` in the AST. Rendering rules:  

- Strings use single quotes: `'value'` (escaped as needed)  
- Numbers appear as-is: `42`, `3.14`  
- Booleans use dialect-appropriate forms but AST conveys only `True`/`False`  
- Nulls are rendered as `NULL`  

String literals are treated as **data**, not identifiers, and never quoted with identifier syntax.

---

## 🔧 5. Expression Conventions

All expressions use the Expression AST derived from the DSL. Common patterns:

### 🧩 5.1 Column references

Represented as `ColumnRef(column_name, table_alias?)`.  

Rendered as:

```sql
"t"."column_name"
```

if a table alias is present, otherwise:

```sql
"column_name"
```

### 🧩 5.2 CONCAT and CONCAT_WS

String concatenation is expressed via:  

- `ConcatExpr(args)` → `CONCAT(a, b, ...)`  
- `ConcatWsExpr(separator, args)` → `CONCAT_WS(sep, a, b, ...)`  

No `||` or `+` operators are used directly in the Logical Plan; these functions are stable and null-aware.

### 🧩 5.3 COALESCE

Null handling uses `CoalesceExpr(a, b, ...)` and renders as:

```sql
COALESCE(a, b, ...)
```

### 🧩 5.4 HASH256 / Hashing

Hash expressions use a vendor-neutral `Hash256Expr(inner_expr)` in the AST. Dialects decide the exact function names, but the inner expression follows the same CONCAT/COALESCE conventions as any other string expression.

---

## 🔧 6. Window Functions

Window functions (e.g. `ROW_NUMBER()`) are expressed via `WindowFunctionExpr` and rendered using standard SQL syntax:

```sql
ROW_NUMBER() OVER (
  PARTITION BY <expr1>, <expr2>
  ORDER BY <expr3> [ASC|DESC]
)
```

Formatting conventions:  

- `OVER` clause is placed on the same line as the function name or on the next line as a block.  
- `PARTITION BY` and `ORDER BY` appear in that order inside the parentheses.  

Example:

```sql
ROW_NUMBER() OVER (
  PARTITION BY "src_identity"
  ORDER BY "_load_ts" DESC
) AS "__src_rank_ord"
```

---

## 🔧 7. Subqueries

Subqueries are rendered as parenthesised SELECT statements with an alias:

```sql
(
  SELECT
    ...
  FROM ...
) AS "alias"
```

Conventions:  

- Opening parenthesis on its own line  
- Inner SELECT indented  
- Closing parenthesis aligned with `FROM` clause  
- Alias always present  

Subqueries are used most prominently for multi-source Stage ranking:  

```sql
SELECT
  *
FROM (
  SELECT
    ...,  
    ROW_NUMBER() OVER (...) AS "__src_rank_ord"
  FROM ...
) AS "ranked"
WHERE "ranked"."__src_rank_ord" = 1
```

---

## 🔧 8. UNION and UNION ALL

Unions are rendered as:

```sql
SELECT ...
UNION ALL
SELECT ...
UNION ALL
SELECT ...
```

Conventions:  

- Each SELECT starts on a new line  
- `UNION` or `UNION ALL` in uppercase  
- No parentheses unless required for precedence or dialect quirks  

UNION nodes are often wrapped in a subquery when additional logic (e.g. ranking) needs to be applied on top of the union.

---

## 🔧 9. Ordering & Grouping

### 🧩 9.1 ORDER BY

Order items are rendered as:  

```sql
ORDER BY
  <expr1> ASC,
  <expr2> DESC
```

- one expression per line
- explicit direction (`ASC`/`DESC`) when required

### 🧩 9.2 GROUP BY

Grouping expressions follow a similar pattern:

```sql
GROUP BY
  <expr1>,
  <expr2>
```

Where possible, the same expression that appears in the SELECT list is reused to avoid ambiguity.

---

## 🔧 10. Hidden Technical Columns

Certain internal columns, used for ranking or internal bookkeeping, follow a clear convention:  

- prefixed with double underscore, e.g. `__src_rank_ord`  
- not surfaced in external models unless explicitly selected  

These columns are still rendered like any other column, but their naming makes their purpose obvious in the SQL.

---

## 🔧 11. Whitespace & Formatting

elevata enforces a consistent formatting style:  

- keywords in uppercase (`SELECT`, `FROM`, `WHERE`, ...)  
- one major clause per line (SELECT, FROM, WHERE, ...)  
- line breaks between complex sections (e.g. SELECT list vs FROM)  
- indentation for subqueries and window function bodies  

A SQL beautifier may be applied after rendering to ensure consistent whitespace, but the Logical Plan and AST are designed so that a stable, readable structure emerges even without heavy post-processing.

---

## 🔧 12. Dialect-Specific Differences

While the **structure** and **layout** are shared across dialects, the following are delegated to the dialect implementation:  

- exact identifier quoting syntax  
- boolean literal spelling  
- hashing functions (`HASHBYTES`, `DIGEST`, `SHA256`, ...)  
- casting / type conversion syntax  

The Logical Plan and Expression AST remain identical. Only the surface syntax differs.

---

## 🔧 13. Summary

These rendering conventions ensure that SQL generated by elevata is:  

- predictable and easy to diff  
- readable for humans  
- independent of any single engine  
- safe for multi-dialect environments  

They provide a stable foundation for future dialects (Snowflake, BigQuery, Databricks, …) without requiring changes to metadata or Logical Plan semantics.

---

## 🔧 14. Related Documents

- [Automatic Target Generation Logic](generation_logic.md)
- [Target Backends](target_backends.md)

---

© 2025 elevata Labs — Internal Technical Documentation
