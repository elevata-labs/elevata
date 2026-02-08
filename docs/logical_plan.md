# ⚙️ Logical Plan

This document describes the **Logical Plan** layer of elevata, which includes:

- Subqueries in the FROM clause  
- Window Functions (ROW_NUMBER, etc.)  
- Multi-source Stage logic (identity vs non-identity)  
- Updated Select/Union/Source nodes  
- Integration with the Expression DSL & AST  
- Dialect-safe rendering  

---

## 🔧 1. Purpose of the Logical Plan

The Logical Plan is the **dialect-agnostic intermediate representation** between:

```
Metadata → Logical Plan → Dialect Renderer → SQL
```

It describes *what* needs to be executed, not *how* a specific SQL dialect expresses it.  

The Logical Plan:  
- is fully structured (tree-based)  
- has no vendor SQL  
- is deterministic  
- is safe for testing  
- can be rendered to any SQL dialect (BigQuery, Databricks, DuckDB, Fabric Warehouse, MSSQL, Postgres, SNowflake)  

---

## 🔧 2. Query Tree and Logical Plan

The **Query Tree** operates at a higher abstraction level than the Logical Plan.

- The Query Tree defines *what* operations occur and in which order  
  (e.g. SELECT → WINDOW → AGGREGATE).  
- The Logical Plan defines *how* these operations are represented as  
  structured SQL components (LogicalSelect, SubquerySource, LogicalUnion, …).

When custom query logic is enabled, the Query Tree is compiled into a Logical Plan  
using the same builder infrastructure as default generation.

This design keeps the Logical Plan as a stable, vendor-neutral intermediate representation,  
regardless of whether SQL is generated automatically or via an explicit Query Tree.

---

## 🔧 3. Core Node Types

### 🧩 3.1 LogicalSelect
Represents a SELECT statement.  

Fields:  
- `select_list: list[SelectItem]`  
- `from_: SourceNode` (table source, subquery, union, etc.)  
- `where: Optional[Expr]`  
- `group_by: list[Expr]`  
- `order_by: list[OrderItem]`  

Example structure:
```
LogicalSelect(
  select_list=[SelectItem(ColumnRef("a")), SelectItem(ColumnRef("b"))],
  from_=TableSource("raw_sales", alias="r")
)
```

---

### 🧩 3.2 SelectItem
Represents a single column in the SELECT list.  

Fields:  
- `expr: Expr`  
- `alias: Optional[str]`  

---

### 🧩 3.3 Source Nodes

#### 🔎 TableSource
```
TableSource(table_name, alias)
```
#### 🔎 SubquerySource
Represents:
```
(SELECT ...) AS alias
```

Used for:  
- multi-source Stage ranking  
- derived tables  
- complex transformations  

### 🧩 3.4 LogicalUnion
Represents a UNION or UNION ALL.

Fields:
- `selects: list[LogicalSelect]`
- `union_type: "ALL" | "DISTINCT"`

Rendered as:
```
SELECT ...
UNION ALL
SELECT ...
```

often wrapped in a SubquerySource.

---

## 🔧 4. Window Functions

Window functions appear via:
```
WindowFunctionExpr(
  function_name="ROW_NUMBER",
  partition_by=[...],
  order_by=[OrderItem(...)]
)
```

Example AST node inside a SelectItem:
```
SelectItem(
  expr=WindowFunctionExpr(
    function_name="ROW_NUMBER",
    partition_by=[ColumnRef("business_key")],
    order_by=[OrderItem(ColumnRef("updated_at"), "DESC")]
  ),
  alias="__src_rank_ord"
)
```

Used primarily in Stage **non-identity** mode.

---

## 🔧 5. Subqueries in Multi-Source Stage

Multi-source Stage requires optional ranking logic:

### 🧩 Identity Mode
- Each upstream dataset contributes a `source_identity_id` literal.  
- No ranking is needed.  
- We use **raw UNION ALL**:  

```
LogicalUnion([sel1, sel2, ...])
```

### 🧩 No Subquery Required.

---

### 🧩 Non-Identity Mode
- UNION ALL is wrapped into a SubquerySource.  
- Outer SELECT applies ROW_NUMBER and filters.  

Logical Plan pattern:
```
LogicalSelect(
  select_list=[...],
  from_=SubquerySource(
    select=LogicalSelect(
      select_list=[..., WindowFunctionExpr(...) AS __src_rank_ord],
      from_=LogicalUnion([...])
    ),
    alias="ranked"
  ),
  where=ComparisonExpr(ColumnRef("__src_rank_ord"), "=", Literal(1))
)
```

---

## 🔧 6. Integration With Expression DSL & AST

All expressions inside the Logical Plan use the DSL/AST layer:  
- column references  
- literals  
- CONCAT/CONCAT_WS  
- COALESCE  
- HASH256  
- window functions  

This ensures cross-dialect consistency.

---

## 🔧 7. Dialect Rendering Responsibilities

Each dialect must render:  
- SELECT lists  
- window functions  
- subqueries  
- unions  
- column references with quoting rules  
- expressions via the DSL AST  

Example dialect responsibilities:  

### 🧩 BigQuery
- window functions: identical to ANSI  
- hashing: `TO_HEX(SHA256(<expr>))`  
- table addressing: dataset-qualified identifiers

### 🧩 Databricks
- window functions: identical to ANSI / Spark SQL  
- hashing: `SHA2(<expr>, 256)`  
- execution: runs on Databricks SQL Warehouse (Unity Catalog for catalog/schema)

### 🧩 DuckDB
- window functions: identical to ANSI  
- hashing: SHA256()  

### 🧩 Fabric Warehouse
- hashing via `HASHBYTES('SHA2_256', <expr>)`  
- convert binary → hex via `CONVERT(VARCHAR(64), ..., 2)`  
- DDL idempotency: `IF OBJECT_ID(...) IS NULL` pattern (no `CREATE TABLE IF NOT EXISTS`)

### 🧩 MSSQL
- hashing via `HASHBYTES('SHA2_256', ...)`  
- convert binary → hex via `CONVERT(VARCHAR(64), ..., 2)`  

### 🧩 Postgres
- subqueries: `(SELECT ...) AS alias`  
- hashing: `ENCODE(DIGEST(...), 'sha256'), 'hex')`  

### 🧩 Snowflake
- window functions: identical to ANSI  
- hashing: `LOWER(TO_HEX(SHA2(<expr>, 256)))`  
- identifiers: quoted via `"..."` and database/schema-qualified rendering

---

## 🔧 8. Logical Plan Rendering Rules (Dialect-Agnostic)

### 🧩 8.1 SELECT
```
SELECT <select_list>
FROM <source>
[WHERE <expr>]
[GROUP BY <exprs>]
[ORDER BY <order_items>]
```

### 🧩 8.2 UNION
```
SELECT ...
UNION ALL
SELECT ...
```

### 🧩 8.3 Subquery
```
(
  SELECT ...
) AS alias
```

### 🧩 8.4 Window Function
```
ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
```

Dialect modifies only:  
- quoting  
- hashing  
- type conversions  

---

## 🔧 9. How the Builder Constructs Logical Plans

Key builder patterns:

### 🧩 9.1 Single-Source Stage
```
LogicalSelect(from_=TableSource(...))
```

### 🧩 9.2 Multi-Source Stage Identity
```
LogicalUnion([...])
```

### 🧩 9.3 Multi-Source Stage Non-Identity
```
SubquerySource(
  select=LogicalSelect(
    select_list=[..., WindowFunctionExpr(...)],
    from_=LogicalUnion([...])
  )
)
```
Outer filter applied via another LogicalSelect.

---

## 🔧 10. Benefits of the Logical Plan
- Supports subqueries as first-class citizens  
- Vendor-neutral window functions  
- Clean separation between logic and SQL syntax  
- Extensible architecture (CASE WHEN, JOIN, GROUPING SETS)  
- Deterministic behavior for multi-source processing  
- No vendor SQL stored in metadata  
- Strict testability  

---

## 🔧 11. Bizcore and the Logical Plan

Bizcore datasets do not introduce new Logical Plan node types.

Instead:

- Bizcore logic is expressed through existing constructs:  
  - joins  
  - expressions  
  - derived SelectItems  
- Business rules compile into standard AST expressions  
- No semantic shortcuts or abstractions exist at plan level

This design guarantees:

- identical behavior between technical and business datasets  
- full reuse of planning, validation, and rendering logic  
- uniform explainability across layers

From the Logical Plan’s perspective,
Bizcore is **just another deterministic dataset** —
with richer intent, not different mechanics.

---

© 2025-2026 elevata Labs — Internal Technical Documentation
