# Generation Logic

This document describes how elevata transforms *metadata* into a structured, dialect-neutral **Logical Plan** and then into optimized SQL through the Expression AST and dialect adapters. It reflects the architecture as of v0.5.x and is intentionally version-agnostic.

---

# 1. Overview

elevata follows a simple principle:

> **Metadata-in → SQL-out**

The generator inspects datasets, columns, lineage, relationships, and configuration to produce a stable, deterministic Logical Plan. From this plan, SQL is rendered through dialect adapters.

Pipeline:

```
Metadata → Logical Plan → Expression AST → Dialect Rendering → SQL
```

---

# 2. Dataset Types & Generation Rules

The generation logic depends heavily on the dataset type.

## 2.1 RAW
- Direct mapping from source fields  
- Column expressions are simple column references  
- No surrogate keys or transformations

## 2.2 STAGE
STAGE layers unify multiple upstream sources:

### Identity Mode
Used when the upstream provides a `source_identity_id`.  
- No ranking logic  
- Multi-source is handled via `UNION ALL`  
- Each branch injects a literal identity ID

### Non-Identity Mode
Used when multiple upstream sources require conflict resolution.  
- All branches are UNIONed  
- Wrapped into a subquery  
- A `ROW_NUMBER() OVER (...)` window assigns a rank  
- Only rows with rank = 1 are selected

## 2.3 CORE / BUSINESS
- Surrogate keys are generated from BK columns  
- Foreign keys reference parent surrogate key structure  
- Expression AST builds deterministic hashing expressions

---

# 3. Column Expression Generation

Each target column is associated with a **Column Mapping** and an expression. Expressions are built using the **Expression DSL** and then parsed into the Expression AST.  

Example DSL:
```text
HASH256(
  CONCAT_WS('|',
    CONCAT('productid', '~', COALESCE({expr:productid}, 'null_replaced')),
    'pepper'
  )
)
```

The builder never writes SQL directly.

---

# 4. Business Keys & Surrogate Keys

## 4.1 Business Keys (BK)
- Defined in metadata per dataset  
- Sorted lexicographically for deterministic ordering  
- Used as inputs to surrogate key expressions

## 4.2 Surrogate Key Expression
Surrogate keys use a fully dialect-agnostic hashing pattern:  

- Each BK yields a *pair expression*: `CONCAT(name, '~', COALESCE(value, 'null_replaced'))`  
- All pairs joined via `CONCAT_WS('|', ...)`  
- Pepper appended as last component  
- Entire structure wrapped in `HASH256()`  

The resulting Expression AST is rendered differently depending on the dialect.

---

# 5. Foreign Keys

Foreign keys reuse the exact same hashing structure as the parent surrogate key, but with child column references.

Process:  
1. Inspect parent BK columns  
2. Build pair expressions with child columns  
3. Build ordered AST structure  
4. Wrap in `Hash256Expr`  

This guarantees SK/FK parity across dialects.

---

# 6. Expression AST

All expressions use a vendor-neutral AST:  
- `ColumnRef`  
- `Literal`  
- `ExprRef`  
- `ConcatExpr`  
- `ConcatWsExpr`  
- `CoalesceExpr`  
- `WindowFunctionExpr`  
- `Hash256Expr`  

The AST is consumed by the dialect renderer, which decides on actual SQL syntax.

---

# 7. Logical Plan Construction

Logical Plans represent SQL structures without dialect specifics.  

Main node types:  
- `LogicalSelect`  
- `LogicalUnion`  
- `SubquerySource`  
- `LogicalJoin` (future)  

Examples:

### Single-source STAGE
```
LogicalSelect(
  from_=source,
  select_list=[...]
)
```

### Multi-source STAGE (identity mode)
```
LogicalSelect(
  from_=LogicalUnion([branch1, branch2])
)
```

### Multi-source STAGE (non-identity mode)
```
LogicalSelect(
  from_=SubquerySource(
    select=LogicalSelect(
      from_=LogicalUnion(...),
      select_list=[..., WindowFunctionExpr(...)]
    ),
    alias="ranked"
  ),
  where=rank == 1
)
```

---

# 8. Dialect Rendering

After Logical Plan + AST construction, SQL is produced by:

```
dialect.render_select(logical_select)
```

Dialect responsibilities:  
- identifier quoting  
- literal rendering  
- hashing syntax  
- function names  
- window functions  
- formatting  

The Logical Plan and AST guarantee correctness; the dialect guarantees syntactic validity.

---

# 9. Deterministic Generation

elevata enforces determinism:  
- Sorted BK pairs  
- Stable column ordering  
- Consistent naming of technical fields (e.g. `__src_rank_ord`)  
- Identical AST for SK/FK  

This ensures:  
- reproducible SQL  
- stable diffs  
- predictable behavior across dialects

---

# 10. Summary

The generation logic is the heart of elevata:  
- metadata describes the transformation  
- Logical Plan formalizes the operation  
- Expression AST encodes column semantics  
- Dialect renders valid SQL  

This architecture supports multiple SQL backends without changing metadata or Logical Plans.

---

© 2025 elevata Labs — Internal Technical Documentation