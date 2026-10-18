# ⚙️ Expression DSL & AST

elevata represents expression semantics independently from SQL dialect syntax. Two portable metadata forms feed the same Expression AST:

- generated Expression DSL used by deterministic key and system expressions  
- structured Portable Expression contracts used for authored calculated values and predicates  

The AST is rendered by the active dialect for BigQuery, Databricks, DuckDB, Fabric Warehouse, MSSQL, Postgres, and Snowflake.

---

## 🔧 1. Expression architecture

```text
Generated DSL -------------------┐
                                 ├-> Expression AST -> Dialect Renderer -> SQL
Portable Expression contract ----┘
```

The AST describes expression meaning. Dialects own platform syntax. Vendor-specific SQL is not part of a portable expression contract.

This separation provides:

- deterministic rendering  
- portable authored transformation logic  
- controlled function and cast semantics  
- fail-closed validation of unsupported or ambiguous expressions  
- one expression model for preview and execution  

---

## 🔧 2. Portable Expression contracts

Portable Expression contracts are structured JSON metadata edited through the Portable Expression Editor. The JSON contract is an internal persistence form; the UI presents expression structure and values rather than requiring manual JSON authoring.

Contracts are normalized before persistence and resolved into AST nodes when a query plan is built. Persisted column references use logical names rather than local database IDs.

Portable expressions are supported for:

- `TargetColumn.portable_expression`  
- input-specific `TargetColumnInput.portable_expression`  
- `SourceDataset.portable_static_filter`  
- `SourceDataset.portable_increment_filter`  
- Query FILTER predicates  

Input-specific TargetColumn expressions take precedence over the column-level expression for that input mapping.

---

## 🔧 3. Value expressions

Portable value expressions support these semantic families:

### 🧩 Primitive values

- column reference  
- literal value  
- runtime value where the owning contract explicitly allows it  

### 🧩 Arithmetic

- addition  
- subtraction  
- multiplication  
- division  
- modulo  
- unary positive/negative operations  

### 🧩 Conditional logic

- comparisons  
- boolean `AND` / `OR`  
- `NOT`  
- `IS NULL` / `IS NOT NULL`  
- `BETWEEN`  
- `IN`  
- searched `CASE` expressions  

### 🧩 Conversion and scalar functions

- controlled logical casts  
- controlled portable scalar functions  
- `CONCAT`  
- `CONCAT_WS`  
- `COALESCE`  
- SHA-256 hashing expressions  

Function names and cast targets are validated against portable contracts. Vendor-specific aliases and vendor-only types are rejected.

---

## 🔧 4. Predicate contracts

Predicate contracts use the same expression semantics but require a boolean result. They are used by Query FILTER nodes and SourceDataset extraction filters.

A SourceDataset static filter can reference columns from that SourceDataset.

A SourceDataset increment filter can additionally reference the runtime value `delta_cutoff`. The increment boundary is resolved by the load runtime and injected into the portable predicate during SQL generation.

Source predicates do not reference TargetColumns, QueryNodes, aggregates, windows, subqueries, or arbitrary SQL.

Query FILTER predicates resolve column references against the output contract of their input QueryNode. Unknown or ambiguous references fail closed.

---

## 🔧 5. Generated Expression DSL

The generated DSL remains the canonical compact representation for deterministic system expressions such as surrogate-key and foreign-key hashing.

Example:

```text
HASH256(
  CONCAT_WS('|',
    CONCAT('productid', '~', COALESCE({expr:productid}, 'null_replaced')),
    {runtime:pepper}
  )
)
```

Core DSL forms include:

| DSL form | Meaning |
|---|---|
| `HASH256(expr)` | portable SHA-256 expression |
| `CONCAT(a,b,...)` | concatenation |
| `CONCAT_WS(sep,a,b,...)` | separator-based concatenation |
| `COALESCE(a,b)` | first non-null value |
| `COL(name)` | column reference |
| `{expr:column}` | upstream expression reference |
| `{runtime:pepper}` | environment-local hashing pepper binding |

The concrete pepper value is resolved at runtime and is never persisted in portable metadata.

---

## 🔧 6. Core AST nodes

The Expression AST contains typed nodes rather than SQL fragments. Core node families include:

- `Literal`  
- `ColumnRef`  
- `ExprRef`  
- arithmetic and unary arithmetic expressions  
- comparison and boolean expressions  
- null/between/in predicates  
- `CaseWhen` / `CaseBranch`  
- cast expressions  
- controlled scalar-function expressions  
- concat/coalesce/hash expressions  
- `WindowFunctionExpr`  
- raw SQL nodes only for non-portable compatibility/runtime boundaries  

Portable metadata normalization never accepts RawSql as a portable expression.

---

## 🔧 7. Window expressions

Window expressions combine a registered function contract with arguments, partitioning, ordering, and an optional explicit frame.

```text
WindowFunctionExpr(
  function_name,
  args=[...],
  partition_by=[...],
  order_by=[...],
  frame=...
)
```

The Query Builder validates function-specific argument counts and types before rendering. Query-level window semantics are described in [Query Builder & Query Tree](query_builder_and_query_tree.md).

---

## 🔧 8. Dialect rendering

All supported dialects receive the same semantic AST and render platform-specific syntax for:

- identifiers and literals  
- arithmetic and comparison operators  
- boolean logic and CASE  
- logical casts  
- controlled scalar functions  
- CONCAT / CONCAT_WS / COALESCE  
- hashing  
- window functions and frames  
- subqueries and UNIONs through the Logical Plan  

This is the portability boundary: the metadata contract describes meaning, while the dialect describes SQL syntax.

---

## 🔧 9. Fail-closed resolution

Portable expressions are validated against their owning input contract.

- unknown column -> error  
- ambiguous column -> error  
- unsupported function -> error  
- invalid argument shape -> error  
- unsupported cast target -> error  
- invalid runtime value -> error  
- vendor-specific SQL or type inside a portable contract -> error  

No ambiguous expression is silently repaired or guessed.

---

## 🔧 10. Deterministic keys

Surrogate-key and foreign-key hashing use the generated DSL and AST with deterministic component ordering, null handling, separators, and environment-local pepper resolution. Rawcore key propagation accepts only direct portable column references where a key component must remain traceable to one upstream field.

See [Hashing Architecture](hashing_architecture.md).

---

© 2025-2026 elevata - Technical Documentation
