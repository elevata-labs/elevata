# ⚙️ Query Builder & Query Tree

The Query Builder models advanced transformation structure as explicit metadata. It keeps relational query shape separate from platform SQL while preserving query contracts, lineage, determinism, and dialect portability.

The central separation is:

```text
Portable Expression   -> What value does this expression produce?
Query Tree            -> Which rows and relational shape does this dataset produce?
```

Portable expressions therefore describe calculated values and predicates. Query nodes describe relational operations such as filtering, aggregation, union composition, and windowing.

---

## 🔧 1. Standard generation and explicit query logic

A TargetDataset without a Query Tree is generated from its dataset definition:

- dataset inputs and joins  
- TargetColumns and column-level lineage  
- portable expressions  
- dialect rendering  

For `bizcore` and `serving`, explicit query logic can be enabled. Enabling it creates a **Base select** QueryNode that uses the existing TargetDataset definition. Additional typed nodes build on that base contract.

Query Trees are not available for `raw`, `stage`, or `rawcore`. These layers remain governed by their integration and generation contracts.

---

## 🔧 2. Query root and query head

A TargetDataset with explicit query logic stores two pointers:

- **query_root** - the stable anchor of the Query Tree, normally the Base select  
- **query_head** - the node that defines the effective dataset output  

SQL preview, query contract inference, governance checks, and physical materialization use the effective head. If `query_head` is absent while `query_root` exists, the root is used as the effective head.

The head changes as typed operators are added or selected. The root remains the stable entry point into the dataset definition.

---

## 🔧 3. Query node semantics

The Query Tree supports these relational operators:

### 🧩 SELECT

The Base select compiles the owning TargetDataset definition, including its inputs, joins, authored TargetColumns, and portable expressions.

### 🧩 FILTER

FILTER consumes one input QueryNode and retains rows for which its portable predicate evaluates to true.

FILTER is a logical operator rather than a literal SQL `WHERE` fragment. The builder may introduce a subquery boundary when required to preserve node semantics. This is what allows filters before aggregation, after aggregation, or after windowing to remain explicit and predictable.

The visible output contract is inherited from the input node. Columns used only by the predicate may be pushed into inner projections without leaking into the final output.

### 🧩 AGGREGATE

AGGREGATE replaces its input schema with an explicit output contract consisting of:

```text
Group Keys + Measures
```

Group keys inherit their upstream datatype contract. Measures define aggregate function, input, output name, and function-specific options. Supported aggregate behavior includes:

- `COUNT(*)` and `COUNT(column)`  
- aggregate `DISTINCT` where supported by the function contract  
- `SUM`, `AVG`, `MIN`, `MAX`  
- ordered `STRING_AGG` with delimiter and reusable ORDER BY definition  

Numeric SUM/AVG semantics use canonical result types and are rendered consistently across dialects.

### 🧩 UNION

UNION combines independent QueryNode branch outputs under one explicit output schema. Branch mappings bind each UNION output column to one input-column name exposed by that branch.

A branch may reference a QueryNode owned by another TargetDataset. The branch plan is compiled inline, so the UNION consumes the referenced query definition rather than requiring that TargetDataset itself to be materialized first. Physical upstream dependencies remain governed by the underlying dataset inputs and joins.

Both `UNION` and `UNION ALL` are supported. UNION validation checks output uniqueness, branch inputs, complete mappings, and mapped input columns.

### 🧩 WINDOW

WINDOW extends its input schema with query-derived output columns. Supported functions include:

- `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`  
- `LAG`, `LEAD`  
- `FIRST_VALUE`, `LAST_VALUE`  
- `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`  

Window columns can reference reusable PARTITION BY and ORDER BY definitions. Explicit `ROWS` and `RANGE` frames are modeled with start/end boundaries and optional offsets. `COUNT(*) OVER (...)` is supported.

Function signatures and argument types are validated fail-closed. Ordered window aggregates and FIRST_VALUE/LAST_VALUE require an explicit frame so semantics remain portable. Numeric PRECEDING/FOLLOWING offsets with RANGE frames are rejected because their meaning is not portable across supported backends.

---

## 🔧 4. Portable predicates and expressions

Query FILTER predicates and calculated TargetColumn expressions are represented as structured, platform-neutral contracts. They are converted into the same Expression AST used by Logical Plan rendering.

Portable expressions can represent:

- column and literal values  
- arithmetic and unary arithmetic  
- comparisons and boolean logic  
- `NOT`, `IS NULL`, `BETWEEN`, and `IN`  
- `CASE`  
- controlled logical casts  
- controlled scalar functions  
- `CONCAT`, `CONCAT_WS`, `COALESCE`, and hashing  

Column references are resolved against the relevant input contract. Unknown or ambiguous references fail closed rather than being guessed.

See [Expression DSL & AST](expression_dsl_and_ast.md).

---

## 🔧 5. Query contracts and TargetColumns

The Query Tree defines the effective output contract of the dataset. Query-derived outputs are synchronized into TargetColumn metadata with `lineage_origin="query_derived"`.

The synchronization rules preserve authored metadata:

- authored TargetColumns are not deleted by Query Tree contract changes  
- query-derived columns are managed by Query Tree output semantics  
- aggregate output replacement and window output extension remain distinct  
- inactive authored inputs may remain available inside an inner plan when a downstream node requires them  
- physical CREATE/INSERT materialization uses the effective Query Tree output contract rather than every persisted TargetColumn  

This separation keeps authoring metadata reversible while ensuring the physical target matches the effective query output.

---

## 🔧 6. Join-aware input contracts

Joins remain part of the TargetDataset relationship model because they carry lineage and execution dependency semantics.

Query Builder column candidates are derived from the effective dataset/query contract. For joined datasets, candidate discovery includes:

1. authored TargetDataset columns  
2. left-side input columns  
3. right-side input columns in join order  

Duplicate names are de-duplicated deterministically with left-side precedence. This applies to filters, aggregate group keys, aggregate measures, and downstream query-node requirements.

---

## 🔧 7. Determinism and validation

Query logic is validated as architecture, not as arbitrary SQL text.

Blocking conditions include:

- missing or invalid query-node inputs  
- unknown or ambiguous portable expression references  
- invalid function signatures  
- missing required ORDER BY or window frames  
- invalid UNION output mappings  
- output-name collisions  
- Query Tree cycles  
- same-layer TargetDataset dependency cycles  

The Query Builder exposes deterministic/ordering status, query contract, Query Tree structure, SQL preview, and operator-specific validation.

See [Determinism & Execution Semantics](determinism_and_execution_semantics.md).

---

## 🔧 8. UI workflow

A typical workflow is:

1. Define the TargetDataset, inputs, joins, and TargetColumns.  
2. Use portable expressions for calculated column values where required.  
3. Enable explicit query logic for a Bizcore or Serving dataset.  
4. Add FILTER, AGGREGATE, UNION, or WINDOW nodes.  
5. Configure operator-specific contracts such as predicates, group keys, measures, mappings, arguments, ordering, partitioning, and frames.  
6. Review the Query Contract and Query Tree.  
7. Validate determinism and UNION integrity.  
8. Inspect dialect-specific SQL Preview.  
9. Execute through the normal Architecture Runtime and execution-scope contracts.  

Disabling/resetting explicit query logic returns the dataset to standard generation from its dataset definition.

---

© 2025-2026 elevata - Technical Documentation
