# ⚙️ Query Builder & Query Tree

This document explains elevata’s **Query Builder** and the underlying **Query Tree** concept.  
It focuses on **why** this exists, **when** to use it, and **how** it fits into elevata’s metadata-driven execution model.

---

## 🔧 1. Why Query Trees exist

elevata generates SQL from metadata by default:

**Metadata → Logical Plan → Dialect Rendering → SQL**

This works for the majority of datasets because joins, columns, lineage, and expressions already describe
a dataset’s intent.

However, some transformations require an explicit **query shape** that cannot be inferred safely from
the dataset definition alone, for example:

- window functions (ROW_NUMBER, LAG/LEAD, ranking)  
- multi-step shaping (subselect boundaries)  
- UNION composition across different branches  
- controlled aggregation steps (group keys vs measures)

The Query Tree provides this explicit query shape — while keeping elevata’s guarantees around governance,
determinism, and reproducibility.

See also: [Lineage Model & Logical Plan](logical_plan.md) and [Expression DSL & AST](expression_dsl_and_ast.md).

---

## 🔧 2. Two modes: Standard SQL vs Custom Query Logic

### 🧩 2.1 Standard SQL (default)
If no Query Tree is configured, elevata builds SQL automatically from the dataset definition:

- inputs / joins (metadata)  
- column mappings (metadata)  
- expressions (DSL/AST)  
- dialect rendering

This is the default and recommended approach whenever possible.

### 🧩 2.2 Custom Query Logic (opt-in)
For **bizcore** and **serving** datasets, custom query logic can be enabled.  
Enabling custom logic creates an explicit Query Tree on top of the dataset definition.

In the UI, this is presented as:

- “Enable custom query logic”  
- “Disable / reset” (safe rollback to standard generation)

The goal is to provide advanced shaping without turning elevata into a manual SQL editor.

> *Complex aggregations are expected to be modeled as separate datasets.*

---

## 🔧 3. What the Query Tree is (and what it is not)

### 🧩 3.1 What it is
The Query Tree is a structured, explicit plan that defines how a dataset is computed.  
It is composed of typed nodes (operators) such as:

- Select node  
- Aggregate node  
- Union node  
- Window node

Each node has clear inputs and produces an inferred output contract.

### 🧩 3.2 What it is not
- Not a BI semantic layer  
- Not a query-time metric store  
- Not a drag & drop graph editor (future possible)  
- Not a place to store vendor-specific SQL

The Query Tree remains **metadata-native**: it compiles into the same vendor-neutral  
Logical Plan and Expression AST that power the rest of elevata.

---

## 🔧 4. Layer semantics: where Query Trees are allowed

Custom query logic is restricted to **semantic layers**:

- ✅ allowed: `bizcore`, `serving`  
- ❌ not allowed: `raw`, `stage`, `rawcore`

Rationale:  
raw/stage/rawcore are generated layers with strict, system-managed behavior.  
bizcore/serving are the correct place for business and presentation shaping.

---

## 🔧 5. Output contract inference

A key design principle: **the Query Tree defines the dataset contract**.

- the output columns of the root node are inferred from the tree  
- computed columns (aggregates, window columns) become part of the dataset contract  
- serving can “see” and build on computed upstream columns

This avoids the classic drift between “what the SQL does” and “what metadata says the dataset provides”.

---

## 🔧 6. Determinism and governance

elevata treats deterministic execution as a correctness feature.

Examples:

- Ranking / navigation window functions require ORDER BY  
- Some aggregates require explicit ordering to avoid nondeterministic results  
- Column name collisions must be resolved deterministically

The Query Builder surfaces determinism as:

- “Deterministic” badge  
- “Needs ordering” badge  
- Errors vs Warnings

See: [Determinism and execution semantics](determinism_and_execution_semantics.md)

---

## 🔧 7. Typical use cases

Use custom query logic when you need:

- **Windowed ranking** for stable selection (e.g., latest record per entity)  
- **Union composition** across multiple shaped branches  
- **Explicit aggregation** as a stable intermediate step  
- **Presentation shaping** in serving without leaking SQL to BI tools

Avoid custom logic when a dataset can be expressed purely via metadata inputs + columns.

---

## 🔧 8. UI workflow (Query Builder)

The Query Builder guides the user through a safe sequence:

1) Decide between Standard SQL and Custom Query Logic  
2) Enable custom logic (creates query root)  
3) Add nodes (e.g. window node)  
4) Review inferred contract  
5) Validate governance & determinism  
6) Preview SQL  
7) Reset/disable if needed

This ensures users are guided, not overwhelmed.

---

© 2025-2026 elevata Labs — Internal Technical Documentation
