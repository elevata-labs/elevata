# ⚙️ Query Builder & Query Tree

This document explains elevata’s **Query Builder** and the underlying **Query Tree** concept.  
It also clarifies the meaning of **query_root** and **query_head** as used in the UI and SQL generation.  
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

## 🔧 3 query_root vs query_head (core concept)

When custom query logic is enabled, elevata stores two pointers on the TargetDataset:

- **query_root**: the *stable anchor* of the query tree  
  - created when you “Enable custom query logic”  
  - typically the initial **Base select** node  
  - used as the safe starting point for the tree  
  - should remain stable across most edits

- **query_head**: the *current leaf / SQL endpoint*  
  - points to the node that represents the dataset’s final query shape  
  - SQL preview and contract inference are based on the **head**  
  - when you add operators (AGGREGATE / UNION / WINDOW), the new operator becomes the new **head**  
  - the head may “move” as you iterate

In short:

- The Query Contract and all governance checks are evaluated at the **head** (or root fallback).  
- **Root = anchor**  
- **Head = current result**

Why this matters:

- The UI shows the “current head” so users understand what defines the dataset output *right now*.  
- Determinism and governance checks are evaluated at the head.  
- Some operations are blocked if downstream datasets depend on the current contract (head output).

### 🧩 Fallback solution

If `query_head` is NULL but `query_root` exists, elevata treats `query_root` as the effective head.  
This ensures stable behavior for early datasets and keeps SQL preview/contract inference deterministic.

---

## 🔧 4. What the Query Tree is (and what it is not)

### 🧩 4.1 What it is
The Query Tree is a structured, explicit plan that defines how a dataset is computed.  
It is composed of typed nodes (operators) such as:

- Select node  
- Aggregate node  
- Union node  
- Window node

Each node has clear inputs and produces an inferred output contract.

### 🧩 4.2 What it is not
- Not a BI semantic layer  
- Not a query-time metric store  
- Not a drag & drop graph editor (future possible)  
- Not a place to store vendor-specific SQL

The Query Tree remains **metadata-native**: it compiles into the same vendor-neutral  
Logical Plan and Expression AST that power the rest of elevata.

---

## 🔧 5. Layer semantics: where Query Trees are allowed

Custom query logic is restricted to **semantic layers**:

- ✅ allowed: `bizcore`, `serving`  
- ❌ not allowed: `raw`, `stage`, `rawcore`

Rationale:  
raw/stage/rawcore are generated layers with strict, system-managed behavior.  
bizcore/serving are the correct place for business and presentation shaping.

---

## 🔧 6. Output contract inference

A key design principle: **the Query Tree defines the dataset contract**.

- the output columns of the root node are inferred from the tree  
- computed columns (aggregates, window columns) become part of the dataset contract  
- serving can “see” and build on computed upstream columns

This avoids the classic drift between “what the SQL does” and “what metadata says the dataset provides”.

Query-derived columns (aggregates, window outputs, union outputs) are automatically synchronized to the dataset schema.  
The physical schema therefore always reflects the inferred query contract of the current query head.

---

## 🔧 7. Determinism and governance

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

## 🔧 8. UNION validation & contract semantics

UNION nodes are subject to stricter validation rules than linear query nodes,
because they merge multiple independent query branches into a single output
contract.

### 🧩 What is validated?

When validating a UNION, elevata checks:

- the UNION defines an explicit output schema  
- all output column names are unique (case-insensitive)  
- at least one branch exists  
- each branch has a valid input node  
- each branch provides exactly one mapping per UNION output column  
- all mapped input columns exist in the branch input contract  
- no output column is mapped more than once per branch

These checks are performed using the same query contract inference that powers  
SQL generation and governance indicators.

### 🧩 When does validation run?

UNION validation is **not executed automatically on every edit**.

Instead, elevata provides an explicit **“Validate UNION”** action in the
Query Builder UI:

- validation is executed on demand  
- results are shown inline, without leaving the current context  
- issues are reported as errors or warnings with clear explanations

This keeps the UI responsive while making validation a conscious, transparent step.

### 🧩 Relationship to query_head

UNION validation is evaluated against the current `query_head`.

Changing the query head may therefore:  
- change the effective output contract  
- introduce or resolve UNION validation issues

Users are encouraged to validate the UNION whenever:  
- output columns change  
- branch mappings are modified  
- the query head is moved across UNION boundaries

In short:  
> A UNION is only considered safe if its validation passes for the current query head.

---

## 🔧 9. UNION validation (UI guardrail)

On UNION-related pages (Branches / Output columns), elevata provides a **Validate UNION** action.

What it does:  
- Runs UNION-specific integrity checks (output schema, mappings, branch input contracts).  
- Shows issues as **Errors vs Warnings** in a compact panel.  
- If issues are detected, the UI automatically scrolls to the **first affected branch**  
  and highlights it briefly, so you can fix problems without hunting.

Tip:  
- Run **Validate UNION** after:  
  - adding/removing output columns  
  - changing a branch input node  
  - using “Auto-map by name”

---

## 🔧 10. Typical use cases

Use custom query logic when you need:

- **Windowed ranking** for stable selection (e.g., latest record per entity)  
- **Union composition** across multiple shaped branches  
- **Explicit aggregation** as a stable intermediate step  
- **Presentation shaping** in serving without leaking SQL to BI tools

Avoid custom logic when a dataset can be expressed purely via metadata inputs + columns.

---

## 🔧 11. UI workflow (Query Builder)

The Query Builder guides the user through a safe sequence:

1) Decide between Standard SQL and Custom Query Logic  
2) Enable custom logic (creates query root; initially root=head)  
3) Add nodes (e.g. window node)  
4) Review inferred contract  
5) Validate governance & determinism  
6) Preview SQL  
7) Reset/disable if needed

This ensures users are guided, not overwhelmed.

---

© 2025-2026 elevata Labs — Internal Technical Documentation
