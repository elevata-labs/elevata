# ⚙️ SQL Preview Pipeline

This document describes how elevata generates SQL previews inside the UI. It covers:

- how metadata becomes a Logical Plan  
- how the Logical Plan becomes SQL  
- how dialect selection interacts with preview generation  
- how HTMX is used to update the preview without reloading the page  
- how caching and formatting fit into the pipeline  

The preview pipeline uses the **same generator** that produces the actual load SQL. It does *not* use shortcuts or partial logic.

---

## 🔧 1. End-to-End Flow

When the user opens a TargetDataset detail page and clicks **“Show SQL Preview”**, the following pipeline runs:

```
Metadata → Logical Plan → Expression AST → Dialect Renderer → Presentation Formatter → UI
```

Step-by-step:

1. **Metadata lookup**  
     - The TargetDataset is loaded together with its upstream inputs.  
     - The effective dataset/query contract determines the visible output columns.  

2. **Logical Plan generation**  
     - The builder creates a `LogicalSelect`, `LogicalUnion`, or `SubquerySource` depending on dataset type.  
     - Multi-source Stage datasets produce subqueries and window functions.  
     - Incremental models may produce `MERGE` plans.  

3. **Expression AST construction**  
     - Portable calculated expressions, predicates, generated key expressions, and window expressions are translated into typed AST nodes.  
     - Arithmetic, CASE, comparisons, logical casts, controlled scalar functions, CONCAT/COALESCE/hashing, and window semantics remain dialect-neutral.  

4. **Dialect selection**  
     - The active dialect is determined by:  
         - explicit URL parameter (`?dialect=postgres`)  
         - profile configuration  
         - environment variable (`ELEVATA_SQL_DIALECT`)  
         - fallback: DuckDB

5. **SQL rendering**  
     - The dialect walks the Logical Plan and Expression AST.  
     - Identifiers, literals, CONCAT, COALESCE, hashing, window functions, and subqueries are rendered.

6. **Presentation formatting**  
     - UI preview SQL is formatted deterministically after dialect rendering.  
     - The formatter respects quoted identifiers and literals, nested subqueries, function arguments, runtime tokens, JOIN structure, predicates, windows, and UNION boundaries.  
     - Formatting changes presentation only; canonical execution SQL is not rewritten by the preview formatter.

7. **HTMX response**  
     - Only the SQL preview fragment is returned.  
     - The full page is *not* re-rendered.

---

## 🔧 2. Logical Plan → SQL Rendering

The Logical Plan is a structured representation that abstracts SQL syntax.

Key node types rendered by SQL Preview:

- `LogicalSelect`  
- `LogicalUnion`  
- `SubquerySource`  
- `ColumnRef`, `Literal`, `ExprRef`  
- window functions and other expressions  

The preview does not simplify or truncate the generated query. It shows the complete rendered structure, including:

- full SELECT lists, including surrogate keys and foreign key expressions  
- nested query levels and complete UNION trees  
- generated technical fields (e.g. `__src_rank_ord`)  
- CASE/COALESCE/CONCAT logic  
- JOIN predicates, filters, window expressions, and aggregate structure  

The UI may reformat whitespace and line breaks for readability, but the query semantics come from the same Logical Plan and dialect renderer used by execution.

### 🧩 Bizcore in SQL Preview

Bizcore datasets are rendered through the **same SQL Preview pipeline** as all other datasets.

This means:

- business rules are shown as real SQL  
- joins and calculations are fully expanded  
- aliases and lineage are explicit  
- preview SQL is derived from the same execution renderer before presentation-only formatting

There is no alternate Bizcore query model for preview.

The preview is derived from the same Logical Plan and dialect renderer used by execution; presentation formatting does not change query semantics.

This property is intentional and foundational:  
Bizcore semantics are **transparent, inspectable, and executable** - not inferred or resolved at query time.

---

## 🔧 3. Dialect Handling in the UI

The SQL preview page includes a dropdown for selecting dialects.

### 🧩 3.1 User selection

When the user changes the dropdown, HTMX makes a request to:

```
/metadata/target-datasets/<id>/sql-preview/?dialect=<dialect_name>
```

### 🧩 3.2 Backend processing

The view calls:

```python
dialect = get_active_dialect(request.GET.get("dialect"))
sql = render_select_for_target(dataset, dialect)
```

### 🧩 3.3 Response

The returned HTML fragment replaces only the preview block. The rest of the page remains unchanged.

---

## 🔧 4. HTMX Integration

HTMX provides:

- partial updates  
- reduced server load  
- fast interaction for switching dialects

### 🧩 4.1 Trigger

A button in the TargetDataset detail view triggers the initial preview load:

```html
<button hx-get=".../sql-preview" hx-target="#sql-preview-area">Show SQL</button>
```

### 🧩 4.2 Dialect switch

The dropdown uses:

```html
<select hx-get=".../sql-preview" hx-target="#sql-preview-area" name="dialect">...</select>
```

### 🧩 4.3 Server response

The view returns only:

```html
<pre>{{ sql }}</pre>
```

which is injected into the page.

No full page reloads occur.

---

## 🔧 5. Presentation formatting

SQL Preview uses a deterministic, structure-aware formatter after dialect rendering.

The formatter:

- indents nested subqueries consistently  
- aligns source tables and JOIN clauses below `FROM`  
- keeps `ON` attached to its JOIN while indenting the predicate  
- separates WHERE/HAVING boolean conditions predictably  
- keeps commas inside functions and string literals intact  
- preserves window expressions and runtime tokens  
- separates UNION / UNION ALL branches clearly  
- formats MERGE and delete-detection previews only when presentation output is requested  

Canonical SQL used outside presentation paths remains unchanged.

---

## 🔧 6. Caching Considerations

SQL preview is regenerated from the authoritative metadata contract on each request. In practice:

- Logical Plan generation is lightweight  
- SQL rendering and presentation formatting are deterministic  

---

## 🔧 7. Error Handling

If SQL generation fails (rare), the preview area displays:

- a message indicating the error  
- the exception message in development  

Errors are never swallowed silently. Query-backed previews also fail closed when the effective query contract is invalid instead of rendering SQL from stale or inconsistent mappings.

---

## 🔧 8. Why Preview and Load Use the Same Engine

elevata does **not** maintain a separate semantic SQL generator for preview.

Preview and execution use the same metadata, Logical Plan, and dialect renderer. The UI applies presentation-only formatting after canonical SQL generation.

This ensures:

- semantic consistency  
- testability  
- predictable dialect behavior  
- easier debugging  

The preview therefore reflects the same generated query semantics that execution consumes, while remaining optimized for human review.

---

## 🔧 9. Summary

The SQL Preview pipeline provides:

- real SQL derived from authoritative metadata  
- the same Logical Plan and dialect rendering semantics used by execution  
- fast feedback through HTMX  
- deterministic, structure-aware presentation formatting  
- fail-closed behavior for invalid query contracts  

The preview pipeline is a key part of elevata’s transparency and usability.

---

© 2025-2026 elevata - Technical Documentation