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
Metadata → Logical Plan → Expression AST → Dialect Renderer → Beautifier → UI
```

Step-by-step:

1. **Metadata lookup**
   - The TargetDataset is loaded together with its upstream inputs.  
   - All TargetColumns are included in ordinal order.  

2. **Logical Plan generation**
   - The builder creates a `LogicalSelect`, `LogicalUnion`, or `SubquerySource` depending on dataset type.  
   - Multi-source Stage datasets produce subqueries and window functions.  
   - Incremental models may produce `MERGE` plans.  

3. **Expression AST construction**
   - Column expressions (including hashing) are translated into AST nodes.  
   - Dialect-neutral functions (CONCAT_WS, COALESCE, ROW_NUMBER) are represented explicitly.  

4. **Dialect selection**
   - The active dialect is determined by:  
     - explicit URL parameter (`?dialect=postgres`)  
     - profile configuration  
     - environment variable (`ELEVATA_SQL_DIALECT`)  
     - fallback: DuckDB

5. **SQL rendering**
   - The dialect walks the Logical Plan and Expression AST.  
   - Identifiers, literals, CONCAT, COALESCE, hashing, window functions, and subqueries are rendered.

6. **Optional beautification**
   - SQL is passed through an optional formatter for consistency.

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

The preview does not simplify or truncate SQL. It always shows:  

- full SELECT list, including surrogate keys and foreign key expressions  
- complete UNION trees  
- generated technical fields (e.g. `__src_rank_ord`)  
- CASE/COALESCE/CONCAT logic  

This ensures that the preview accurately reflects the actual SQL generator.

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

## 🔧 5. Caching Considerations

Currently the SQL preview is regenerated on every request. In practice:  

- Logical Plan generation is lightweight  
- SQL rendering is fast even for large DAGs  

---

## 🔧 6. Error Handling

If SQL generation fails (rare), the preview area displays:  

- a message indicating the error  
- the exception message in development  

Errors are never swallowed silently.

---

## 🔧 7. Why Preview and Load Use the Same Engine

elevata does **not** generate a separate preview version of the SQL.  

The SQL preview shows the *exact* SQL that the Load Runner will execute.  

This ensures:  
- consistency  
- testability  
- predictability  
- easier debugging  

If the preview looks correct, the actual load SQL is correct.

---

## 🔧 8. Summary

The SQL Preview pipeline provides:  

- real SQL from real metadata  
- real dialect rendering  
- fast feedback through HTMX  
- stable formatting via the Logical Plan and AST  

This architecture enables accurate previews today and paves the way for:  
- cross-dialect comparisons  
- diff views  
- preview caching  
- incremental preview of specific pipeline sections  

The preview pipeline is a key part of elevata’s transparency and usability.

---

© 2025 elevata Labs — Internal Technical Documentation