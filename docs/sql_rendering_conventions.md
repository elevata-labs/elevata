# 🧱 SQL Rendering & Alias Conventions

> This document defines how *elevata* renders SQL statements from metadata models.  
> The goal is to ensure **deterministic, vendor-neutral SQL output** across all dialects.

---

## 🧩 Core Principle

> Every **target column** in the rendered SQL must explicitly use an **alias (`AS target_column_name`)**.

This ensures that the final dataset’s column layout is:  
✅ deterministic (no dependency on source column order)  
✅ vendor-agnostic (works on DuckDB, BigQuery, Snowflake, Fabric, etc.)  
✅ lineage-compatible (each `TargetColumn` maps directly to its alias)  

The renderer now also supports automatic column alignment in UNION queries (v0.3.0).

---

## 📌 Example: Simple Mapping

**Metadata definition**

| TargetColumn | SourceColumn | Expression         |
|---------------|---------------|--------------------|
| customer_id   | customer_id   | direct             |
| customer_name | name          | UPPER(name)        |

**Rendered SQL**

```sql
SELECT
  c.customer_id AS customer_id,
  UPPER(c.name) AS customer_name
FROM src_customer AS c;
```

✅ Each target column uses an alias with its *final name*.

---

## 💡 Why We Always Use Aliases

| Benefit              | Description                                                   |
|----------------------|---------------------------------------------------------------|
| **Lineage clarity**  | Explicit mapping of source → target column                    |
| **SQL portability**  | Required or recommended when expressions are used            |
| **Predictable schema** | Column order & naming always controlled by metadata        |
| **Governance ready** | Easy traceability in documentation and profiling views       |

---

## 📜 Alias Style Guidelines

| Context        | Convention                          | Example                          |
|----------------|--------------------------------------|----------------------------------|
| Column names   | Always lowercase, `snake_case`      | `AS customer_name`              |
| Expressions    | Always explicit alias               | `COALESCE(a,b) AS full_name`    |
| Table aliases  | Short, stable aliases per source    | `FROM customer AS c`            |
| Join references| Always qualify with table alias     | `c.customer_id = o.customer_id` |

---

## 🧪 Dialect Behavior

The SQL renderer ensures that each dialect implementation (`SqlDialect`) handles
identifier quoting and escaping according to its platform rules.

| Dialect     | Example alias                      |
|-------------|------------------------------------|
| **DuckDB**  | `AS customer_name`                 |
| **Snowflake** | `AS "CUSTOMER_NAME"`            |
| **BigQuery** | `AS customer_name`               |
| **MSSQL**   | `AS [customer_name]`              |

---

## 🧩 Template Naming Conventions (HTMX / Partials)

> Defines the naming standard for all Django/HTMX partial templates used in *elevata*  
> to ensure consistency, reusability, and clarity across UI components.

### 📘 Purpose

In *elevata*, most interactive frontend components (inline edits, previews, imports, etc.)  
are rendered through Django templates. To keep them organized and predictable,  
all partials follow a strict naming and folder convention.

---

### 📐 Naming Pattern

| Pattern | Example | Description |
|----------|----------|-------------|
| `_context_purpose.html` | `_targetcolumn_inline_cell.html` | Inline editing cell for a TargetColumn |
| `_context_inline_preview.html` | `_targetcolumn_inline_preview.html` | Rename preview (HTMX dry-run) |
| `_import_result.html` | `_import_result.html` | Generic import result partial |
| `_import_result_error.html` | `_import_result_error.html` | Import error feedback partial |

---

### 🧱 Rules

1. **Prefix with `_`**  
   Indicates that the file is a *partial* (never rendered as a standalone view).  

2. **Use clear context prefix**  
   e.g. `targetcolumn`, `targetdataset`, `import`, `lineage`.  

3. **Describe purpose in suffix**  
   e.g. `cell`, `preview`, `result`, `error`, `form`, `confirm`.  

4. **HTMX partials include `_inline_`**  
   This distinguishes dynamic components from static includes.  

5. **Consistent folder layout**  
   templates/  
   metadata/  
   partials/  
   _targetcolumn_inline_cell.html  
   _targetcolumn_inline_preview.html  
   _import_result.html  
   _import_result_error.html  

6. **View usage**  
- Full views render page templates (e.g. `list.html`, `detail.html`).  
- HTMX endpoints and AJAX handlers always return partials.  

---

🧩 *Following this convention ensures all frontend templates remain consistent, discoverable, and safely reusable across modules.*

---

## 🔗 Related Documents

- [Automatic Target Generation Logic](generation_logic.md)
- [Target Backends](target_backends.md)

---

© 2025 elevata Labs — Internal Technical Documentation
