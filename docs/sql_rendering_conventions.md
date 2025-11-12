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

## 🔗 Related Documents

- [Automatic Target Generation Logic](generation_logic.md)
- [Target Backends](target_backends.md)

---

© 2025 elevata Labs — Internal Technical Documentation
