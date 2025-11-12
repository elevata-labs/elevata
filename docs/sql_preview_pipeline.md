# SQL Preview & Rendering Pipeline

## Overview

The SQL Preview feature of Elevata provides an interactive view
of how generated target datasets are derived from their upstream inputs.

It leverages the lineage model to construct accurate,
human-readable SQL queries that reflect the actual transformation logic
across Raw, Stage, and Rawcore layers.

---

## 1. End-to-End Flow

| Step | Component | Description |
|------|------------|-------------|
| 1 | `TargetGenerationService` | Builds or updates lineage for all target datasets and columns. |
| 2 | `LogicalPlanBuilder` | Resolves dataset and column dependencies. |
| 3 | `SqlRenderer` | Translates logical plans into SQL syntax with formatting rules. |
| 4 | `PreviewView` | Renders formatted SQL in the Elevata web interface. |

---

## 2. Layer-Specific Rendering Rules

| Layer | Render Logic |
|--------|---------------|
| **Raw** | Simple `SELECT` from the source dataset with column mappings. |
| **Stage** | `SELECT` from one or more raw datasets, joined or unioned depending on configuration. |
| **Rawcore** | Derived `SELECT` from stage datasets, including surrogate key generation and field harmonization. |

---

## 3. Auto-Union Behavior

If multiple upstream datasets are linked to a stage target,
the SQL Preview automatically aligns their column structures and renders:

```sql
SELECT
  ...
FROM raw.raw_aw1_person
UNION ALL
SELECT
  ...
FROM raw.raw_aw2_person
```
Missing columns are filled with NULL to ensure alignment across all inputs.

---

## 4. Formatting & Display

The SQL renderer applies consistent formatting rules:

- Two-space indentation
- Keywords (SELECT, FROM, UNION ALL) aligned vertically
- Columns formatted as alias."column_name" AS target_column
- Empty lines before major clauses for readability

Rendered SQL is displayed in a light green box (`alert alert-success` style) in the Elevata UI.

## 5. Future Enhancements

Planned features for the preview pipeline include:

- Inline display of column-level expressions
- Support for user-defined filters or transformations
- Integration with external data quality rules
- Multi-layer dependency graphs

## 6. Example Output

Example of a generated Rawcore SQL Preview:

```sql
SELECT
  hash256(concat_ws('|', concat('productmodelid', '~', coalesce({expr:productmodelid}, 'null_replaced')), 'supersecretpeppervalue')) AS rc_aw_product_model_key,
  s."productmodelid" AS product_model_id,
  s."name" AS product_model_name,
  s."catalogdescription" AS catalog_desc,
  s."instructions" AS instructions_txt
FROM
  "stage"."stg_aw_productmodel" AS s
```

---

> Last updated: November 2025

---

© 2025 elevata Labs — Internal Technical Documentation