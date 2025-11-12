# Lineage Model & Logical Plan

## Overview

This document describes how Elevata models data lineage across datasets and columns
and how that lineage is used to construct a logical plan for SQL preview generation.

The lineage model ensures that every generated target dataset and column
has a traceable origin — either a source dataset or an upstream target dataset.

---

## 1. Dataset-Level Lineage

Each target dataset maintains a set of lineage inputs stored in
`TargetDatasetInput` objects. These describe the relationship between
a target dataset and its data providers.

| Field | Description |
|--------|-------------|
| `source_dataset` | The original source dataset, if this target is loaded directly from source. |
| `upstream_target_dataset` | The upstream target dataset (e.g., from Raw → Stage or Stage → Rawcore). |
| `role` | Defines whether the input is `primary`, `enrichment`, `reference_lookup`, or `audit_only`. |

Depending on the schema layer:
- **Raw** targets link to *source datasets* directly.
- **Stage** targets link to *Raw* datasets if available, otherwise to sources.
- **Rawcore** targets always link to *Stage* datasets.

---

## 2. Column-Level Lineage

Column-level lineage is maintained through `TargetColumnInput` objects,
which define how each generated column in a target dataset derives from
a source column or an upstream target column.

This lineage information is also consumed by the SQL Preview engine,
allowing it to render real derivation queries instead of generic `SELECT *` stubs.

---

## 3. Logical Plan Construction

The logical plan layer translates dataset lineage into a structured representation
of the final SQL query.

Steps:

1. **Resolve inputs**  
   Identify all source or upstream datasets using `TargetDatasetInput`.

2. **Map columns**  
   Reconstruct derived column mappings from `TargetColumnInput`.

3. **Assemble select plan**  
   Build the logical SELECT statement using `builder.build_logical_select()`.

4. **Render SQL**  
   The `renderer` module turns the logical plan into formatted SQL text
   with indentation, naming, and aliasing conventions applied.

---

## 4. Combination Modes

Each dataset defines a `combination_mode` attribute to indicate
how multiple sources are joined together:

| Mode | Description |
|------|--------------|
| `single` | The dataset has exactly one source. |
| `union` | The dataset combines multiple homogeneous sources via `UNION ALL`. |

This flag informs both the SQL preview and future optimization features
(e.g., deduplication or incremental merge strategies).

---

## 5. Future Outlook

Upcoming versions may extend the lineage model with:
- Derived column transformations (expression templates)
- Historical change tracking (temporal lineage)
- Lineage visualization in the Elevata UI

---

> Last updated: November 2025

---

© 2025 elevata Labs — Internal Technical Documentation