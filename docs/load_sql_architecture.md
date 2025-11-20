# 🏗️ Load SQL Architecture

> How elevata transforms metadata and lineage into executable SQL for Raw, Stage, and Rawcore —
> covering the entire pipeline from logical plan to dialect-aware SQL rendering.

This document complements the *Generation Logic*, *Incremental Load*, and *Dialect System* docs by describing
how Load SQL is generated, what components participate, and how merge/delete SQL is constructed in a modular, vendor-neutral way.

---

## 🧩 1. Overview
Load SQL in elevata is produced through a multi-layer architecture:  

1. **Metadata Model** → Defines datasets, columns, lineage, natural keys  
2. **Logical Plan Builder** → Converts lineage into a structured, dialect-agnostic plan  
3. **SQL Renderer** → Translates logical plan into SQL text  
4. **Dialect Adapter** → Applies vendor-specific syntax rules  
5. **Load SQL Functions** → Generate full-load, merge, and delete-detection SQL  

The result: deterministic, readable, platform-portable SQL.

---

## 🔗 2. Architecture Flow
The SQL pipeline follows a strict sequence:

```text
TargetDataset
   ↓ (lineage, keys, columns)
LogicalPlanBuilder
   ↓ (structured plan)
SqlRenderer
   ↓ (strings + dialect hooks)
Active SqlDialect
   ↓ (final SQL)
Load SQL (full / merge / delete)
```

This separation keeps metadata clean, SQL deterministic, and dialect differences isolated.

---

## 📐 3. Logical Plan Builder
The logical plan is a neutral representation of the desired query, independent of SQL syntax.  
It performs:  
- resolving upstream datasets  
- building select lists  
- applying lineage mappings  
- generating aliases  
- determining join or union behavior  

The plan does **not** contain SQL text — only structured objects.
This makes it reusable across dialects.

Example (conceptual):
```
LogicalSelect(
  from = StageDataset,
  columns = [
    LogicalColumn(name="customer_id", source="stg.customer_id"),
    LogicalColumn(name="city_name", source="stg.city"),
    ...
  ]
)
```

---

## 🖋️ 4. SQL Renderer
`SqlRenderer` turns the logical plan into SQL using systematic rendering rules:  
- two-space indentation  
- explicit aliasing for every target column  
- stable table aliases  
- deterministic column order  

The renderer delegates quoting, hashing, concatenation, and merge patterns to the active dialect.

This ensures:  
- consistent output across platforms  
- predictable diffs in UI and version control  

---

## 🎛️ 5. Dialect Integration
The renderer never references vendor-specific syntax directly.  
All variations (identifier quoting, merge syntax, hashing, boolean rules) are handled by:

```
dialect = get_active_dialect()
```

Examples:  
- DuckDB: `"identifier"`  
- Snowflake: `"IDENTIFIER"`  
- BigQuery: backtick quoting  
- MSSQL: `[identifier]`  

The dialect also provides:  
- `render_merge_sql(context)`  
- `render_delete_detection_sql(context)`  
- `hash_expression()`  
- `concat_expression()`  

---

## 🔄 6. Full Load SQL
Full-load SQL is the simplest mode:  
- truncate (if supported)  
- `INSERT INTO rawcore SELECT ... FROM stage`  

Logical steps:  
1. Build logical select for the target dataset  
2. Render SQL using dialect rules  
3. Wrap in `INSERT INTO` target table  

Full loads ignore incremental filters and delete detection.

---

## 🔄 7. Merge SQL
Merge SQL is more complex and fully dialect-aware.

Typical structure:  
- join Stage and Rawcore on natural keys  
- update changed attributes  
- insert new rows  

Since merge syntax differs heavily between platforms, the renderer produces a **merge context**:
```
MergeContext(
  target = RawcoreDataset,
  source = StageDataset,
  key_columns = [...],
  all_columns = [...]
)
```
and delegates final SQL construction to the active dialect.

DuckDB and MSSQL: native `MERGE INTO`  
Some platforms: emulated via `UPDATE` + `INSERT`.

---

## 🗑️ 8. Delete Detection SQL
Delete detection removes Rawcore rows missing in upstream Stage datasets.

Renderer builds a structured delete context:
```
DeleteContext(
  target = RawcoreDataset,
  source = StageDataset,
  key_columns = [...]
)
```

The dialect decides between:  
- `NOT EXISTS` anti-join  
- `EXCEPT`  
- dialect-specific anti-semi join  

---

## 🧬 9. Interaction with Lineage
Lineage drives all crucial parts of SQL generation:  
- natural keys → merge conditions  
- column inputs → select expressions  
- dataset-level lineage → FROM source and join structure  
- transformation lineage → column expressions  

This makes Load SQL fully metadata-driven and eliminates the need for manual ETL coding.

---

## 🚀 10. Future Enhancements
Planned extensions include:  
- multi-source merge support  
- staging unions with incremental alignment  
- materialization strategies (views vs. tables)  
- column-level expression templates  
- dialect capability validation / feature flags  

---

© 2025 elevata Labs — Internal Technical Documentation

