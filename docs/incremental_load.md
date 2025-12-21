# ⚙️ Incremental Load Architecture

> How elevata performs metadata-driven incremental processing — merge-based upserts, delete detection,
> and lineage-driven keys across the Stage → Rawcore pipeline.

---

## 🔧 1. Overview
The incremental loading framework in elevata provides a **metadata-driven, deterministic** way to keep Rawcore
datasets up to date. It relies entirely on the metadata model — especially lineage — rather than hardcoded mapping rules.

Incremental logic is configured per `TargetDataset` using:  
- `incremental_strategy = "full" | "merge"`  
- `handle_deletes = True | False`  

Currently implemented strategies:  
- **full** → full rebuild  
- **merge** → incremental upsert based on natural key lineage  

---

## 🔧 2. Source-side incremental scoping (Ingestion)

While incremental strategies operate between Stage → Rawcore, elevata also supports  
**incremental scoping during source ingestion**.

This is controlled at the SourceDataset level via:  

- `static_filter` – permanent scoping, applied only during ingestion  
- `increment_filter` – time-based delta scoping using `{{DELTA_CUTOFF}}`  

Key rules:  
- `static_filter` is applied only during ingestion (RAW or stage-direct-source)  
- `increment_filter` is applied during ingestion and delete detection  
- Incremental scoping during ingestion does **not** imply incremental RAW storage;  
  RAW tables are always rebuilt (TRUNCATE + INSERT)  

This ensures consistency between:  
- extracted source data  
- incremental merge logic  
- delete detection scope  

---

## 🔧 3. Core Concepts

### 🧩 Metadata-driven behavior
Incremental behavior is determined solely by metadata. No external configuration or custom SQL is needed.

### 🧩 Lineage as the authoritative contract
Lineage defines:  
- which columns form the natural key  
- how Stage maps to Rawcore  
- which columns participate in merge  
- which expressions are used upstream  

This eliminates the need for a separate incremental field map.

### 🧩 Stable surrogate keys
Rawcore surrogate keys are deterministic hash keys derived from the natural key and environment-specific pepper.
They are **never** used for merging.

---

## 🔧 4. Incremental Strategies

### 🧩 Full Load
A full load recreates or truncates the Rawcore table and inserts *all* upstream rows.
Used when:  
- initial load  
- upstream structure changed heavily  
- incremental strategy is intentionally disabled  

### 🧩 Merge Load (Incremental Upsert)
A merge load performs:  
1. **INSERT** new records  
2. **UPDATE** existing records when upstream attributes changed  
3. optional **DELETE detection** for records that disappeared upstream  

Natural key lineage defines the merge join condition.

Merge is only valid when the effective materialization of Rawcore is a **table**.
The Metadata Health Check prevents invalid configurations.

---

## 🔧 5. Delete Detection
If `handle_deletes=True`, elevata generates a dialect-aware anti-join delete.

Example pattern:
```sql
DELETE FROM rc_table rc
WHERE NOT EXISTS (
  SELECT 1
  FROM stg_table s
  WHERE <natural key match>
);
```

Key characteristics:  
- derived entirely from natural key lineage  
- removes rows no longer present in *any* Stage input  
- executed after merge  
- implemented for all dialects via the `SqlDialect` abstraction  

---

## 🔧 6. Lineage-Driven Mapping
Lineage determines all mappings:  
- natural key → merge condition  
- business keys → stable grain  
- additional attributes → column-level lineage expressions  

This ensures:  
- no manual mapping maintenance  
- automatic propagation of renames, datatypes, and transformations  
- SQL preview shows the real executed logic  

Example effects:  
- If a source column is renamed, merge logic updates automatically.  
- If a Stage dataset adds an enrichment column, Rawcore will reflect it.  

---

## 🔧 7. SQL Rendering & Dialect Abstraction
All incremental SQL uses the active SQL dialect:
```python
dialect = get_active_dialect()
```

The dialect determines:  
- merge syntax (`MERGE INTO` vs. UPDATE+INSERT emulation)  
- identifier quoting  
- concat and hash functions  
- delete detection patterns  

DuckDB is the default fallback dialect to ensure consistent behavior when no active profile is set.

---

© 2025 elevata Labs — Internal Technical Documentation

