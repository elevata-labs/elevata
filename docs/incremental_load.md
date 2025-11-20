# ⚡ Incremental Load Architecture

> How elevata performs metadata-driven incremental processing — merge-based upserts, delete detection,
> and lineage-driven keys across the Stage → Rawcore pipeline.

---

## 🧩 Overview
The incremental loading framework in elevata provides a **metadata-driven, deterministic** way to keep Rawcore
datasets up to date. It relies entirely on the metadata model — especially lineage — rather than hardcoded mapping rules.

Incremental logic is configured per `TargetDataset` using:  
- `incremental_strategy = "full" | "merge"`  
- `handle_deletes = True | False`  

Currently implemented strategies:  
- **full** → full rebuild  
- **merge** → incremental upsert based on natural key lineage  

Additional strategies such as `append` and `snapshot` are *planned but not implemented yet* and therefore not documented here.

Incremental pipelines always operate **between Stage and Rawcore**, never directly from source systems.

---

## 🧠 Core Concepts

### Metadata-driven behavior
Incremental behavior is determined solely by metadata. No external configuration or custom SQL is needed.

### Lineage as the authoritative contract
Lineage defines:  
- which columns form the natural key  
- how Stage maps to Rawcore  
- which columns participate in merge  
- which expressions are used upstream  

This eliminates the need for a separate incremental field map.

### Stable surrogate keys
Rawcore surrogate keys are deterministic hash keys derived from the natural key and environment-specific pepper.
They are **never** used for merging.

---

## 🔄 Incremental Strategies

### Full Load
A full load recreates or truncates the Rawcore table and inserts *all* upstream rows.
Used when:  
- initial load  
- upstream structure changed heavily  
- incremental strategy is intentionally disabled  

### Merge Load (Incremental Upsert)
A merge load performs:  
1. **INSERT** new records  
2. **UPDATE** existing records when upstream attributes changed  
3. optional **DELETE detection** for records that disappeared upstream  

Natural key lineage defines the merge join condition.

Merge is only valid when the effective materialization of Rawcore is a **table**.
The Metadata Health Check prevents invalid configurations.

---

## 🗑️ Delete Detection
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

## 🧬 Lineage-Driven Mapping
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

## 🖋️ SQL Rendering & Dialect Abstraction
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

## 🚀 Future Enhancements
Future work will extend incremental loading capabilities:  
- multi-source incrementals (multiple Stage inputs)  
- alternative merge policies (e.g. soft-delete, SCD-lite)  
- batch-optimized delete detection  
- incremental snapshots  
- dialect-specific performance tuning  

These features will be documented when implemented.

---

© 2025 elevata Labs — Internal Technical Documentation

