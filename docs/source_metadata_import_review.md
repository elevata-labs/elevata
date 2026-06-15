# ⚙️ Source Metadata Import Review

Source Metadata Import Review makes source onboarding inspectable.

Instead of only confirming that metadata import ran successfully, elevata explains what was discovered, what changed, what stayed unchanged, what was removed, and where manual attention is needed.

---

## 🔧 1. Purpose

Metadata import is often the first trust boundary in a data platform.

Users need to know whether a source import actually changed metadata or whether the existing SourceColumn definitions were simply checked and confirmed.

Source Metadata Import Review answers this question directly:

```text
What did elevata discover, create, change, keep unchanged, remove, infer, skip, or leave unresolved?
```

The report is shown immediately after importing metadata for a SourceDataset or for all importable datasets of a SourceSystem.

---

## 🔧 2. Import Summary

The compact summary shows the overall import impact:

- datasets processed  
- columns imported  
- created columns  
- changed columns  
- unchanged columns  
- removed columns  
- detected primary key columns  
- datasets or decisions needing review

This separates processing volume from actual metadata impact.

---

## 🔧 3. Column Outcomes

### 🧩 3.1 Created

`Created` means the column did not exist in elevata metadata before the import and was newly added as a SourceColumn.

### 🧩 3.2 Changed

`Changed` means the SourceColumn already existed, but one or more technical metadata attributes changed compared to the previous state.

Relevant attributes include:

- ordinal position  
- raw source datatype  
- normalized datatype  
- max length  
- decimal precision and scale  
- nullable flag  
- primary key flag  
- referenced source dataset name  
- JSON path

### 🧩 3.3 Unchanged

`Unchanged` means the SourceColumn existed before and still matches the imported source metadata.

This is important because an unchanged result confirms that the source was checked without creating downstream uncertainty.

### 🧩 3.4 Removed

`Removed` means an existing SourceColumn no longer appears in the imported source shape and was removed by the existing import cleanup behavior.

---

## 🔧 4. Dataset Review Signals

### 🧩 4.1 Primary Key Detection

If primary key columns are detected, the review shows them explicitly.

When auto-integrate PK is enabled, detected primary key columns are marked for integration by the existing import behavior.

### 🧩 4.2 Needs Review

A dataset needs review when import could not complete cleanly or when a relevant metadata decision remains unresolved.

Examples include:

- skipped dataset  
- missing source object  
- empty sample  
- no primary key candidate detected where manual key modeling may be required

---

## 🔧 5. Design Boundaries

Source Metadata Import Review is intentionally small and deterministic.

It does not:

- introduce AI-based inference  
- create a new import wizard  
- persist import history  
- add database models or migrations  
- generate target architecture  
- execute loads  
- change existing import semantics

The existing SQLAlchemy, file and REST import paths remain authoritative for actual SourceColumn mutations.

The review report only makes the import outcome transparent.

---

## 🔧 6. User Guidance

A typical review flow is:

1. Run metadata import for a SourceDataset or SourceSystem.  
2. Check the summary badges.  
3. Inspect changed or removed columns first.  
4. Confirm primary key detection.  
5. Expand dataset details only when more context is needed.  
6. Continue with generation or modeling once the source metadata impact is understood.

The most reassuring import result is often not a large changed count, but a clear unchanged count.

```text
Changed: 0
Unchanged: 77
```

This means elevata checked the source metadata and found no structural difference from the previous state.

---

© 2025-2026 elevata Labs — Internal Technical Documentation
