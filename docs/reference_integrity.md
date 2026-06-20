# ⚙️ Reference Integrity

Reference Integrity makes modeled TargetDataset references inspectable against loaded target data.

It helps users understand whether child datasets contain key values that do not exist in the referenced parent dataset.

The feature is deterministic, read-only, and started explicitly by the user.

---

## 🔧 1. Purpose

elevata models relationships between TargetDatasets as architecture metadata.

Reference Integrity uses this metadata to make relationship quality visible without turning the Catalog into an execution workflow.

It answers a focused question:

```text
Do the loaded child rows contain reference key values
that have no matching parent row?
```

This helps users find broken modeled relationships after data has been loaded.

---

## 🔧 2. Reference Integrity Review

Reference Integrity Review is available from the Architecture Catalog detail page for datasets with modeled outgoing references.

The review is started on demand. Opening a Catalog detail page does not query target data automatically.

The review checks modeled outgoing references from the selected TargetDataset against the current loaded target data.

For each checkable reference, elevata:

- reads the modeled child-to-parent key mapping  
- renders dialect-owned SQL for the active target backend  
- checks for child key combinations without a matching parent key combination  
- returns bounded missing parent examples  
- reports whether the reference is complete, needs attention, is not applicable, or could not be checked

A dataset without outgoing references has no Reference Integrity Review panel in Catalog Detail.

---

## 🔧 3. Missing Parent Examples

Returned rows are **missing parent examples**.

They are not random samples and not statistical estimates.

Each returned example is a proven finding:

```text
A child key value exists in the selected dataset,
but no matching parent key value exists in the referenced dataset.
```

The review intentionally returns a bounded number of examples. It does not calculate or persist a full violation inventory by default.

This keeps the review compact and safe for interactive Catalog usage while still proving that reference integrity needs attention.

---

## 🔧 4. Null Handling

Reference Integrity Review only checks complete child key combinations.

Rows with null child key components are not reported as missing parent examples by this review.

Nullability remains part of the column contract and metadata health context. Reference Integrity focuses on non-null child keys that claim to reference a parent row.

---

## 🔧 5. Runtime and Dialect Boundary

Reference Integrity Review follows elevata's SQL rendering architecture.

The review service provides semantic ingredients only:

- child schema and table  
- parent schema and table  
- child-to-parent key pairs  
- example limit

The active dialect owns the final SQL shape, including identifier quoting, table rendering and backend-specific row limiting syntax.

This keeps Reference Integrity aligned with the same dialect boundary used by load SQL, previews and execution logic.

---

## 🔧 6. Governance Boundary

Reference Integrity Review is a read-only Catalog capability.

It does not:

- edit metadata  
- create or change TargetDatasetReferences  
- insert parent rows  
- create inferred members  
- execute loads  
- create approvals  
- check approvals  
- persist review history  
- add database models or migrations  
- introduce AI-based inference

The review makes modeled relationship issues visible. It does not repair them automatically.

---

## 🔧 7. Catalog Integration

Reference Integrity appears in Catalog Detail only when the selected dataset has modeled outgoing references.

This keeps datasets without reference checks visually clean and avoids presenting a runtime review where no modeled relationship exists.

When outgoing references exist, the panel explains that the review is on demand and that returned missing parent examples are proven findings.

---

© 2025-2026 elevata - Technical Documentation
