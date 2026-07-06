# ⚙️ Reference Integrity

Reference Integrity makes modeled TargetDataset references inspectable and controllable.

It has two deliberately separate parts:

- **Reference Integrity Review** checks loaded target data for missing parent examples.  
- **Controlled Reference Members** can create default and inferred members during controlled load execution.  
- **Default Member Fallback** can map still-unresolved child reference keys to the referenced default member when explicitly enabled.

The review is diagnostic and read-only. Controlled member creation and fallback handling are part of execution and require explicit metadata intent.

---

## 🔧 1. Purpose

elevata models relationships between TargetDatasets as architecture metadata.

Reference Integrity uses this metadata to answer two related questions:

```text
Do loaded child rows contain reference key values
that have no matching parent row?
```

and, when explicitly enabled:

```text
Can the runtime create controlled artificial parent members
or map unresolved child references to a controlled fallback
so downstream relationships remain loadable and explainable?
```

This keeps relationship quality visible while preserving a clear boundary between review and execution.

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

Reference Integrity Review is diagnostic. It never creates default members, inferred members, approvals, execution records, or metadata changes.

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

## 🔧 4. Controlled Reference Members

Controlled Reference Members are execution-time artificial parent rows for modeled rawcore references.

They are separate from Reference Integrity Review:

```text
Reference Integrity Review
  = read-only diagnosis

Controlled Reference Members
  = deterministic load-time member handling
```

Controlled member creation happens only during load execution. It is not triggered by opening Catalog Detail, running the Reference Integrity Review, creating approvals, or checking approvals.

The runtime supports two artificial member types:

| Member type | Marker behavior | Purpose |
|---|---|---|
| Default Member | `default_member = true`, `inferred_member = false` | Stable fallback member for a reference table |
| Inferred Member | `inferred_member = true`, `default_member = false` | Missing parent member derived from a loaded child reference |

Both markers are system-managed columns. Normal source-backed rows are loaded with both markers set to false.

---

## 🔧 5. Default Members

A default member is a deterministic artificial row in a rawcore reference dataset.

It provides a stable fallback member that can be recognized by consumers and frontends without guessing.

Default member behavior:

- created during load execution for controlled rawcore reference datasets  
- idempotent across repeated executions  
- marked with `default_member = true`  
- marked with `inferred_member = false`  
- uses deterministic artificial values for required columns  
- uses readable string values such as `(Default)` where a non-null string value is required

Default members are not created by Reference Integrity Review.

### 🧩 5.1 Default Member Fallback

Default Member Fallback is an optional execution-time mapping for modeled TargetDatasetReferences.

It maps still-unresolved child reference keys to the referenced dataset's default member when `default_member_fallback_enabled` is enabled. This keeps modeled relationships joinable even when the child row has no usable parent reference or when a complete reference remains unresolved and inferred members are not enabled.

Fallback behavior:

- requires `default_member_fallback_enabled` on the modeled TargetDatasetReference  
- can be enabled independently from inferred members  
- is automatically enabled as a modeling default when `inferred_members_enabled` is switched on  
- can be disabled explicitly afterwards  
- runs after inferred-member creation  
- never creates parent rows  
- maps only child reference keys that still have no matching parent row  
- leaves real parent matches and newly created inferred parent matches unchanged

The effective resolution priority is:

```text
real parent > inferred parent > default member > unresolved
```

When inferred members or Default Member Fallback are enabled, the referenced parent dataset is treated as a runtime execution dependency. This schedules the parent before the child during dependency-aware execution without changing semantic lineage.

For composite references, null, empty or blank values in any modeled key component make the child reference incomplete for fallback purposes. However, if the calculated child reference key still matches an existing parent row, that parent row wins and the fallback is not applied. Business-invalid but technically complete values are not interpreted by Controlled Reference Members; they remain candidates for inferred members when enabled or for separate quality checks.

---

## 🔧 6. Inferred Members

An inferred member is an artificial parent row created from a missing child reference.

Inferred member creation is **child-load-driven**:

```text
Load child dataset
  ↓
Find child reference values without parent rows
  ↓
Insert missing parent members into the referenced rawcore dataset
```

Loading the parent dataset does not create inferred members. A parent full load can later replace an inferred member with the real source-backed parent row.

Inferred member behavior:

- requires `inferred_members_enabled` on the modeled TargetDatasetReference  
- runs during the referencing child dataset load  
- uses the modeled reference components to map child key values to parent business key values  
- derives the parent surrogate key from the already calculated child foreign key  
- inserts only missing parent keys  
- is idempotent across repeated executions  
- marks inserted rows with `inferred_member = true`  
- marks inserted rows with `default_member = false`  
- uses readable string values such as `(Inferred)` where a non-null string value is required

This makes incomplete upstream master data loadable while keeping the artificial nature of the row explicit.

Inferred members handle complete child reference values whose parent row is missing. They do not represent null, empty or incomplete child references. Those cases are handled by Default Member Fallback when it is enabled.

---

## 🔧 7. Parent Data Remains Authoritative

Inferred members are temporary architecture-runtime safeguards, not final master data.

If the referenced parent dataset is later loaded from its real source and the parent row exists, the normal parent load can replace the artificial row with source-backed values.

Typical lifecycle:

```text
1. Child references parent key 69.
2. Parent row 69 is missing.
3. Child load creates parent row 69 as inferred.
4. Later parent full load reads row 69 from the source.
5. Parent row 69 becomes source-backed again.
```

The marker columns make this lifecycle observable:

```text
inferred_member = true   -> artificial inferred row
inferred_member = false  -> normal source-backed row
```

---

## 🔧 8. Null, Blank and Incomplete Reference Handling

Reference Integrity Review only checks complete child key combinations.

Rows with null child key components are not reported as missing parent examples by this review.

For Controlled Reference Members, elevata does not insert null values into modeled not-null columns. Required artificial member attributes receive deterministic typed sentinel values.

Default Member Fallback can resolve null, empty, blank or incomplete child references by mapping the child reference key to the parent default member when `default_member_fallback_enabled` is enabled and no matching parent row exists.

Examples:

| Column kind | Default member value | Inferred member value |
|---|---|---|
| Required string | `(Default)` | `(Inferred)` |
| Required number | `-1` | `-1` |
| Required boolean | `false` | `false` |
| Required date/timestamp | deterministic early date/time | deterministic early date/time |

The marker columns remain the authoritative indicator of artificial member semantics.

---

## 🔧 9. Runtime and Dialect Boundary

Reference Integrity follows elevata's SQL rendering architecture.

The review and controlled member services provide semantic ingredients only:

- child schema and table  
- parent schema and table  
- child-to-parent key pairs  
- example limit or insert semantics  
- marker-column intent

The active dialect owns the final SQL shape, including identifier quoting, table rendering, hashing, literals, row limiting and backend-specific DML syntax.

This keeps Reference Integrity aligned with the same dialect boundary used by load SQL, previews and execution logic.

---

## 🔧 10. Governance Boundary

Reference Integrity Review is a read-only Catalog capability.

It does not:

- edit metadata  
- create or change TargetDatasetReferences  
- insert parent rows  
- create default members  
- create inferred members  
- execute loads  
- create approvals  
- check approvals  
- persist review history  
- add database models or migrations  
- introduce AI-based inference

Controlled Reference Members are an execution capability.

They:

- require explicit modeled metadata intent  
- run only during load execution  
- remain deterministic and dialect-owned  
- mark artificial rows explicitly  
- do not replace Architecture Control approval or execution guardrails  
- do not introduce AI-based inference

The review makes modeled relationship issues visible. Controlled execution handles enabled artificial members and fallback mapping when the architecture explicitly allows it.

---

## 🔧 11. Catalog Integration

Reference Integrity appears in Catalog Detail only when the selected dataset has modeled outgoing references.

This keeps datasets without reference checks visually clean and avoids presenting a runtime review where no modeled relationship exists.

When outgoing references exist, the panel explains that the review is on demand and that returned missing parent examples are proven findings.

---

© 2025-2026 elevata - Technical Documentation
