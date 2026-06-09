# ⚙️ Metadata Naming Guidance

> Deterministic, project-specific naming assistance for TargetColumn modeling based on existing metadata mappings, not AI or global naming dictionaries.

---

## 🔧 1. Purpose

Metadata Naming Guidance helps modelers reuse already established column naming decisions while editing TargetColumns.

It is designed for practical modeling situations where technical source-system fields need to be translated into consistent platform names.  
For example, SAP-style source columns such as `MANDT`, `VBELN`, `KUNNR` or `ProductModelID` may need to become project-specific names such as `client_no`, `sales_document_no`, `customer_no` or `product_model_id`.

The goal is not to define one universal naming standard.  
The goal is to make already used project-specific naming decisions visible at the moment where a modeler needs them.

---

## 🔧 2. What it does

Naming Guidance analyzes existing metadata mappings and shows known target names for the current input column.

When a modeler edits a TargetColumn name, elevata can show:

```text
Naming guidance  ProductModelID

Recommended: product_model_id

Known target names
product_model_id  used 1 time
  rawcore.rc_aw_product_model
```

If the recommendation is clear, the modeler can apply it directly from the inline editor.

The existing TargetColumn rename flow remains authoritative. Applying a recommendation still uses the standard rename validation, collision checks, audit handling and former-name tracking.

---

## 🔧 3. How guidance is derived

Naming Guidance is deterministic and read-only while building suggestions.

It uses existing `TargetColumnInput` metadata.

### 🧩 3.1 Direct source inputs

For direct source inputs, guidance uses:

```text
SourceColumn.source_column_name
```

### 🧩 3.2 Upstream target inputs

For upstream target inputs, guidance uses:

```text
upstream TargetColumn.target_column_name
```

The lookup intentionally follows only one column-lineage edge.

This keeps guidance aligned with the current modeling step:

```text
Source system column: AccountNumber
rawcore column:       account_no
bizcore input:        account_no
```

When editing a bizcore column based on `account_no`, the relevant input name is `account_no`, not the original `AccountNumber`.

---

## 🔧 4. Guidance states

### 🧩 4.1 new

No previous target name is known for the current input column.

The UI keeps this state quiet to avoid unnecessary noise.

### 🧩 4.2 suggested

There is either one known target name or a clearly dominant target name.

The recommendation is shown in the TargetColumn inline editor and can be applied directly.

### 🧩 4.3 conflict

There are multiple known target names without a clear dominant naming pattern.

In this case, elevata shows the known variants and examples, but does not choose a recommendation.

---

## 🔧 5. Scope and boundaries

Naming Guidance focuses on technical naming decisions in modeling layers:

- rawcore  
- bizcore

The following are intentionally excluded from recommendation evidence:

- serving-layer friendly names  
- historized rawcore datasets  
- ambiguous mixed-input evidence where one previous target column cannot be clearly attributed to one input name  
- system-managed technical columns

For multi-input TargetColumns, guidance is shown separately per immediate input column when clear previous usage exists.

This keeps the guidance focused on reusable technical naming conventions rather than presentation-layer labels, generated structures or ambiguous derivation evidence.

---

## 🔧 6. What it does not do

Naming Guidance does not:

- enforce names  
- block saves  
- mutate metadata while building guidance  
- introduce AI-based inference  
- use a global dictionary  
- require a glossary workflow  
- add database tables or migrations

It is a transparent advisory layer derived from the metadata that already exists.

---

## 🔧 7. Why this matters

Consistent naming is difficult when source systems use cryptic technical identifiers.

The challenge is not only choosing a good name once.  
The challenge is remembering how the same field was named elsewhere.

Naming Guidance reduces that manual lookup effort without replacing modeler judgment.

It makes project-specific naming conventions visible where they are needed most: directly in the TargetColumn modeling workflow.

---

© 2025-2026 elevata - Technical Documentation