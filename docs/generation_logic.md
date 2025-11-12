# ⚙️ Automatic Target Generation Logic

> How elevata transforms imported source metadata  
> into consistent, governed target datasets — automatically.

---

## 🧩 1. Overview

The automatic target generation in elevata translates imported metadata  
from *source systems* (SAP, Navision, APIs, etc.) into well-structured target datasets  
for each logical layer (Raw, Stage, Rawcore, Bizcore, …).

This process is **deterministic, auditable, and rule-driven**,  
using configurable mappings, key rules, and governance defaults.

All logic is implemented in the `TargetGenerationService`  
and associated helper modules in `metadata/generation`.

---

## 📜 2. Core Principles

- **Deterministic Transformation**  
  Every dataset and column name is generated predictably, based on naming rules.

- **Layer Awareness**  
  The applied naming, surrogate key logic, and materialization depend on the target schema.

- **Separation of Business vs. Physical Keys**  
  Natural keys (business-defined) are preserved; surrogate keys (system-defined) are added.

- **System-Managed Governance**  
  Automatically generated datasets and columns are flagged as system-managed,  
  protecting them from manual alteration.

---

## ♻️ 3. The Generation Flow

The central entry point is:

```python
from metadata.generation.target_generation_service import TargetGenerationService

svc = TargetGenerationService()
svc.apply_all(eligible_source_datasets, target_schema)
```

The process performs:  
1. Dataset draft creation → `build_dataset_bundle()`  
2. Surrogate key injection (if applicable)  
3. Column mapping and business key preservation  
4. Persistence as TargetDataset + TargetColumn  

Each dataset is generated per target layer, with schema-specific rules
defined in `TargetSchema` (e.g., surrogate key requirement, physical prefix, null token, separators).
 
---

## 🧩 4. Key Building Blocks

### 🔹 `build_dataset_bundle(source_dataset, target_schema)`

Creates a temporary dataset/column draft before database persistence.

Steps:

1. Build the physical name `(build_physical_dataset_name)`
2. Collect source columns `(filtered by integrate=True)`
3. Identify business key columns
4. Add surrogate key column first (if schema requires one)
5. Append natural key columns
6. Append remaining integrated columns

Output:

```python
{
  "dataset": TargetDatasetDraft(...),
  "columns": [TargetColumnDraft(...), ...]
}
```
---

### 🔹 Surrogate Key Logic

If `target_schema.surrogate_keys_enabled` is True,
a deterministic hash-based surrogate key column is created.

It uses:

- All **business key columns**, sorted alphabetically
- Configured separators from `TargetSchema`
- A **pepper** (loaded at runtime from environment variables)

Example surrogate expression:
```less
hash256(concat(
  'order_no', '~', order_no, '|', '<pepper>'
))
```

The resulting column:

```python
TargetColumnDraft(
  target_column_name="customer_order_key",
  datatype="string",
  max_length=64,
  surrogate_key_column=True,
  lineage_origin="surrogate_key",
  is_system_managed=True
)
```
---
### 🔹 Pepper Handling

The pepper is dynamically resolved at runtime,  
using the secure loader from `metadata.utils.security`:

```python
from metadata.generation.security import get_runtime_pepper
pepper = get_runtime_pepper()
```

This ensures:  
- No pepper values are persisted in metadata  
- Deterministic but non-reversible hash values  
- Separate peppers per environment (Dev/Test/Prod)

---

## 🧭 5. Layer-Aware Behavior

| Layer | Prefix Example | Key Behavior | Description |
|-------|----------------|--------------|--------------|
| **Raw** | `raw_sap_customer` | No surrogate key | Direct 1:1 import of source dataset |
| **Stage** | `stg_sap_customer` | No surrogate key | Normalized, unified staging layer |
| **Rawcore** | `rc_sap_customer` | Deterministic surrogate key | System-managed consolidation layer |
| **Bizcore** | `bz_customer` | Logical business entity key | Business-defined structure (manual) |

Each generated dataset stores both source and upstream target inputs, depending on the schema layer.  
- Raw datasets link directly to source datasets.  
- Stage datasets link to raw (if available) or directly to sources.  
- Rawcore datasets always link to upstream stage datasets.  

The behavior per layer is defined via `TargetSchema`:

```python
TargetSchema(
  short_name="rawcore",
  ...
  physical_prefix="rc",
  surrogate_keys_enabled=True,
  surrogate_key_null_token="null_replaced",
  surrogate_key_pair_separator="~",
  surrogate_key_component_separator="|",
  ...
)
```

## 🧩 6. Business vs. Surrogate Keys

| Property | Meaning | Editable |
|-----------|----------|-----------|
| `business_key_column` | Logical key column, defined by user | ✅ |
| `surrogate_key_column` | System-generated hash key | ❌ (system-managed) |

In the generated metadata:  
- Business keys remain visible and editable in the UI  
- Surrogate keys are locked (`is_system_managed=True`)

---

## ↔️ 7. Mapping Behavior

Each source column mapped via `map_source_column_to_target_column()`  
inherits its logical datatype, constraints, and description, but may be adjusted by:  
- Naming rules in `naming.py`  
- Integration flag (`integrate=True`)  
- Natural key membership (`primary_key_column=True`)

This column-level lineage is also consumed by the SQL Preview engine,  
allowing it to render true derivation queries across layers (e.g., Stage → Rawcore).

---

## 🔬 8. Filtering Logic

Target generation only considers **integrated source columns**:

```python
src_cols_qs = source_dataset.source_columns.filter(integrate=True)
```

Likewise, **datasets** are only included if:
- `integrate=True`, **and**
- `generate_raw_table=True` (directly or inherited)

---

## 🚀 9. Example Result

Example generated dataset for schema = `rc`:

```sql
rc_sap_customer
├── rc_sap_customer_key (hash256(...))
├── customer_no
├── client_code
├── customer_name
├── city_name
└── country_code
```

Each target column:  
- Preserves semantic meaning  
- Has defined lineage origin  
- Is system-managed where applicable  

---

## 🚧 10. Future Extensions (v0.4+)

The generated **logical plan** will be rendered into SQL  
through the new Rendering Layer:

```python
logical = build_logical_select_for_target(target_dataset)
sql = render_sql(logical, dialect=DuckDBDialect())
```

Planned capabilities:

- Vendor-specific dialect adapters
- Inline expressions and transformations
- Metadata-driven view materialization

---

© 2025 elevata Labs — Internal Technical Documentation