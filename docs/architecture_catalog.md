# ⚙️ Architecture Catalog

The Architecture Catalog is the read-only discovery layer for metadata-defined
executable architecture.

It helps users understand:

- what datasets exist  
- why they exist  
- how they are defined  
- how they are connected  
- how they are controlled  
- where execution evidence is available  
- which architecture quality and governance signals need attention

The Catalog does not edit metadata and does not execute loads.

---

## 🔧 1. Purpose

elevata treats architecture as executable metadata.

The Architecture Catalog provides a structured way to discover that architecture  
without entering editing or execution workflows.

It complements Architecture Control:

```text
Architecture Catalog
  = What exists, why it exists, how it is defined, how it is connected,
    how it is controlled, and where its execution evidence is.

Architecture Control
  = Review, approve, preview, execute and inspect execution evidence.
```

This separation keeps discovery, control and execution responsibilities clear.

---

## 🔧 2. Catalog Workspace

The Architecture Catalog workspace provides a compact inventory of TargetDatasets.

It supports filtering by:

- dataset key and name  
- schema / layer  
- owner  
- active status  
- system-managed status  
- materialization type  
- incremental strategy  
- query logic

Each catalog row shows:

- dataset key  
- description  
- schema / layer  
- effective materialization type  
- incremental strategy  
- lifecycle status  
- management mode  
- ownership  
- upstream and downstream counts  
- metadata health  
- query logic  
- direct navigation links  
- entry point to Catalog Insights

The workspace links to:

- dataset details  
- lineage  
- query contract  
- Architecture Control  
- Catalog detail

---

## 🔧 3. Catalog Insights

Architecture Catalog Insights provide read-only architecture quality and governance
signals across TargetDatasets.

Insights summarize:

- datasets without assigned ownership  
- datasets with metadata health issues  
- datasets with metadata health warnings  
- datasets with custom query logic  
- datasets without downstream consumers  
- inactive datasets with downstream consumers  
- datasets without Architecture Execution Record evidence

Insight cards show compact dataset lists and can expand to reveal all matching datasets.

Each dataset entry links back to the Catalog detail view, where dataset-specific
insight signals are shown in context.

Catalog Insights do not create approvals, check approvals, execute loads, delete
execution records, or mutate metadata.

---

## 🔧 4. Catalog Detail View

The Catalog detail view summarizes one TargetDataset as an architecture object.

It displays:

- architecture summary  
- ownership  
- metadata health findings  
- latest Architecture Execution Record summary  
- upstream inputs  
- downstream consumers  
- column contract signals  
- dataset-specific Catalog insight signals  
- Architecture Control review status summary

The detail view remains read-only. Editing stays on the dataset detail and scoped  
metadata pages. Execution and approval workflows stay in Architecture Control.

---

## 🔧 5. Review Status

For TargetDataset scopes, the Catalog detail view surfaces the Architecture Control review status  
as a read-only summary.

The review status summary includes:

- review state  
- review message  
- report fingerprint reference  
- architecture change indicator  
- policy status  
- link to the selected Architecture Control scope

Catalog detail pages use the existing Architecture Control review status contract.  
They do not create approvals and do not run approval checks.

---

## 🔧 6. Execution Evidence

For TargetDataset scopes, the Catalog detail view surfaces the latest Architecture Execution Record summary when one exists.

The evidence summary includes:

- execution status  
- start timestamp  
- duration  
- dependency mode  
- compact record fingerprint  
- link to Architecture Control execution history

Architecture Execution Records remain stored, filtered, downloaded and governed
through Architecture Control.

The Catalog shows the latest evidence reference in dataset context without
duplicating the execution history workspace.

---

## 🔧 7. Lineage and Contract Signals

The Catalog shows direct upstream and downstream relationships.

Upstream inputs can be:

- source datasets  
- target datasets

Downstream consumers are TargetDatasets that directly consume the selected dataset.

Column contract signals show:

- ordinal position  
- target column name  
- datatype  
- nullability  
- system role  
- lineage origin  
- lifecycle status

This makes the dataset structure inspectable without replacing dedicated lineage, query contract  
or metadata editing pages.

---

## 🔧 8. Governance Boundary

The Architecture Catalog is a discovery surface.

It does not:

- create Approval Artifacts  
- check approvals  
- execute loads  
- delete execution records  
- mutate metadata

Architecture Control remains responsible for approval state, execution preview, controlled execution,  
execution records, execution history and retention cleanup.

---

© 2025-2026 elevata Labs — Internal Technical Documentation
