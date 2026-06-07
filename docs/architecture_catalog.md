# ⚙️ Architecture Catalog

The Architecture Catalog is the read-only discovery layer for metadata-defined executable architecture.

It helps users understand:

- what datasets exist  
- why they exist  
- how they are defined  
- how they are connected  
- how they are controlled  
- where execution evidence is available  
- how architecture flows across layers  
- how portfolio posture looks across governance and execution evidence  
- which serving-layer datasets are ready for trusted consumption  
- which architecture quality and governance signals need attention

The Catalog does not edit metadata and does not execute loads.

---

## 🔧 1. Purpose

elevata treats architecture as executable metadata.

The Architecture Catalog provides a structured way to discover that architecture without entering editing or execution workflows.

It complements Architecture Control:

```text
Architecture Catalog
  = What exists, why it exists, how it is defined, how it is connected,
    how it is controlled, where its execution evidence is,
    and how portfolio posture looks across the architecture.

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
- Portfolio worklist signals

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
- entry point to Catalog Portfolio  
- entry point to Catalog Data Products  
- entry point to Catalog Insights  
- entry point to Catalog Map

The workspace links to:

- dataset details  
- lineage  
- query contract  
- Architecture Control  
- Catalog detail  
- Catalog Portfolio  
- Catalog Data Products  
- Catalog Map

---

## 🔧 3. Catalog Portfolio

The Architecture Catalog Portfolio provides a read-only executive lens across metadata-defined executable architecture.

It answers the portfolio-level question:

```text
What is the overall architecture posture across readiness, ownership,
contracts, health, review state, execution evidence and layers?
```

The Portfolio shows:

- active dataset coverage  
- ownership coverage  
- contract coverage  
- metadata health clearance  
- Architecture Control review clearance  
- Architecture Execution Record evidence coverage  
- Data Product readiness distribution  
- aggregated attention areas  
- layer-level ownership, contract, health, execution evidence and custom query signals

Portfolio metrics are aggregated posture signals rather than dataset lists.  
When a metric needs action, its button opens a filtered Catalog worklist with the affected TargetDatasets. Users can then inspect each dataset in Catalog Detail and navigate to the existing Details, Contract, Lineage or Architecture Control entry points.

Portfolio layer rows link to filtered Catalog layer views.

The Portfolio does not edit metadata, create approvals, check approvals or execute loads.

---

## 🔧 4. Catalog Insights

Architecture Catalog Insights provide read-only architecture quality and governance signals across TargetDatasets.

Insights summarize:

- datasets without assigned ownership  
- datasets with metadata health issues  
- datasets with metadata health warnings  
- datasets with custom query logic  
- datasets without downstream consumers  
- inactive datasets with downstream consumers  
- datasets without Architecture Execution Record evidence

Insight cards show compact dataset lists and can expand to reveal all matching datasets.

Each dataset entry links back to the Catalog detail view, where dataset-specific insight signals are shown in context.

Catalog Insights do not create approvals, check approvals, execute loads, delete execution records, or mutate metadata.

---

## 🔧 5. Catalog Data Products

Architecture Catalog Data Products provide a read-only consumer-readiness perspective for serving-layer datasets.

They help users understand which metadata-defined architecture objects are ready for trusted consumption by combining existing signals from:

- ownership  
- metadata health  
- query contract columns  
- upstream and downstream relationships  
- Architecture Control review state  
- latest Architecture Execution Record evidence  
- lifecycle status  
- query logic transparency

Readiness is shown through transparent groups:

- Consumption-ready  
- Review recommended  
- Not consumption-ready

Catalog Data Products focus on the serving layer. Bizcore remains the business logic implementation layer and stays visible in the Architecture Catalog, Catalog Insights and Catalog Maps without being presented as a consumer-facing Data Product.

Catalog detail pages also show dataset-specific Consumer Readiness, so users can understand why a dataset is ready for consumption or why it belongs to a non-consumer architecture layer.

Catalog Data Products derive readiness from existing architecture metadata. They do not edit metadata, request access or execute loads.

---

## 🔧 6. Catalog Map

The Architecture Catalog Map provides a read-only architecture lens across schemas, layers and direct TargetDataset dependencies.

It helps users understand:

- how datasets are distributed across architecture layers  
- how populated layers connect to each other  
- which direct TargetDataset dependencies cross layer boundaries  
- where custom query logic appears in layer context  
- which datasets and dependency examples explain each layer transition

The Catalog Map includes:

- layer summary cards grouped by schema / layer  
- layer flow overview for populated architecture layers  
- source-to-target layer dependency matrix  
- layer transition groups with expandable dependency examples  
- dataset links to Catalog detail pages and lineage pages

The Catalog Map uses direct TargetDataset dependencies. It does not replace the dedicated lineage view and does not introduce graph editing, execution controls or metadata mutations.

---

## 🔧 7. Catalog Detail View

The Catalog detail view summarizes one TargetDataset as an architecture object.

It displays:

- architecture summary  
- ownership  
- metadata health findings  
- latest Architecture Execution Record summary  
- upstream inputs  
- downstream consumers  
- column contract signals  
- dataset-specific Consumer Readiness  
- dataset-specific Catalog insight signals  
- Architecture Control review status summary

The detail view remains read-only. Editing stays on the dataset detail and scoped metadata pages. Execution and approval workflows stay in Architecture Control.

---

## 🔧 8. Review Status

For TargetDataset scopes, the Catalog detail view surfaces the Architecture Control review status as a read-only summary.

The review status summary includes:

- review state  
- review message  
- report fingerprint reference  
- architecture change indicator  
- policy status  
- link to the selected Architecture Control scope

Catalog detail pages use the existing Architecture Control review status contract. They do not create approvals and do not run approval checks.

---

## 🔧 9. Execution Evidence

For TargetDataset scopes, the Catalog detail view surfaces the latest Architecture Execution Record summary when one exists.

The evidence summary includes:

- execution status  
- start timestamp  
- duration  
- dependency mode  
- compact record fingerprint  
- link to Architecture Control execution history

Architecture Execution Records remain stored, filtered, downloaded and governed through Architecture Control.

The Catalog shows the latest evidence reference in dataset context without duplicating the execution history workspace.

---

## 🔧 10. Lineage and Contract Signals

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

This makes the dataset structure inspectable without replacing dedicated lineage, query contract or metadata editing pages.

---

## 🔧 11. Governance Boundary

The Architecture Catalog is a discovery surface.

It does not:

- create Approval Artifacts  
- check approvals  
- request access  
- execute loads  
- delete execution records  
- mutate metadata

Architecture Control remains responsible for approval state, execution preview, controlled execution, execution records, execution history and retention cleanup.

---

© 2025-2026 elevata - Technical Documentation
