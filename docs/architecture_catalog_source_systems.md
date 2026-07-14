# ⚙️ Architecture Catalog Source Systems

Architecture Catalog Source Systems provide the read-only source-side view of metadata-defined executable architecture.

They connect Source System and SourceDataset metadata with deterministic Source Ingestion Readiness, RAW landing intent and modeled RAW TargetDataset handoffs.

---

## 🔧 1. Purpose

The Source Systems view answers:

```text
Which source architecture objects exist, how are they expected to enter the
platform, and which SourceDatasets need attention before controlled execution?
```

This closes the discovery gap between source metadata import and the target architecture.

The Catalog architecture flow is:

```text
Source Systems
  → SourceDatasets
  → RAW TargetDatasets
  → target architecture layers
  → serving-layer Data Products
```

---

## 🔧 2. Source Ingestion Readiness

Each SourceDataset receives one transparent readiness state.

| State | Meaning |
|---|---|
| Ready | No blocking or warning signal is present |
| Attention | At least one blocking or warning signal needs resolution or review |
| Not applicable | The dataset is inactive, outside integration scope, or intentionally uses direct or federated access without elevata-managed RAW landing |
| Unavailable | The dataset cannot be evaluated as a valid source architecture object |

The state is derived from explicit diagnostic signals. It is not a score and does not use statistical or AI-based inference.

---

## 🔧 3. Evaluated Metadata

Readiness uses metadata already available to elevata.

Signal areas include:

- valid Source System association  
- Source System source classification  
- automated or manual metadata maintenance mode  
- SourceDataset lifecycle state  
- SourceDataset integration scope  
- RAW landing requirement  
- effective native, external or no-ingestion mode  
- active RAW TargetDataset input links  
- integrated SourceColumn availability  
- `json_path` availability for semi-structured integrated columns  
- file `ingestion_config` shape and canonical `uri`  
- JSON Lines file and Source System type alignment  
- Excel sheet selector consistency  
- REST dataset path and nested configuration shape  
- REST cursor type  
- relational incremental filter and active increment policy alignment

Native relational and REST ingestion also report that a runtime connection secret is expected. The readiness check does not resolve the secret and does not inspect whether its value is valid.

External ingestion remains an explicit architecture contract. When external ingestion is declared, elevata evaluates metadata and RAW handoff readiness without requiring a native connector configuration.

---

## 🔧 4. SourceDataset Detail

SourceDataset detail pages include a Source Ingestion Readiness panel.

The panel shows:

- readiness state  
- source type  
- metadata maintenance mode  
- RAW landing requirement  
- effective ingestion mode  
- integrated SourceColumn count  
- linked RAW TargetDatasets  
- diagnostic signals

The first diagnostic signals remain directly visible. Larger result sets use a bounded expander that shows only the remaining signals.

The panel explains configuration posture. It does not test external availability or prove that a source or external ingestion process delivered data successfully.

---

## 🔧 5. Global Source Systems View

The Architecture Catalog Source Systems page groups all SourceDatasets by their owning Source System.

The global summary shows:

- Source System count  
- SourceDataset count  
- Ready count  
- Attention count  
- Not applicable count  
- Unavailable count  
- blocking finding count  
- warning finding count

Each Source System group shows:

- short name and display name  
- source type  
- declared ingestion mode  
- lifecycle state  
- readiness posture  
- status distribution  
- blocking and warning counts  
- SourceDataset worklist

Datasets requiring attention are shown first. Ready and not-applicable datasets remain available through compact expanders. Expanded lists contain only rows that were not already visible.

---

## 🔧 6. Filters and Drill-Down

The Source Systems view supports:

- search across Source System, SourceDataset, schema and RAW target names  
- readiness state  
- Source System  
- source type  
- declared ingestion mode

Source System matches retain the matching system group. SourceDataset matches narrow the group to matching datasets.

Each SourceDataset links to its detail page for complete diagnostics. Each Source System links to its existing system detail page.

---

## 🔧 7. Portfolio Integration

Architecture Catalog Portfolio places Source ingestion readiness beside Data Product readiness.

The Source readiness card shows:

- Source System and SourceDataset counts  
- readiness distribution  
- blocking findings  
- warning findings

Each readiness group links to the Source Systems page with the corresponding status filter.

SourceDataset readiness remains separate from TargetDataset coverage metrics and TargetDataset attention areas because the underlying architecture objects and populations differ.

---

## 🔧 8. Catalog Map Integration

The Catalog Map starts with Source-to-RAW handoffs before showing target-layer flow.

Each handoff is classified as:

| Handoff state | Meaning |
|---|---|
| Mapped | RAW landing is required and exactly one active RAW TargetDataset is linked |
| No RAW landing | RAW landing is intentionally not required |
| RAW target missing | RAW landing is required but no active RAW TargetDataset is linked |
| Multiple RAW targets | More than one active RAW TargetDataset is linked |

The map groups handoffs by Source System and links SourceDatasets and RAW TargetDatasets to their existing details.

Source handoffs remain separate from the TargetDataset layer dependency matrix. This preserves the distinction between source entry points and target architecture layers.

---

## 🔧 9. Governance Boundary

Source Ingestion Readiness and the Source Systems Catalog view are read-only.

They do not:

- connect to source systems  
- resolve or display secrets  
- read files  
- call REST endpoints  
- import metadata  
- create or remove SourceColumns  
- generate TargetDatasets  
- edit ingestion configuration  
- execute native or external ingestion  
- execute target loads  
- create approvals  
- persist readiness history

Metadata import remains responsible for SourceColumn discovery and mutation. Target generation remains responsible for creating target architecture. Architecture Control remains responsible for review, approval, controlled execution and audit evidence.

---

## 🔧 10. Related Documents

- [Architecture Catalog](architecture_catalog.md)  
- [Architecture Catalog Portfolio](architecture_catalog_portfolio.md)  
- [Source Backends](source_backends.md)  
- [Source Metadata Import Review](source_metadata_import_review.md)  
- [Architecture Overview](architecture_overview.md)

---

© 2025-2026 elevata - Technical Documentation
