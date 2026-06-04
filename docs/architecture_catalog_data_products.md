# ⚙️ Architecture Catalog Data Products

Architecture Catalog Data Products provide a read-only consumer-readiness
perspective for serving-layer datasets.

They help users understand which metadata-defined architecture objects are ready for trusted consumption  
and which ones need attention before they are exposed as consumer-facing assets.

Data Product readiness is derived from existing architecture metadata. It is not
a separate declaration layer.

---

## 🔧 1. Purpose

Modern data platforms often expose many datasets, tables and views.

The Architecture Catalog Data Products view focuses on a narrower question:

```text
Which serving-layer datasets are ready for trusted consumption?
```

This perspective brings together signals that already exist in elevata:

- ownership  
- metadata health  
- query contract columns  
- upstream lineage  
- downstream consumers  
- Architecture Control review state  
- Architecture Execution Record evidence  
- lifecycle status  
- query logic transparency

The result is a consumer-oriented view of executable architecture without adding
a separate marketplace workflow.

---

## 🔧 2. Serving-Layer Scope

Catalog Data Products focus on serving-layer datasets.

```text
bizcore  = business logic implementation
serving  = consumer-facing presentation and consumption layer
```

Bizcore datasets remain visible in the Architecture Catalog, Catalog Insights and Catalog Maps.  
They are part of the executable architecture, but they are not presented as consumer-facing Data Products.

Serving datasets represent the architecture layer where curated, presentation-ready
assets are exposed for consumption.

---

## 🔧 3. Readiness Groups

Each Data Product candidate is assigned to one transparent readiness group:

| Group | Meaning |
|---|---|
| Consumption-ready | No blocking or review-level readiness signals are present |
| Review recommended | The dataset is usable for inspection but has signals that need review |
| Not consumption-ready | Blocking signals prevent trusted consumption |

The groups are derived from visible signals. They are not a hidden score.

---

## 🔧 4. Readiness Signals

Consumer readiness uses explicit signals that can be inspected per dataset.

Signal areas include:

- active lifecycle state  
- serving-layer suitability  
- business-facing description  
- ownership assignment  
- metadata health  
- query contract column availability  
- direct upstream lineage  
- downstream consumer visibility  
- Architecture Control review state  
- latest Architecture Execution Record evidence  
- query logic transparency

Signals are displayed with clear severity:

| Severity | Meaning |
|---|---|
| OK | The signal supports trusted consumption |
| Warning | The signal deserves review before broad consumption |
| Blocking | The signal prevents consumption readiness |
| Info | The signal adds transparency without reducing readiness |

This keeps Data Product readiness explainable and auditable.

---

## 🔧 5. Data Product Cards

The Data Products page displays one card per serving-layer candidate.

Each card shows:

- dataset key  
- schema / layer  
- readiness group  
- description  
- owner count and owner labels  
- contract column count  
- upstream and downstream counts  
- metadata health state  
- query logic state  
- Architecture Control review state  
- latest execution evidence summary  
- expandable readiness signals

Each card links to:

- Catalog detail  
- lineage  
- query contract  
- Architecture Control

The page supports filtering by search term, consumer layer, readiness group and
lifecycle status.

---

## 🔧 6. Catalog Detail Consumer Readiness

Catalog detail pages include a dataset-specific Consumer Readiness card.

This card uses the same signal model as the Data Products page and shows the readiness context  
for the selected TargetDataset.

For serving datasets, the card explains consumption readiness.

For non-consumer layers, the card explains why the selected dataset is part of the executable architecture  
but is not offered as a consumer-facing Data Product.

---

## 🔧 7. Relationship to Data Marketplace Concepts

Catalog Data Products provide a foundation for marketplace-style transparency without turning elevata  
into a generic marketplace, request-access portal or business glossary platform.

The view focuses on architecture-derived readiness:

```text
metadata-defined architecture
  → ownership, health, lineage, contracts, review and evidence
  → trusted consumption readiness
```

This keeps the Architecture Catalog aligned with elevata's Architecture Runtime model:  
Data Products are not declared separately from architecture. They are recognized through the  
architecture signals that make consumption trustworthy.

---

## 🔧 8. Relationship to Architecture Catalog Portfolio

Architecture Catalog Portfolio uses Data Product readiness as one portfolio-level signal among ownership,  
contracts, health, review state, execution evidence and layer distribution.

Data Products answer which serving-layer datasets are ready for trusted consumption.  
Portfolio summarizes how that readiness contributes to the overall architecture posture.

The separation keeps Data Products focused on consumer readiness and Portfolio focused on aggregated  
architecture posture.

---

+## 🔧 9. Governance Boundary

Architecture Catalog Data Products are part of the read-only Architecture Catalog.

They do not:

- edit metadata  
- request access  
- create approvals  
- run approval checks  
- execute loads  
- create execution records  
- delete execution records

Architecture Control remains responsible for approval state, execution preview, controlled execution,  
execution records, execution history and retention cleanup.

---

© 2025-2026 elevata Labs — Internal Technical Documentation
