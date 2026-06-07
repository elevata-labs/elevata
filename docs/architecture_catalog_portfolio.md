# ⚙️ Architecture Catalog Portfolio

Architecture Catalog Portfolio is the read-only executive lens across metadata-defined executable architecture.

It summarizes platform posture across readiness, ownership, contracts, health, review state, execution evidence and architecture layers without turning the Catalog into a reporting dashboard or an execution surface.

---

## 🔧 1. Purpose

elevata treats architecture as executable metadata.

The Architecture Catalog Portfolio provides a compact way to understand the state of that architecture at portfolio level:

```text
Architecture Catalog Portfolio
  = Overall architecture posture across governance coverage,
    execution evidence, review state, Data Product readiness and layers.
```

The Portfolio complements the other Catalog lenses:

```text
Catalog Workspace
  = Find and inspect architecture objects.

Catalog Insights
  = Understand concrete quality and governance signals.

Catalog Map
  = Understand layer structure and direct dependencies.

Catalog Data Products
  = Understand consumer readiness for serving-layer datasets.

Catalog Portfolio
  = Understand aggregated architecture posture and open focused worklists.
```

---

## 🔧 2. Portfolio Metrics

The Portfolio shows compact metrics for active architecture scope and coverage.

Metric cards include:

- active datasets  
- ownership coverage  
- contract coverage  
- metadata health clearance  
- Architecture Control review clearance  
- Architecture Execution Record evidence coverage

Completed metrics remain informational.  
Metrics with open attention link to filtered Catalog worklists for the affected datasets.

This keeps the Portfolio action-oriented without adding editing or execution forms.

---

## 🔧 3. Worklist Drill-Downs

Portfolio drill-downs use existing Catalog filtering.

Supported worklist signals include:

- missing ownership  
- missing contract columns  
- metadata health attention  
- Architecture Control review attention  
- missing Architecture Execution Record evidence  
- inactive datasets with active downstream consumers

A worklist opens the Catalog with the matching datasets and a clear active-filter message. Users can then inspect each TargetDataset through Catalog Detail and navigate to existing Details, Query Contract, Lineage or Architecture Control entry points.

The full Catalog reset link returns from a worklist or layer filter to the unfiltered Catalog view.

---

## 🔧 4. Data Product Readiness Summary

The Portfolio includes the Data Product readiness distribution for serving-layer datasets.

It shows how many active Data Products are:

- Consumption-ready  
- Review recommended  
- Not consumption-ready

The readiness cards link to filtered Catalog Data Product views.

Data Product readiness remains owned by the Catalog Data Products lens. The Portfolio uses it as an aggregated posture signal.

---

## 🔧 5. Portfolio Attention Areas

Portfolio attention areas summarize the main governance and quality signals that need attention across the active architecture portfolio.

They include:

- missing ownership  
- missing contract columns  
- health attention  
- review attention  
- missing execution evidence  
- inactive datasets with consumers

Signals with affected datasets link to focused Catalog worklists. Signals without affected datasets stay informational.

---

## 🔧 6. Layer Portfolio Overview

The layer portfolio overview groups posture signals by architecture layer.

For each layer it shows:

- dataset count  
- active dataset count  
- ownership coverage  
- contract coverage  
- metadata health attention  
- execution evidence coverage  
- custom query count

Layer names link to Catalog views filtered by schema / layer.

This makes layer posture explainable without replacing the Catalog Map or dedicated Lineage views.

---

## 🔧 7. Governance Boundary

Architecture Catalog Portfolio is part of the read-only Architecture Catalog.

It does not:

- edit metadata  
- create approvals  
- run approval checks  
- execute loads  
- create execution records  
- delete execution records  
- replace Catalog Insights  
- replace Architecture Control

Architecture Control remains responsible for approval state, execution preview, controlled execution, execution records, execution history and retention cleanup.

---

© 2025-2026 elevata - Technical Documentation
