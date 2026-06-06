# Why Modern Data Platforms Need an Architecture Runtime
## From implicit pipelines to metadata-defined, executable architecture

> **First published:** 2026-06-06  
> **Author:** Ilona Tag  
> **Publication series:** Architecture Runtime  
> **Reference implementation:** elevata  
> **Status:** First public version

<p align="center">
  <img src="assets/architecture-runtime-essay-runtime-layer.png" alt="Layered architecture runtime emerging from a central metadata core" width="900"/>
</p>

Modern data platforms have industrialized execution.

They can ingest data, transform it, orchestrate workloads, scale compute, deploy environments, and expose analytics faster than ever before.

But one fundamental layer often remains implicit:

architecture.

In this essay, architecture does not mean infrastructure.

It does not mean networks, cloud landing zones, IP ranges, or deployment topology.

It means the structural backbone of a data platform: datasets, layers, relationships, lineage, ownership, semantics, behavior, control, and evidence.

A modern data platform is not valuable simply because data moves through pipelines.

It becomes valuable when movement, structure, semantics, governance, execution, and evidence follow a coherent architecture.

But in many platforms, architecture does not live in one explicit system.

It is scattered across SQL, Python jobs, notebooks, orchestration rules, transformation frameworks, BI models, catalogs, documentation, naming conventions, and team memory.

Often, it is a platform assembled from implementation decisions.

An Architecture Runtime changes this.

It treats metadata as the operational definition of architecture.

From that metadata, the runtime derives architectural structures, logical plans, dialect-aware SQL, execution scopes, architecture state, change reports, approval artifacts, execution records, catalog views, and audit evidence.

---

## Part I - The hidden architecture problem

<p align="center">
  <img src="assets/architecture-runtime-essay-hidden-architecture.png" alt="Fragmented architectural landscape representing hidden architecture across implementation details" width="900"/>
</p>

### We industrialized execution, but architecture stayed implicit

Over the last decades, data engineering has become remarkably good at execution.

We built ingestion frameworks.  
We automated transformations.  
We introduced orchestration.  
We scaled compute.  
We standardized deployment.  
We made data platforms faster to assemble than ever before.

Execution is no longer the main bottleneck.

But architecture often still emerges during implementation.

Models are designed.  
Business logic is implemented.  
Transformations are assembled.  
Dependencies grow.  
Conventions spread.  
Documentation tries to keep up.

This is not necessarily the result of bad engineering.

It is often the result of good engineering inside incomplete architectural boundaries.

Teams solve local problems.  
They make reasonable implementation decisions.  
They add transformations where they are needed.  
They encode assumptions where the tools allow them to encode assumptions.

And over time, the platform starts to work.

But working is not the same as being architecturally explicit.

The result is not always a deliberately engineered platform.

Often, it is a platform assembled from implementation decisions.

### Architecture means more than data models

Data architecture is often reduced to modeling.

Modeling matters.

But architecture is more than a conceptual model or a set of tables.

It is the structural backbone of the platform.

It includes:

- ingestion paths  
- landing and raw preservation  
- staged integration  
- core and business modeling  
- deterministic key generation  
- source consolidation patterns  
- incremental processing  
- historization  
- schema evolution  
- lineage  
- ownership  
- contracts  
- serving boundaries  
- review state  
- execution evidence  

A layered architecture matters because responsibilities need boundaries.

A raw or landing layer should preserve technical truth.  
A staging or integration layer should standardize access.  
A core layer should stabilize structure.  
A business layer should express meaning intentionally.  
A serving layer should expose trusted consumption without hiding how the result was produced.

But layers alone are not enough.

A modern data platform also needs the repeatable mechanisms that make those layers executable.

How are stable keys generated?  
How are structurally equivalent source objects consolidated?  
How is incremental change processed?  
How is history preserved?  
How are schemas evolved safely?  
How are serving contracts protected?  
How does execution prove what happened?

Those are not secondary implementation details.

They are part of the architecture.

### Tool-centric platforms do not automatically define architecture

Warehouses, lakehouses, transformation frameworks, orchestration tools, catalogs, semantic layers, and BI tools are powerful.

But each tool usually owns only part of the system.

The execution platform runs workloads.  
The orchestrator schedules them.  
The catalog observes assets.  
The semantic layer exposes meaning.  
The transformation framework structures code.  
The BI layer consumes results.

None of these alone defines the full architectural system.

That is why architecture often has to be reconstructed inside the tools.

A platform may have excellent pipelines and still lack explicit architecture.

It may have a catalog and still lack control.

It may have orchestration and still lack architectural review.

It may have semantic models and still hide business meaning behind query-time abstractions.

Tooling can make a platform powerful.

But tooling alone does not make the architecture explicit.

### Pipelines are not platforms

A pipeline moves data.

A platform must remain understandable, governable, executable, and trustworthy over time.

That distinction matters.

A collection of pipelines can produce correct data today while still creating architectural debt tomorrow.

A platform needs explicit answers:

- What is the intended structure?  
- Which layer owns which responsibility?  
- Which data object represents which business meaning?  
- Which dependencies are intentional?  
- Which changes are safe?  
- Which execution was approved?  
- Which evidence proves what happened?  

Without an explicit architecture system, these questions are answered indirectly through implementation details.

Someone reads SQL.  
Someone traces Python jobs.  
Someone checks orchestration definitions.  
Someone compares BI models.  
Someone searches documentation.  
Someone asks the people who still remember why things were built that way.

That is not architecture as a system.

That is architecture as archaeology.

---

## Part II - The runtime shift

### SQL, Python, and notebooks are artifacts

SQL remains essential.

It is powerful, expressive, inspectable, and deeply embedded in analytical platforms.

Python jobs, notebooks, templates, generated SQL, and transformation scripts may also be essential.

But they should not be the primary place where architecture lives.

Implementation artifacts should express architecture.

They should not be the only source from which architecture can be inferred.

When architecture lives in implementation artifacts, intent becomes dependent on interpretation.

The question shifts from:

```text
What architecture did we define?
```

to:

```text
What architecture can we reconstruct from the implementation?
```

That is the wrong direction.

An Architecture Runtime reverses it.

This is the shift:

> **Architecture first.**  
> **Artifacts second.**

Metadata defines intent.  
The runtime derives executable artifacts.

SQL remains visible.

Python may remain useful.

Orchestration remains necessary.

But architecture no longer disappears inside them.

### Metadata must become operational

Many platforms already have metadata.

But metadata is often descriptive.

It documents what exists.

It helps users search, classify, inspect, and understand.

That is valuable.

But an Architecture Runtime treats metadata differently.

Metadata becomes operational.

It defines architecture in a form the runtime understands.

From this metadata, the runtime can derive:

- logical plans  
- dialect-aware SQL  
- schema evolution intent  
- execution scopes  
- control states  
- catalog signals  
- architecture evidence  

This is the difference between documenting architecture and executing architecture.

Documentation explains what should be true.

Operational metadata makes the truth executable.

### Determinism comes before intelligence

Architecture must be explicit, reproducible, and guaranteed.

AI can be useful for explanation, assistance, summarization, exploration, and developer productivity.

But the structural backbone of a platform should not be inferred probabilistically.

AI is based on probability and mathematical statistics, not certainty.

That distinction matters.

AI is useful where probability is acceptable.

Architecture is not such a place.

If every AI-generated result still needs to be validated, debugged, constrained, and corrected, then the practical question becomes unavoidable:

Is that more efficient than maintaining a small amount of explicit metadata and generating architecture reproducibly from it?

For architecture, determinism is a fundamental trust requirement.

The same metadata should produce the same logical plan.  
The same logical plan should produce semantically equivalent SQL for the selected dialect.  
The same architecture state should produce the same fingerprint.  
The same approved change should remain verifiable.  
The same controlled execution should produce an audit record that can be inspected later.

Without determinism, governance becomes subjective.

Without determinism, reviews become fragile.

Without determinism, generated artifacts become hard to trust.

Without determinism, audit evidence becomes weak.

A runtime that governs architecture must be reproducible by design.

### Discovery is not control

Catalogs are valuable.

They help users find datasets, inspect lineage, understand ownership, and see quality signals.

But discovery does not equal control.

A catalog can show that a dataset exists.  
It does not necessarily decide whether an architecture change is safe.

A catalog can show lineage.  
It does not necessarily approve schema evolution.

A catalog can show ownership.  
It does not necessarily enforce execution guardrails.

A catalog can show quality signals.  
It does not necessarily produce runtime evidence.

An Architecture Runtime adds the missing operational layer:

- architecture state  
- change reports  
- policy decisions  
- approvals  
- execution previews  
- controlled execution  
- execution records  

This turns architecture changes into explicit, reviewable, executable, and auditable events.

Discovery tells us what exists.

Control decides what may change and what may run.

Evidence proves what happened.

---

## Part III - What changes when architecture becomes executable

<p align="center">
  <img src="assets/architecture-runtime-essay-platform-choice.png" alt="Central architectural core connecting to multiple execution platforms" width="900"/>
</p>

### Architecture Runtime as the missing layer

An Architecture Runtime sits above the execution platform.

It does not replace the warehouse.

It does not replace orchestration.

It does not necessarily replace transformation frameworks.

The principle is simple:

The platform executes workloads.  
The runtime defines the architectural system.

Depending on the implementation, an Architecture Runtime may integrate existing tools or generate transformations itself.

But the architectural idea remains the same:

Transformation frameworks help structure transformation logic.  
An Architecture Runtime structures the entire system.

It defines where data enters.  
It defines how layers behave.  
It defines how relationships are represented.  
It defines which changes are safe.  
It defines what must be reviewed.  
It defines what execution evidence means.

That is why it is a runtime.

Not because it simply runs jobs.

But because architecture itself becomes executable.

### Platforms become choices again

When architecture lives inside platform-specific implementation details, platform decisions become sticky.

Changing platforms then means more than changing execution engines.

It means rediscovering structure, dependencies, naming, contracts, lineage, materialization behavior, and business semantics.

An Architecture Runtime changes the center of gravity.

The platform may change.  
The SQL dialect may change.  
The execution backend may change.  
The architecture remains defined above those choices.

When the architectural model is metadata-defined and dialect rendering is owned by the runtime, the same solution can target different supported platforms without changing the solution itself.

It can run on Databricks today, Fabric tomorrow, Snowflake after that, or any other supported execution backend - as long as the runtime owns the architecture and the platform only executes the rendered artifacts.

This does not mean that infrastructure, security, deployment, and operational setup disappear.

But those are platform operations.

They are not the architectural model.

Teams gain freedom of choice because the architecture no longer lives inside one specific tool.

### Best practices stop being reinvented in every project

Many architecture decisions repeat from project to project.

How to generate stable keys.  
How to consolidate structurally equivalent source objects.  
How to handle incremental change.  
How to preserve history.  
How to evolve schemas safely.  
How to define serving boundaries.  
How to expose lineage and contracts.

In traditional projects, these mechanisms are often implemented again and again.

Sometimes explicitly.

Sometimes through templates.

Sometimes through copied patterns.

Sometimes through individual engineering judgment.

An Architecture Runtime can encode them once as reusable architectural behavior.

This fundamentally changes how data platforms are built.

Teams no longer start every project by reinventing technical mechanics.

They start from an architectural system where best practices are already part of the runtime.

The work shifts.

Less time is spent rebuilding the same foundations.

More time is spent defining the actual business architecture.

### elevata as reference implementation

elevata® is an open-source reference implementation of the Architecture Runtime idea.

It demonstrates that the concept is not only theoretical.

It is implementable.

It works.

In elevata, architecture can be modeled through metadata, maintained through an intuitive, easy-to-use user interface, transformed into deterministic logical plans, rendered into dialect-aware SQL, executed warehouse-native, reviewed through Architecture Control, discovered through Architecture Catalog, and preserved through execution evidence.

The point is not that every Architecture Runtime must look exactly like elevata.

The point is that the idea is real.

Architecture can become executable.

---

## Conclusion - Architecture becomes the contract

Modern data platforms are no longer limited by execution alone.

They are limited by architecture that remains implicit.

An Architecture Runtime makes that architecture explicit, executable, controllable, and auditable.

That is the shift:

from assembled pipelines  
to engineered platforms

from implementation details  
to metadata-defined architecture

from platform lock-in  
to architectural portability

from reinvented mechanics  
to reusable best-practice behavior

from running jobs  
to governing architecture

from documentation after the fact  
to runtime evidence

SQL remains an artifact.

Architecture becomes the contract.

---

© 2026 Ilona Tag. All rights reserved.  
This publication material is governed by [PUBLICATION_RIGHTS.md](PUBLICATION_RIGHTS.md) and is not part of the repository's AGPL-3.0 software license.
