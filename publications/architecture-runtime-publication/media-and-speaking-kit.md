# Architecture Runtime Media & Speaking Kit

> **Author:** Ilona Tag  
> **Publication series:** Architecture Runtime  
> **Reference implementation:** elevata  
> **Status:** Public media and speaking material

## Purpose

This document provides concise material for editors, event organizers, community hosts, podcast hosts, and anyone interested in the Architecture Runtime concept.

It summarizes the core idea behind Architecture Runtime, suggests article and talk angles, and provides a compact author profile.

The material can be used as a starting point for editorial conversations, conference proposals, community sessions, interviews, or podcast discussions.

## One-sentence positioning

Architecture Runtime is a category for modern data platforms that makes data architecture explicit, metadata-defined, deterministic, controllable, executable, and auditable.

## Short abstract

Modern data platforms have industrialized execution, but architecture often remains implicit.

Here, architecture does not mean infrastructure. It means the structural backbone of a data platform: datasets, layers, relationships, ownership, semantics, lineage, behavior, control, and evidence.

Data can be ingested, transformed, orchestrated, cataloged, and served faster than ever before. Yet the structural backbone of the platform often lives scattered across SQL, Python jobs, orchestration rules, catalogs, BI models, documentation, naming conventions, and team memory.

Architecture Runtime introduces a missing system layer for modern data platforms: a metadata-defined runtime that makes architecture explicit, deterministic, controllable, executable, and auditable.

Instead of documenting architecture after implementation, an Architecture Runtime defines the architectural system before execution and turns it into reproducible runtime artifacts, including logical plans, dialect-aware SQL, reviewable change reports, controlled execution, and audit evidence.

## Long abstract

Modern data platforms are no longer limited by execution alone.

They can ingest data, transform it, orchestrate workloads, scale compute, deploy environments, and expose analytics quickly.

But one crucial layer often remains implicit: architecture.

In this context, architecture does not mean infrastructure. It does not mean networks, cloud landing zones, IP ranges, or deployment topology. It means the structural backbone of a data platform: datasets, layers, relationships, lineage, ownership, semantics, behavior, control, and evidence.

In many platforms, this architecture is not defined in one explicit system. It is scattered across implementation details: SQL, Python jobs, notebooks, orchestration rules, transformation frameworks, BI models, catalogs, documentation, naming conventions, and team memory.

This creates platforms that may work operationally, but are difficult to reason about architecturally. Changes become hard to review. Dependencies become hard to verify. Semantics become distributed. Best practices are reinvented repeatedly. Platform choices become sticky because architecture is embedded inside tool-specific implementation.

Architecture Runtime changes this by treating metadata as the operational definition of architecture.

From metadata, the runtime can derive architectural structures, logical plans, dialect-aware SQL, execution scopes, architecture state, change reports, approval artifacts, execution records, catalog views, and audit evidence.

The platform executes workloads.  
The runtime defines the architectural system.

This makes architecture reproducible, reviewable, executable, and auditable. It also makes supported execution platforms interchangeable at the architectural level: the same solution can target different platforms without changing the solution itself, while platform-specific infrastructure and operations remain outside the architectural model.

elevata is an open-source reference implementation of this idea. It demonstrates that Architecture Runtime is not only a conceptual category, but an implementable approach for modern data platforms.

## Why now?

Modern data platforms have become highly capable, but also increasingly fragmented.

Organizations combine warehouses, lakehouses, orchestration tools, transformation frameworks, catalogs, semantic layers, BI tools, notebooks, and custom code.

Each tool may be useful.

But together, they do not automatically create an explicit architecture.

This matters because data platforms now need to support:

- continuous change  
- multiple teams  
- governed semantics  
- reproducible execution  
- auditability  
- platform evolution  
- AI-assisted development without losing deterministic control  

Architecture Runtime addresses this gap by turning architecture from an implicit implementation outcome into an explicit runtime system.

## Target audience

This topic is relevant for:

- data architects  
- data engineers  
- analytics engineers  
- platform engineers  
- technical leaders  
- data governance stakeholders  
- decision-makers responsible for long-lived data platforms  
- communities discussing Modern Data Stack, DataOps, metadata, governance, and platform engineering  

## Key takeaways

Readers or attendees should understand:

- why pipelines are not the same as platforms  
- why architecture means more than data models or infrastructure  
- why SQL, Python, notebooks, and orchestration should be treated as implementation artifacts  
- why metadata must become operational, not merely descriptive  
- why determinism is a trust requirement for platform architecture  
- why discovery is not the same as control  
- how Architecture Runtime makes architecture executable and auditable  
- how elevata demonstrates the concept as an open-source reference implementation  

## Suggested article angles

The following angles can be used as starting points for tailored guest articles, editorial contributions, interviews, or community discussions.

### Why Modern Data Platforms Need an Architecture Runtime

A broad category article explaining the hidden architecture problem in modern data platforms and introducing Architecture Runtime as a missing system layer.

### The Platform Is Not the Pipeline

An opinionated article about why moving data through pipelines does not automatically create a governed, understandable, and executable platform architecture.

### Discovery Is Not Control

A governance-oriented article about why catalogs are valuable but insufficient when architecture changes need review, approval, controlled execution, and audit evidence.

### SQL Is an Artifact

A technical architecture article explaining why SQL remains essential, but should be treated as an artifact derived from explicit architectural intent rather than the primary place where architecture lives.

### Platforms Become Choices Again

A platform strategy article about how metadata-defined architecture and dialect-owned rendering can reduce architectural lock-in across supported execution platforms.

## Suggested talk titles

The following titles describe variations of the same core talk and can be adapted to different audiences, formats, or event themes.

- Why Modern Data Platforms Need an Architecture Runtime  
- The Missing Layer of the Modern Data Stack: Architecture Runtime  
- The Platform Is Not the Pipeline  
- SQL Is an Artifact: Architecture Is the Contract  
- From Implicit Pipelines to Metadata-Defined Architecture  
- Discovery Is Not Control: Why Data Catalogs Are Not Enough  
- Architecture Runtime: Making Data Architecture Executable  

## Suggested talk abstract

Modern data platforms have industrialized execution. We can ingest data, transform it, orchestrate workloads, scale compute, and expose analytics quickly.

But one crucial layer often remains implicit: architecture.

By architecture, this talk does not mean infrastructure. It means the structural backbone of a data platform: datasets, layers, relationships, ownership, semantics, lineage, behavior, control, and evidence.

In practice, these architectural decisions are often spread across SQL, Python jobs, orchestration rules, transformation frameworks, catalogs, BI models, documentation, naming conventions, and team memory. Over time, this creates drift, repeated design decisions, inconsistent structures, and changes that are difficult to review before execution.

This talk introduces Architecture Runtime as a missing system layer for modern data platforms: a metadata-defined runtime that makes architecture explicit, deterministic, controllable, executable, and auditable.

Using elevata as an open-source reference implementation, the talk shows how metadata can define architectural structure, generate deterministic runtime artifacts, support review and approval, execute warehouse-native workloads, and preserve architecture evidence.

## Possible session formats

- 20-minute impulse talk  
- 30-minute community talk  
- 45-minute conference session  
- podcast interview  
- editorial interview  
- written guest article  
- architecture roundtable discussion  

## Discussion questions

- Where does architecture live in modern data platforms today?  
- Is architecture explicitly modeled, or reconstructed from implementation details?  
- Are data catalogs enough if architecture changes need review and approval?  
- Should the structural backbone of a platform be inferred probabilistically?  
- What happens when platform choices change, but architecture is embedded inside the tool?  
- How much project work is spent reinventing the same architectural mechanics?  

## About Ilona Tag

Ilona Tag works in Data & Analytics with a focus on data architecture, data engineering, metadata-driven platform design, and governance-oriented data platform concepts.

She is the creator of elevata®, an open-source Architecture Runtime for modern data platforms.

With elevata, she explores how metadata can define executable architecture across data platforms: from dataset structure, lineage, keys, semantics, and contracts to review, controlled execution, catalog visibility, and audit evidence.

Her current work focuses on making data architecture explicit, deterministic, discoverable, controllable, executable, and auditable.

## Reference implementation

elevata is the open-source reference implementation of the Architecture Runtime concept.

Repository: https://github.com/elevata-labs/elevata  
Documentation: https://elevata-labs.github.io/elevata/

## Publications

- [Architecture Runtime Manifesto](architecture-runtime-manifesto.md)  
- [Why Modern Data Platforms Need an Architecture Runtime](architecture-runtime-essay.md)  

## Rights

© 2026 Ilona Tag. All rights reserved.  
This publication material is governed by [PUBLICATION_RIGHTS.md](PUBLICATION_RIGHTS.md) and is not part of the repository's AGPL-3.0 software license.
