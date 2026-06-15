# Changelog

All notable changes to this project will be documented in this file.  
This project adheres to [Semantic Versioning](https://semver.org/) and [Keep a Changelog](https://keepachangelog.com/).

---

📈 For the full roadmap, see [Project Readme](https://github.com/elevata-labs/elevata/blob/main/README.md)

🧾 Licensed under the **AGPL-v3** — open, governed, and community-driven.  
💡 *elevata keeps evolving — one small, meaningful release at a time.*

---

## [2.9.0] - 2026-06-15

This release adds **Source Metadata Import Review**:  
a compact, deterministic import report for source metadata onboarding.

The report helps users understand what a metadata import actually did.  
It separates created, changed, unchanged and removed columns, surfaces detected primary key columns, and highlights skipped datasets or unresolved review points without changing the existing import workflow.

---

### ✨ Added

#### Source Metadata Import Review

- Added deterministic Source Metadata Import Report structures for import runs  
- Added dataset-level import review summaries for processed and skipped datasets  
- Added column-level import outcomes for:  
    - created  
    - changed  
    - unchanged  
    - removed  
- Added primary-key detection visibility in import results  
- Added needs-review indicators for skipped datasets and unresolved metadata decisions  
- Added compact import result UI with expandable dataset and column outcome details

---

### 🔄 Improved

#### Metadata Import UX

- Replaced ambiguous updated/refreshed wording with explicit changed and unchanged outcomes  
- Improved user confidence by distinguishing real metadata changes from checked-but-unchanged columns  
- Kept legacy import summary counters compatible for existing callers  
- Added structured report output for SQLAlchemy-backed, file-based and REST-oriented metadata import paths  
- Improved import result rendering inside list and detail contexts with stable left-aligned report content

#### Documentation

- Added Source Metadata Import Review documentation  
- Updated Source Backends documentation with import review positioning  
- Updated Architecture Overview with import review in the metadata ingestion layer  
- Updated README workflow and source ingestion summary  
- Updated CHANGELOG with v2.9.0 release notes

---

### 🔒 Governance & Determinism

- Source Metadata Import Review is deterministic  
- Source Metadata Import Review does not introduce AI-based inference  
- Source Metadata Import Review does not create a new import wizard or workflow  
- Source Metadata Import Review does not persist import history  
- Source Metadata Import Review does not add database models or migrations  
- Source Metadata Import Review does not change existing source import semantics  
- Existing metadata import behavior remains authoritative for actual SourceColumn mutations

---

### 🧪 Quality & Stability

- Added tests for Source Metadata Import Report serialization  
- Added tests for legacy import summary compatibility  
- Added tests for file import report integration  
- Added tests for skipped dataset reporting  
- Added tests for created, changed, unchanged and removed column outcomes  
- Added tests for primary-key detection visibility  
- Verified SourceDataset and system-level import result rendering in the UI
 
---

## [2.8.0] - 2026-06-10

This release adds **Architecture Review Briefing**:  
a compact, deterministic reviewer guidance panel inside Architecture Control.

The briefing helps reviewers understand the selected architecture scope before approval or execution.  
It summarizes review state, change volume, policy attention, destructive or blocking signals,  
execution readiness and suggested reviewer focus without replacing Architecture Change Reports,  
Approval Artifacts or Execution Preview.

---

### ✨ Added

#### Architecture Review Briefing

- Added deterministic Architecture Review Briefing service for Architecture Control scopes  
- Added compact briefing sections for:  
    - scope summary  
    - review state  
    - change summary  
    - policy attention  
    - destructive and blocking attention  
    - execution readiness  
    - suggested reviewer focus  
- Added support for all-dataset, schema and TargetDataset Architecture Control scopes  
- Added execution-preview awareness for dependency mode, gate state and execution step count  
- Added compact Architecture Control UI panel with attention badges and on-demand detail expansion  
- Added reviewer-focus guidance derived from existing report, review status and execution preview signals

---

### 🔄 Improved

#### Architecture Control Review UX

- Improved Architecture Control readability by surfacing the most important review signals near the selected scope  
- Kept detailed briefing sections collapsed by default to avoid duplicating existing Review Status, Report and Execution Preview details  
- Kept suggested reviewer focus visible because it is the decision-oriented value of the briefing  
- Kept the briefing inside the existing Architecture Control page instead of adding a separate workflow  
- Added defensive view integration so an unavailable briefing does not break Architecture Control rendering

#### Documentation

- Updated Architecture Control Plane documentation with Architecture Review Briefing behavior  
- Updated Architecture Overview with Review Briefing in the Architecture Control runtime flow  
- Updated documentation index with Review Briefing positioning  
- Updated README Architecture Control summary with compact reviewer guidance  
- Updated CHANGELOG with v2.8.0 release notes

---

### 🔒 Governance & Determinism

- Architecture Review Briefing is read-only  
- Architecture Review Briefing does not mutate metadata  
- Architecture Review Briefing does not create Approval Artifacts  
- Architecture Review Briefing does not run approval checks  
- Architecture Review Briefing does not execute loads  
- Architecture Review Briefing does not change approval or execution logic  
- Architecture Review Briefing does not introduce AI-based inference  
- Architecture Review Briefing does not add database models or migrations  
- Architecture Change Reports, Approval Artifacts, Execution Preview and Architecture Execution Records remain authoritative artifacts

---

### 🧪 Quality & Stability

- Added tests for no-change Architecture Control scopes  
- Added tests for pending review briefing state  
- Added tests for approved matching changes  
- Added tests for blocked policy attention  
- Added tests for destructive migration attention  
- Added tests for approval drift reviewer focus  
- Added tests for TargetDataset, schema and all-dataset scope summaries  
- Added tests for read-only service behavior  
- Added tests for Architecture Control view briefing context  
- Added regression coverage for optional briefing rendering with lightweight view test doubles  
- Verified compact briefing display and expandable detail sections in the UI
 
---

## [2.7.0] - 2026-06-04

This release adds **Metadata Naming Guidance**:  
deterministic, project-specific naming assistance for TargetColumn modeling.

Naming Guidance helps modelers reuse already established column naming decisions directly while editing columns.  
It learns from existing metadata mappings without AI, without a global dictionary and without enforcing rules.

---

### ✨ Added

#### Metadata Naming Guidance

- Added deterministic Naming Guidance service for TargetColumn modeling  
- Added guidance states for:  
    - new  
    - suggested  
    - conflict  
- Added usage counts for known target names  
- Added transparent example datasets behind each known target name  
- Added dominant-name detection for clear naming patterns  
- Added TargetColumn inline rename integration  
- Added one-click recommendation button that applies the recommended column name through the existing rename flow  
- Added advisory guidance display for both valid rename previews and validation feedback

---

### 🔄 Improved

#### TargetColumn Modeling UX

- Improved TargetColumn rename workflow with immediate, context-aware naming suggestions  
- Reused existing HTMX rename preview instead of adding a separate workflow  
- Kept recommendation application aligned with existing validation, collision checks, audit fields and former-name handling  
- Reduced manual lookup effort when standardizing cryptic source-system field names such as SAP-style technical columns  
- Kept guidance compact by hiding empty new-state guidance in the UI

#### Guidance Scope

- Naming Guidance now uses the immediate input-column name for the current modeling step  
- Direct source inputs use the SourceColumn name  
- Upstream target inputs use the immediate upstream TargetColumn name  
- Guidance is scoped to rawcore and bizcore technical naming decisions  
- Serving-layer friendly names are excluded from technical naming suggestions  
- Historized rawcore datasets are excluded because they are synchronized from their base datasets  
- Ambiguous multi-input column cases are excluded from recommendation evidence

---

### 🔒 Governance & Determinism

- Naming Guidance is advisory only  
- Naming Guidance does not enforce column names  
- Naming Guidance does not mutate metadata while building suggestions  
- Naming Guidance does not introduce AI-based inference  
- Naming Guidance does not use a global dictionary or glossary workflow  
- Naming Guidance does not add new database models or migrations  
- Existing TargetColumn rename validation remains authoritative  
- Recommendations are derived transparently from existing TargetColumnInput metadata

---

### 🧪 Quality & Stability

- Added tests for empty guidance  
- Added tests for single known naming usage  
- Added tests for conflicting known target names  
- Added tests for dominant naming usage  
- Added tests for immediate upstream input-name behavior  
- Added tests for excluding current TargetColumn usage  
- Added tests for excluding historized datasets  
- Added tests for excluding serving-layer friendly names  
- Added tests for excluding ambiguous multi-input evidence  
- Added tests for read-only service behavior  
- Added tests for inline UI guidance context  
- Verified TargetColumn rename preview, recommendation button and existing apply/cancel flow in the UI

---

## [2.6.0] - 2026-06-04

This release extends the **Architecture Catalog** with **Architecture Catalog Portfolio**:  
a read-only executive lens across metadata-defined executable architecture.

The Portfolio summarizes architecture posture across readiness, ownership, contracts, health,  
review state, execution evidence and layer distribution without becoming a second Insights page  
or an Architecture Control workflow.

---

### ✨ Added

#### Architecture Catalog Portfolio

- Added dedicated Architecture Catalog Portfolio page  
- Added portfolio-level KPI cards for:  
    - active datasets  
    - ownership coverage  
    - contract coverage  
    - metadata health clearance  
    - Architecture Control review clearance  
    - Architecture Execution Record evidence coverage  
- Added Data Product readiness distribution to the Portfolio page  
- Added Portfolio attention areas for aggregated governance and quality posture  
- Added layer portfolio overview with ownership, contract, health, execution evidence and custom query signals  
- Added drill-down links from actionable Portfolio KPIs to filtered Catalog worklists  
- Added drill-down links from layer rows to filtered Catalog layer views  
- Added clickable Portfolio attention areas for actionable worklists  
- Added full Catalog reset link for Portfolio worklists and other active Catalog filters  
- Added direct Portfolio navigation across Catalog, Data Products, Insights, Map and Catalog Detail pages

#### Catalog Worklist Filters

- Added Portfolio-oriented Catalog signal filters for:  
    - missing ownership  
    - missing contract columns  
    - metadata health attention  
    - Architecture Control review attention  
    - missing Architecture Execution Record evidence  
    - inactive datasets with active downstream consumers  
- Added active filter context for Catalog worklist views  
- Added full Catalog reset handling for schema and signal-driven filtered Catalog views

---

### 🔄 Improved

#### Architecture Catalog Navigation

- Improved Catalog-to-Portfolio navigation consistency across Catalog lenses  
- Kept Portfolio actions as filtered read-only worklists instead of operational forms  
- Kept completed KPI cards non-actionable to avoid empty drill-downs  
- Improved Portfolio metric readability and removed redundant general navigation buttons  
- Improved Portfolio rendering compatibility with editor HTML validation  
- Optimized Portfolio review-state aggregation by reusing shared Architecture Control review context

#### Documentation

- Added Architecture Catalog Portfolio documentation  
- Updated Architecture Catalog documentation with Portfolio positioning and worklist drill-downs  
- Updated Architecture Catalog Data Products documentation with Portfolio relationship  
- Updated documentation index with Architecture Catalog Portfolio  
- Updated README Architecture Catalog summary with Portfolio posture and worklist visibility  
- Updated CHANGELOG with v2.6.0 release notes

---

### 🔒 Governance & Determinism

- Architecture Catalog Portfolio remains read-only  
- Portfolio does not mutate metadata  
- Portfolio does not create Approval Artifacts  
- Portfolio does not run approval checks  
- Portfolio does not execute loads  
- Portfolio drill-downs use existing Catalog, Data Product and Architecture Control boundaries  
- Architecture Control remains responsible for approval state, execution preview, controlled execution,  
execution records, execution history and retention workflows

---

### 🧪 Quality & Stability

- Added tests for Architecture Catalog Portfolio context construction  
- Added tests for Portfolio metric aggregation  
- Added tests for Data Product readiness distribution inside Portfolio  
- Added tests for layer portfolio summaries and drill-down URLs  
- Added tests for Portfolio Catalog signal worklist filters  
- Added tests for active Catalog filter reset context  
- Added tests for Architecture Catalog Portfolio view rendering  
- Verified Portfolio, Catalog worklists, Data Products, Insights, Map and Catalog Detail navigation in the UI  
- Verified Portfolio page load performance after shared review-context optimization

---

## [2.5.0] - 2026-06-02

This release extends the **Architecture Catalog** with **Catalog Data Products**:  
a read-only consumer-readiness perspective for serving-layer datasets.

The Catalog now connects discoverable architecture with trusted consumption by showing which serving assets  
have the ownership, health, lineage, contracts, review state and execution evidence needed for responsible use.

---

### ✨ Added

#### Architecture Catalog Data Products

- Added dedicated Architecture Catalog Data Products page  
- Added serving-layer Data Product candidate selection  
- Added transparent consumer-readiness groups:  
    - Consumption-ready  
    - Review recommended  
    - Not consumption-ready  
- Added Data Product cards with dataset key, layer, description and readiness state  
- Added ownership, contract column, upstream, downstream, health and query logic summaries  
- Added Architecture Control review state summary per Data Product  
- Added latest Architecture Execution Record evidence summary per Data Product  
- Added expandable readiness signals for each Data Product  
- Added filters for search, consumer layer, readiness group and lifecycle status  
- Added direct links from Data Product cards to Catalog detail, lineage, query contract and Architecture Control

#### Catalog Detail Consumer Readiness

- Added dataset-specific Consumer Readiness card to Catalog detail pages  
- Added readiness signal summaries for selected TargetDataset scopes  
- Added non-consumer layer explanations for datasets outside the serving layer  
- Added direct navigation between Catalog Detail and Catalog Data Products

---

### 🔄 Improved

#### Architecture Catalog Navigation

- Added direct navigation from Architecture Catalog to Catalog Data Products  
- Added direct navigation from Catalog Insights to Catalog Data Products  
- Added direct navigation from Catalog Map to Catalog Data Products  
- Added explicit Catalog and Back navigation on Catalog Detail pages  
- Improved Data Product card action labels for clearer dataset-level navigation  
- Kept Data Products as a Catalog perspective instead of a separate workflow

#### Documentation

- Added Architecture Catalog Data Products documentation  
- Updated Architecture Catalog documentation with Data Product readiness  
- Updated Architecture Overview with consumer-readiness positioning  
- Updated documentation index and MkDocs navigation  
- Updated README Architecture Catalog summary with Catalog Data Products  
- Updated Getting Started with Catalog Data Products entry point

---

### 🔒 Governance & Determinism

- Catalog Data Products derive readiness from existing architecture metadata  
- Readiness groups are transparent and signal-based rather than hidden scores  
- Serving remains the consumer-facing layer; bizcore remains the business logic implementation layer  
- Architecture Catalog remains read-only  
- Architecture Control remains responsible for approval, execution, execution records,  
execution history and retention workflows

---

### 🧪 Quality & Stability

- Added tests for Catalog Data Products context construction  
- Added tests for serving-layer candidate selection  
- Added tests for readiness group counts  
- Added tests for ownership, contract, lineage, review and execution evidence signals  
- Added tests for excluding bizcore datasets from the Data Products list  
- Added tests for Catalog Detail Consumer Readiness context  
- Added tests for Catalog Data Products view rendering  
- Verified Catalog Data Products, Catalog Detail, Lineage, Query Contract and Architecture Control navigation in the UI

---

## [2.4.0] - 2026-05-31

This release extends the **Architecture Catalog** with **Catalog Maps**:  
a read-only architecture lens across layers and direct TargetDataset dependencies.

The Catalog now shows not only which datasets exist and where architecture signals need attention,  
but also how metadata-defined architecture is structured across populated schemas and layer transitions.

---

### ✨ Added

#### Architecture Catalog Maps

- Added dedicated Architecture Catalog Map page  
- Added layer summary cards grouped by schema / layer  
- Added canonical layer ordering for raw, stage, rawcore, bizcore and serving  
- Added compact dataset examples per layer with Catalog detail and lineage links  
- Added expandable layer dataset lists for complete layer visibility  
- Added layer flow overview across populated architecture layers  
- Added direct dependency counts between adjacent populated layers  
- Added source-to-target layer dependency matrix  
- Added layer transition groups for direct TargetDataset dependencies  
- Added expandable transition examples with source, target and lineage links

---

### 🔄 Improved

#### Architecture Catalog Navigation

- Added direct navigation from Architecture Catalog to Catalog Map  
- Added direct navigation from Catalog Detail to Catalog Map  
- Added direct navigation between Catalog Map, Catalog Insights and Catalog workspace  
- Kept Catalog Map separate from Catalog Insights to preserve clear discovery responsibilities  
- Improved Catalog Map readability with compact cards, matrix layout and expandable details

#### Theme and Documentation Styling

- Centralized custom UI color values in the main elevata theme stylesheet  
- Replaced scattered custom color literals with theme variables  
- Separated the MkDocs Material documentation theme from the Django application theme  
- Renamed the documentation-specific theme file to make the styling boundary explicit  
- Preserved the visual appearance of the documentation while improving theme maintainability

#### Documentation

- Updated Architecture Catalog documentation with Catalog Maps  
- Updated Architecture Overview with Catalog Map positioning  
- Updated documentation index with Catalog Maps  
- Updated README Architecture Catalog summary with layer map and dependency matrix visibility

---

### 🔒 Governance & Determinism

- Architecture Catalog Maps remain read-only  
- Catalog Maps do not mutate metadata  
- Catalog Maps do not create Approval Artifacts  
- Catalog Maps do not run approval checks  
- Catalog Maps do not execute loads  
- Catalog Maps use direct TargetDataset dependencies without replacing dedicated lineage views  
- Architecture Control remains responsible for approval, execution, execution records,  
execution history and retention workflows

---

### 🧪 Quality & Stability

- Added tests for Catalog Map context construction  
- Added tests for layer summary grouping and layer ordering  
- Added tests for layer flow overview construction  
- Added tests for dependency matrix counts  
- Added tests for collapsed and expanded layer dataset handling  
- Added tests for collapsed and expanded transition example handling  
- Added tests for Catalog Map view rendering and navigation links  
- Verified Catalog Map, Catalog, Catalog Insights, Catalog Detail and Lineage navigation in the UI  
- Verified the documentation theme separation with MkDocs rendering

---

## [2.3.0] - 2026-05-30

This release extends the **Architecture Catalog** with **Catalog Insights**:  
read-only architecture quality and governance signals for metadata-defined executable architecture.

The Catalog now highlights where ownership, metadata health, lineage usage, review state  
and execution evidence need attention without turning discovery into editing or execution.

---

### ✨ Added

#### Architecture Catalog Insights

- Added dedicated Architecture Catalog Insights page  
- Added read-only insight cards for:  
    - datasets without assigned ownership  
    - datasets with metadata health issues  
    - datasets with metadata health warnings  
    - datasets with custom query logic  
    - datasets without downstream consumers  
    - inactive datasets with downstream consumers  
    - datasets without Architecture Execution Record evidence  
- Added expandable insight card lists for complete dataset visibility  
- Added direct links from insight items to Catalog detail pages

#### Catalog Detail Insight Signals

- Added dataset-specific Catalog insight signals to Catalog detail pages  
- Added signals for missing ownership, metadata health findings, custom query logic,  
missing downstream consumers, inactive consumer relationships and missing execution evidence  
- Added relevant Architecture Control review states as dataset-specific Catalog detail signals  
- Added read-only Architecture Control review status summary on Catalog detail pages  
- Added review state, review message, report fingerprint, change indicator and policy status  
to Catalog detail context  
- Added direct Catalog detail link to the selected Architecture Control scope

---

### 🔄 Improved

#### Architecture Catalog Navigation

- Added direct navigation from Architecture Catalog to Catalog Insights  
- Added direct navigation from Catalog Detail to Catalog Insights  
- Improved Catalog Detail action button ordering  
- Improved expanded insight list labels with clear show/collapse behavior  
- Kept review status evaluation off the main Catalog list to preserve lightweight discovery

#### Documentation

- Updated Architecture Catalog documentation with Catalog Insights, dataset-specific insight signals  
and review status summaries  
- Updated Architecture Overview with Catalog Insights positioning  
- Updated README Architecture Catalog summary with quality and governance signals

---

### 🔒 Governance & Determinism

- Architecture Catalog Insights remain read-only  
- Catalog Insights do not mutate metadata  
- Catalog Insights do not create Approval Artifacts  
- Catalog Insights do not run approval checks  
- Catalog Insights do not execute loads  
- Catalog Insights do not delete execution records  
- Architecture Control remains responsible for approval, execution, execution records,  
execution history and retention workflows  
- Review status is surfaced in Catalog detail context without moving Architecture Control  
responsibilities into the Catalog

---

### 🧪 Quality & Stability

- Added tests for Catalog Insights context construction  
- Added tests for ownership, health, query logic and execution evidence signal grouping  
- Added tests for collapsed and expanded insight item handling  
- Added tests for Catalog Insights view rendering  
- Added tests for Catalog Detail dataset-specific insight signals  
- Added tests for Catalog Detail review status summaries  
- Added tests for Catalog Detail navigation additions  
- Verified Catalog, Insights page, Catalog Detail, Review Status, Architecture Control links  
and expandable insight lists in the UI

---

## [2.2.0] - 2026-05-29

This release introduces the **Architecture Catalog** as a read-only discovery layer  
for metadata-defined executable architecture.

The Catalog makes architecture easier to find, inspect and navigate without turning discovery  
into editing or execution.

---

### ✨ Added

#### Architecture Catalog

- Added central Architecture Catalog workspace to the main navigation  
- Added read-only TargetDataset discovery with search and filters for:  
    - dataset key and name  
    - schema / layer  
    - owner  
    - active status  
    - system-managed status  
    - materialization type  
    - incremental strategy  
    - query logic  
- Added compact Catalog dataset summaries with:  
    - dataset key and description  
    - architecture layer  
    - effective materialization type  
    - incremental strategy  
    - ownership  
    - upstream and downstream counts  
    - metadata health  
    - query logic  
    - direct links to Details, Lineage, Query Contract and Architecture Control  
- Added Architecture Catalog detail view for TargetDataset scopes  
- Added Catalog detail cards for architecture summary, ownership, health and execution evidence  
- Added Catalog detail upstream input and downstream consumer sections  
- Added Catalog detail column contract table  
- Added latest Architecture Execution Record summary on Catalog detail pages  
- Added direct link from Catalog detail to Architecture Control execution history  

---

### 🔄 Improved

#### Architecture Navigation

- Added context-aware back navigation across Catalog, Details, Lineage, Query Contract and Architecture Control  
- Kept Catalog navigation read-only and separate from execution actions  
- Improved button group consistency in Catalog action links  
- Added Query Contract back navigation for Catalog-driven workflows  

#### Documentation

- Added Architecture Catalog documentation  
- Added Architecture Catalog to documentation index and MkDocs navigation  
- Updated README with a compact Architecture Catalog summary  
- Updated Architecture Overview with Architecture Catalog positioning  
- Updated Architecture Control Plane documentation with Catalog boundary semantics  
- Updated Getting Started with Architecture Catalog UI entry point  
- Updated Platform Strategy with Architecture Catalog as discovery layer for the Architecture Runtime contract  

---

### 🔒 Governance & Determinism

- Architecture Catalog does not mutate metadata  
- Architecture Catalog does not execute loads  
- Architecture Control remains responsible for approval, execution, execution history and retention workflows  
- Latest execution evidence references do not change stored Architecture Execution Record payloads  
- Catalog views preserve the separation between discovery, control and execution  

---

### 🧪 Quality & Stability

- Added tests for Architecture Catalog context construction and filtering  
- Added tests for custom query logic filtering  
- Added tests for Catalog detail context construction  
- Added tests for Catalog detail execution evidence summary  
- Added view tests for Catalog workspace and Catalog detail rendering  
- Verified Catalog, Detail, Lineage, Query Contract and Architecture Control navigation in the UI  

---

## [2.1.0] - 2026-05-27

This release makes Architecture Execution Records operable from Architecture Control.

Controlled UI executions are retained as deterministic audit artifacts and can be reviewed, filtered,  
downloaded and governed through retention cleanup without changing the controlled execution path.

---

### ✨ Added

#### Architecture Execution History

- Added Architecture Execution Record history to Architecture Control  
- Added scope-aware history filtering for the selected Architecture Control scope  
- Added option to inspect records across all scopes  
- Added history filters for execution status, dependency mode and execution date range  
- Added stored execution record summaries with execution identifier, timestamps, duration,  
status, scope, dependency mode and fingerprint references  
- Added Architecture Execution Record detail view  
- Added Architecture Execution Record JSON download  
- Added retention cleanup for stored execution records older than selected retention windows

---

### 🔄 Improved

#### Architecture Control Workflow

- Extended Architecture Control from last-result inspection to persisted execution history inspection  
- Kept execution history read-only except for explicit retention cleanup  
- Kept controlled execution, approval checks and Architecture Guard enforcement unchanged  
- Preserved Architecture Execution Records as compact file-backed audit artifacts

#### Documentation

- Updated Architecture Control Plane documentation with execution history and retention behavior  
- Updated Load Execution Architecture with Architecture Execution Record history semantics  
- Updated Determinism & Execution Semantics with history and retention fingerprint behavior  
- Updated README Architecture Control summary

---

### 🔒 Governance & Determinism

- Architecture Execution Record fingerprints remain derived from canonical record payloads  
- History filtering and detail rendering do not change stored execution record payloads  
- Retention cleanup removes stored execution record artifacts only  
- Approval Artifacts, Architecture State artifacts, load run logs and load run snapshots  
are not affected by execution record retention

---

### 🧪 Quality & Stability

- Added tests for execution record loading, summary construction, filtering and retention cleanup  
- Added tests for Architecture Control history rendering  
- Added tests for execution record detail rendering and JSON download  
- Added tests for retention action messaging

---

## [2.0.0] - 2026-05-25

This major release introduces **Architecture Control** for controlled, gated and auditable  
execution of metadata-defined architecture from the UI.

Architecture review workflows now extend into execution: users can select controlled scopes,  
inspect the Execution Preview, verify approval readiness, run controlled loads, inspect captured  
output, and retain deterministic Architecture Execution Records.

The focus is closing the Architecture Runtime loop:

```text
Architecture State
  ↓
Architecture Change Report
  ↓
Architecture Approval Artifact
  ↓
Execution Preview
  ↓
Controlled Execution
  ↓
Architecture Execution Record
```

---

### ✨ Added

#### Architecture Control UI

- Added a central Architecture Control workspace to the main navigation  
- Added settings-driven support for non-model main menu entries  
- Added scope-aware Architecture Control for:  
    - all active target datasets  
    - schema scopes  
    - TargetDataset scopes  
    - TargetDataset scopes with target-only execution  
- Kept TargetDataset-level Architecture Control as a contextual deep link  
- Added guided scope selection with automatic TargetDataset and schema context handling  
- Added UI controls for dependency mode selection where applicable  
- Added full-scope visibility for selected roots and execution order through expandable sections

#### Execution Preview

- Added Architecture Control Execution Preview for selected scopes  
- Added root dataset and execution-order inspection before execution  
- Added execution gate status for:  
    - ready  
    - ready without architecture changes  
    - pending approval  
    - blocked by policy  
- Added dependency mode visibility:  
    - `with_dependencies`  
    - `target_only`  
- Added deterministic preview fingerprints

#### Controlled Execution

- Added controlled UI execution for approved or no-change Architecture Control scopes  
- Added constrained `elevata_load --execute` invocation from Architecture Control  
- Added enforced Architecture Guard mode for UI-driven controlled execution  
- Added target-only execution for TargetDataset scopes  
- Added execution confirmation and in-page running feedback  
- Added captured command output and error output in the UI  
- Added expandable full captured output and error details  
- Added session-bound execution result display after redirect  
- Prevented stale execution results from appearing on unrelated or fresh Architecture Control page loads

#### Architecture Execution Records

- Added deterministic Architecture Execution Records for controlled Architecture Control executions  
- Added file-backed Architecture Execution Record store  
- Added configurable execution record directory via `ELEVATA_ARCH_EXECUTION_DIR`  
- Added execution record JSON payload with:  
    - execution identifier  
    - operator  
    - timestamps and duration  
    - execution status and message  
    - Architecture Control scope  
    - dependency mode  
    - report fingerprint  
    - approval identifier  
    - preview fingerprint  
    - command invocation metadata  
    - output and error tails  
    - output and error line counts  
    - deterministic record fingerprint  
- Added UI display of stored execution record path and fingerprint

#### Execution Output Usability

- Added command output tail display for controlled executions  
- Added expandable captured output and captured error sections  
- Added ANSI escape sequence cleanup for captured command output  
- Added show/hide labels for expandable details sections  
- Added clearer controlled execution failure messages with details retained in the execution result panel

---

### 🔄 Improved

#### Architecture Control Workflow

- Renamed the UI concept from Architecture Operations to Architecture Control  
- Promoted Architecture Control from TargetDataset-only workflow to a central scope-aware workspace  
- Preserved TargetDataset deep links for direct contextual entry  
- Improved scope handling to avoid accidental all-dataset execution when a TargetDataset is selected  
- Improved schema and TargetDataset selection behavior for clearer user expectations  
- Kept target-only execution restricted to TargetDataset scopes  
- Kept schema and all-dataset execution lineage-aware by default

#### Execution Semantics

- Treated no-change Architecture Control scopes as executable without requiring an Approval Artifact  
- Kept controlled execution blocked when reports contain blocking policy decisions  
- Kept controlled execution blocked when architecture changes require but do not have matching approval  
- Ensured UI-driven execution uses the same load runner and guardrails as CLI execution  
- Kept arbitrary diagnostic and debug flags CLI-only

#### Shadow Compare and Guard Diagnostics

- Refined schema-operation shadow compare for Full Refresh scopes  
- Suppressed non-actionable Full Refresh `ALTER_COLUMN` expectations  
- Restricted expected schema-operation comparison to materialization-relevant datasets  
- Improved shadow-compare output with comparable action counts and suppression counters

#### Documentation

- Updated Architecture Control Plane documentation for controlled execution and execution records  
- Updated Architecture Overview for the Architecture Control runtime flow  
- Updated Load Execution Architecture with Architecture Control execution semantics  
- Updated Schema Evolution documentation with controlled execution and audit records  
- Updated Determinism & Execution Semantics with Architecture Execution Record fingerprints  
- Updated Getting Started with Architecture Control workflow and execution record configuration  
- Updated Platform Strategy to reflect Architecture Runtime, Architecture Control and audit evidence  
- Updated README Architecture Control summary  
- Added `.env.example` entry for `ELEVATA_ARCH_EXECUTION_DIR`

---

### 🔒 Governance & Determinism

- Architecture Control execution remains gated by report state, approval matching and policy decisions  
- Controlled execution does not bypass preflight validation, materialization policy checks or Architecture Guard enforcement  
- Architecture Execution Records provide deterministic audit references for controlled UI executions  
- Execution Preview fingerprints include scope, dependency mode, report fingerprint, approval state and execution dataset set  
- Approval Artifacts remain bound to exact Architecture Change Report fingerprints  
- Target-only execution is explicit, visible and restricted to TargetDataset scopes  
- Full captured output remains inspectable in the UI while persisted execution records retain compact audit payloads

---

### 🧪 Quality & Stability

- Added tests for Architecture Execution Preview gate behavior and target-only execution  
- Added tests for controlled Architecture Control execution command construction  
- Added tests for controlled execution failure handling and Architecture Guard enforcement  
- Added tests for Architecture Execution Record creation, rendering and storage  
- Added tests for scope-aware menu configuration  
- Added tests for Architecture Control view execution feedback and session-bound result handling  
- Updated shadow-compare tests for Full Refresh suppression behavior  
- Verified controlled execution through the UI for TargetDataset and target-only scopes

---

## [1.9.0] - 2026-05-21

This release introduces the **Architecture Operations UI** for the Architecture Control Plane.

Architecture review workflows can now be operated directly from the TargetDataset Architecture Review page:  
reports can be generated, inspected, downloaded, approved, checked and refreshed from the UI.

The focus is making architecture review workflows actionable while keeping load execution protected by  
the load runner and materialization guardrails.

---

### ✨ Added

#### Architecture Operations UI

- Added an Architecture Operations panel to the TargetDataset Architecture Review page  
- Added UI action to show the scoped Architecture Change Report  
- Added UI action to download the scoped Architecture Change Report as deterministic JSON  
- Added UI action to create an Architecture Approval Artifact for the current report fingerprint  
- Added UI action to check the stored Approval Artifact against the current scoped report  
- Added UI action to refresh the Architecture Review Status  
- Added Django message feedback for approval creation and approval checks

#### Architecture Operations Service

- Added shared Architecture Operations service functions for TargetDataset-scoped artifact workflows  
- Added shared context construction for scoped Architecture Change Report, Review Status and Approval Store  
- Added guarded Approval Artifact creation from the current scoped report  
- Added stored Approval Artifact checks with explicit valid, missing, drift and invalid outcomes  
- Added deterministic report rendering paths for UI preview and JSON download

### 🔄 Improved

#### Architecture Review Workflow

- Replaced manual command guidance on the Architecture Review page with UI-driven artifact actions  
- Kept Approval Artifact creation disabled when the report has no changes, is blocked by policy,  
or already has a matching approval  
- Kept policy decisions authoritative: approval does not override blocked execution policy  
- Surfaced approval check results through standard UI messages  
- Added global message rendering for success, warning and error feedback

#### Documentation

- Documented the Architecture Operations UI in the Architecture Control Plane guide  
- Documented server-side artifact storage semantics for shared deployments  
- Clarified persistent storage requirements for `ELEVATA_ARCH_STATE_DIR` and `ELEVATA_ARCH_APPROVAL_DIR`  
- Updated README Architecture Control Plane summary with UI-operable review workflows

### 🔒 Governance & Determinism

- Architecture Operations UI actions operate on deterministic Architecture Control Plane artifacts  
- Report preview and JSON download use the scoped Architecture Change Report contract  
- Approval Artifacts remain bound to exact report fingerprints  
- Approval checks compare stored artifacts against the current scoped report  
- Load execution remains separated from Architecture Operations UI actions

### 🧪 Quality & Stability

- Added tests for Architecture Operations service context construction  
- Added tests for report rendering through Architecture Operations service functions  
- Added tests for guarded Approval Artifact creation  
- Added tests for approval check outcomes  
- Added tests for Architecture Operations UI view behavior and feedback messages

---

## [1.8.0] - 2026-05-19

This release introduces **Architecture Review Decisions** for the Architecture Control Plane.

Architecture Change Reports can now be approved as deterministic Approval Artifacts, verified against their  
original report fingerprint, and surfaced through a read-only Architecture Review Status UI.

The focus is closing the architecture review loop: planned architecture changes become reviewable,  
approvable, verifiable, and visible as stable artifacts.

---

### ✨ Added

#### Architecture Approval Artifacts

- Added deterministic Architecture Approval Artifacts for Architecture Change Reports  
- Added approval artifact fingerprints derived from canonical JSON payloads  
- Added stable approval identifiers derived from approval artifact fingerprints  
- Added approval report references containing:  
    - report fingerprint  
    - report scope  
    - report state  
    - report summary  
    - policy block status  
- Added review decision metadata:  
    - decision  
    - reviewer  
    - decision timestamp  
    - review note

#### Architecture Review Commands

- Added `elevata_approve` for creating Architecture Approval Artifacts  
- Added `elevata_approval_check` for verifying approval artifacts against Architecture Change Reports  
- Added `--store` support for storing approval artifacts by report fingerprint  
- Added configurable approval artifact directory via `ELEVATA_ARCH_APPROVAL_DIR`  
- Added fingerprint-only output for Approval Artifacts

#### Architecture Review Status UI

- Added read-only Architecture Review Status page for TargetDataset architecture scopes  
- Added visual review states for:  
    - approved and matching  
    - pending review  
    - approval drift  
    - blocked by policy  
    - no architecture changes  
    - invalid approval artifact  
- Added visible report fingerprint, approval identifier, approval fingerprint, policy status,  
  architecture scope, state fingerprints and change summary  
- Added TargetDataset navigation entry for Architecture Review Status

### 🔒 Governance & Determinism

- Approval artifacts bind review decisions to exact Architecture Change Report fingerprints  
- Approval checks fail when the artifact payload, approval identifier, report reference or review decision is invalid  
- Approval does not override policy decisions or load execution guardrails  
- The Architecture Control Plane remains read-only; load execution remains protected by preflight and materialization checks

### 🧪 Quality & Stability

- Added tests for Architecture Approval Artifact determinism  
- Added tests for approval artifact store behavior and fingerprint validation  
- Added tests for `elevata_approve --store`  
- Added tests for `elevata_approval_check`  
- Added tests for Architecture Review Status states

---

## [1.7.1] - 2026-05-18

This patch release hardens the **Architecture Control Plane** report contract by making scoped  
architecture reports precise, self-contained, and fingerprint-stable for review and CI workflows.

The focus is reliable scope semantics for deterministic architecture artifacts.

---

### 🔄 Improved

#### Architecture Control Plane

- Hardened scoped Architecture Change Reports so report payloads only contain  
  dataset changes, column changes, MigrationPlan actions, policy decisions and  
  summary counts that belong to the selected scope  
- Refined scope mode semantics:  
    - `elevata_plan --all` reports `all`  
    - schema-scoped and dataset-scoped reports report `scoped`  
    - scoped promotion reports expose the selected target dataset in the  
      embedded Architecture Change Report  
- Strengthened report fingerprints so scoped report fingerprints represent the  
  selected architecture scope precisely

### 📘 Documentation

- Documented Architecture Control Plane scope semantics for `all` and `scoped` 
  report modes  
- Clarified that `--schema` is optional for unique dataset names and useful for  
  explicit schema-scoped CI workflows  
- Added operational smoke checks for Architecture State, Architecture Change  
  Reports, Architecture Promotion Reports and no-change exit behavior

### 🧪 Quality & Stability

- Added tests for scoped report payload consistency  
- Added tests for `elevata_plan` scope mode behavior  
- Added tests for `elevata_promote` scoped target metadata

---

## [1.7.0] - 2026-05-16

This release introduces the **Architecture Control Plane** for deterministic architecture review, comparison,  
and promotion workflows.

Architecture changes can now be rendered as explicit, policy-aware reports before execution.  
Architecture State, Change Reports, and Promotion Reports expose stable fingerprints, making architecture   
intent reviewable, comparable, and CI-friendly.

The focus is controlled lifecycle management for executable architecture.

---

### ✨ Added

#### Architecture Control Plane

- Added deterministic Architecture Change Reports via `elevata_plan`  
- Added Architecture State export via `elevata_state`  
- Added Architecture Promotion Reports via `elevata_promote`  
- Added JSON and text rendering for architecture reports  
- Added deterministic fingerprints for:  
    - Architecture State  
    - Architecture Change Report  
    - Architecture Promotion Report  
- Added CI-oriented exit policies:  
    - `--fail-on-changes`  
    - `--fail-on-blocked`  
    - `--fail-on-destructive`  
- Added explicit baseline support for `elevata_plan` via `--previous-state`  
- Added configurable architecture state directory via `ELEVATA_ARCH_STATE_DIR`

#### Architecture State Artifacts

- Added public Architecture State file serialization and deserialization helpers  
- Added explicit Architecture State export for review, CI, and promotion workflows  
- Added fingerprint-only output for Architecture State inspection  
- Added controlled baseline handling for persisted runtime architecture state  

#### Shared Architecture Services

- Added shared architecture scope resolution for report and load paths  
- Added shared policy decision reporting for MigrationPlan actions  
- Added shared schema-operation shadow compare utilities  
- Added deterministic report rendering helpers for architecture and promotion reports

---

### 🔄 Improved

- `elevata_load` uses shared architecture scope resolution while preserving execution guard behavior  
- `elevata_load` uses shared shadow-compare utilities while preserving enforce-mode blocking semantics  
- Architecture state persistence keeps a stable runtime baseline while allowing explicit state artifact exports  
- Architecture report generation uses the same semantic path as execution:  
  Architecture State → Architecture Diff → MigrationPlan → Policy Decisions  
- README highlights Architecture Control Plane capabilities in a compact form  
- Documentation now describes Architecture State, Change Reports, Promotion Reports, CI exit policies,  
and deterministic fingerprints

---

### 🔒 Governance & Determinism

- Architecture changes can be reviewed as deterministic artifacts before execution  
- Promotion comparisons use stable Architecture State files as input artifacts  
- Destructive schema actions are surfaced through explicit policy decisions  
- Report fingerprints provide stable references for review, CI, and promotion workflows  
- Load execution remains protected by its own preflight and guard checks

---

### 🧪 Quality & Stability

- Added tests for Architecture Change Reports
- Added tests for Architecture Promotion Reports
- Added tests for Architecture State export and file roundtrips
- Added tests for report fingerprints and deterministic JSON output
- Added tests for scope resolution and `_hist` scope expansion
- Added tests for shared shadow-compare behavior
- Added CLI tests for `elevata_state`, `elevata_plan`, and `elevata_promote`
- Preserved Architecture Guard enforce-mode behavior in `elevata_load`

---

## [1.6.0] - 2026-05-14

This release completes the cutover to **Architecture-driven Materialization**.  
Schema evolution is now driven by Architecture State → Architecture Diff → MigrationPlan,  
and the resulting materialization steps are applied deterministically before load SQL execution.

The focus is turning architecture from an observable plan into an executable runtime contract.

---

### ✨ Added

#### Architecture-driven Materialization

- MigrationPlan is now the authoritative schema-evolution intent for materialization  
- Materialization steps are derived from architecture changes instead of planner-side drift heuristics  
- Deterministic execution path for:  
    - dataset renames  
    - column renames  
    - additive schema changes  
    - type evolution  
    - rebuild-based remediation where required  
- Shadow compare can enforce consistency between architecture intent and planned materialization DDL

#### Controlled rebuild execution

- Deterministic rebuild flows can now execute all required steps when policy allows them:  
    - temporary table cleanup  
    - temporary table creation  
    - backfill via `INSERT ... SELECT`  
    - source table replacement  
    - final table rename  
- Rebuild steps remain policy-aware and are only applied after preflight validation

---

### 🔄 Improved

- `elevata_load` now requires Architecture MigrationPlan intent for deterministic materialization paths  
- Preflight behavior is stricter and more explainable before DDL or DML execution starts  
- Destructive schema operations are now executable only after explicit policy approval  
- `_hist` physical drops remain separately gated via `ELEVATA_ALLOW_AUTO_DROP_HIST_COLUMNS`  
- Full-refresh materialization suppresses redundant column-level DDL while preserving dataset rename intent  
- Architecture guard enforcement now runs even with `--no-print`, preventing execution from bypassing shadow-compare blocking

---

### 🛠️ Fixed

- Prevented destructive-but-approved materialization steps from being silently skipped by the applier  
- Fixed enforcement behavior where `--no-print` could suppress Architecture guard execution  
- Stabilized MaterializationPolicy compatibility for direct test and caller instantiation  
- Cleaned up outdated preview-only wording around MigrationPlan semantics

---

### 🔒 Governance & Determinism

- Architecture is now the source of truth for schema evolution intent  
- Materialization is no longer driven by implicit planner-side schema changes  
- Policy gates remain explicit for destructive operations  
- Execution blocks before applying schema changes when architecture intent and materialization output diverge

---

## [1.5.1] - 2026-04-28

This patch release improves developer experience and UI consistency.  
It adds safe test isolation (preventing accidental writes to the metadata DB),  
updates the recommended Python version, and polishes list view actions.

### 🔄 Improved

- Recommended Python version updated to **3.14** (supported: **Python 3.11+**)
- Safer test runner setup:
    - `pytest-django` added to base requirements
    - test settings enforce isolated DB behavior (SQLite tests run in-memory)
    - `runtests.py` now uses test settings by default and fails fast if `pytest-django` is missing

### 🛠️ Fixed

- UI list actions: consistent button group dividers across mixed button styles (solid/outline/disabled)
- SourceDataset list: Import action now renders consistently (disabled placeholder when unsupported), avoiding visual “jitter”
- Reduced pytest warning noise by avoiding premature plugin imports

---

## [1.5.0] - 2026-04-24

This release strengthens elevata’s Architecture Runtime by making architecture drift visible,  
scoping changes to the executed dataset set, and introducing policy-gated destructive schema operations.

The focus is deterministic, explainable schema evolution across supported warehouses.

---

### ✨ Added

#### Architecture State & Diff (scoped)

- Architecture state persistence with stable fingerprinting  
- Scoped architecture diff for the current execution order (includes related `_hist` targets)  
- Migration plan preview for renames, adds, drops and type evolution signals  
- Shadow compare that cross-checks migration intent vs. planned materialization DDL

#### Policy-gated destructive schema operations

- Base-table `DROP COLUMN` support gated by `ELEVATA_ALLOW_AUTO_DROP_COLUMNS`  
- Optional `_hist` physical drops gated by `ELEVATA_ALLOW_AUTO_DROP_HIST_COLUMNS`  
- Minimal CLI notice when destructive DDL is planned/executed (prevents “silent cleanup” surprises)

---

### 🔄 Improved

- Clearer CLI output for architecture drift and migration intent  
- More robust cross-dialect type normalization and equivalence handling  
- Improved determinism for drift detection across warehouse-specific type representations

---

### 🛠️ Fixed

- Multiple edge cases in schema drift planning around rename/add/drop interactions  
- Improved correctness for historization-related schema sync scenarios  
- Miscellaneous stability improvements in execution + materialization tooling

---

## [1.4.3] - 2026-04-04

This patch release restores the correct logo asset reference in elevata's UI.

The focus of this release is a complete and consistent default UI experience  
for users cloning and starting elevata locally.

---

### 🛠️ Fixed

#### UI Logo Asset Reference

- Corrected the UI logo asset reference used by the web interface  
- Restored the intended default UI appearance for fresh local setups  
- Removed a visual regression that could make the interface appear incomplete  
  despite an otherwise successful startup

---

## [1.4.2] - 2026-03-11

This patch release introduces deterministic audit attribution for metadata mutations  
created through ManyToMany relations and scoped metadata editing workflows.

The focus of this release is to ensure that all metadata mutations performed within  
elevata's architecture runtime are consistently attributed to the initiating user.

---

### ✨ Added

#### Deterministic Audit Attribution for Through Models

- Introduced automatic audit propagation for ManyToMany through models
  created via `form.save_m2m()`  
- Added audit backfill logic ensuring `created_by` and `updated_by`
  are populated for lineage relations such as `TargetColumnInput`  
- Ensured consistent audit attribution across scoped metadata editors
  and Query Builder operations

---

### ⚙ Improved

#### Metadata Mutation Audit Consistency

- Hardened audit attribution across metadata CRUD operations,
  scoped views, and contract synchronization workflows  
- Centralized audit backfill logic for ManyToMany through models
  created outside standard ORM save paths

#### Audit Backfill Performance

- Optimized audit propagation for through models using
  SQL-based updates instead of row-by-row ORM updates

---

## [1.4.1] - 2026-03-01

This patch release introduces and stabilizes elevata’s
dialect-specific SQL keyword registry.

The focus of this release is deterministic, dialect-owned handling
of reserved identifiers across all supported warehouses.

---

### ✨ Added

#### Deterministic SQL Keyword Registry

- Introduced dialect-specific reserved keyword modules  
  (`rendering/dialects/keywords/*.py`)  
- Added `elevata_generate_reserved_keywords` management command  
- Engine-truth extraction for:  
  - Databricks (`sql_keywords()`)  
  - DuckDB (`duckdb_keywords()`)  
- Documentation-based extraction with strict validation for:  
  - PostgreSQL  
  - Snowflake  
  - MSSQL  
  - Fabric Warehouse  
  - BigQuery  
- Sanity checks guarding against incomplete or malformed keyword sets  
- Optional core SQL overlay for defensive fallback safety

---

### 🔄 Improved

- Fully restored dialect-owned SQL rendering for keyword extraction  
- Ensured no vendor-specific SQL remains in management commands  
- Stabilized documentation parsing across vendor doc layouts  
- Harmonized quoting behavior across all dialects

---

### 🔒 Architectural Integrity

- Keyword extraction is deterministic and reproducible  
- All SQL used in extraction is owned by the dialect layer  
- Identifier quoting behavior is now fully dialect-driven

---

## [1.4.0] - 2026-02-28

This release significantly expands elevata’s ingestion capabilities (REST + Files)  
while further strengthening deterministic, dialect-owned execution semantics  
across all supported warehouses.

---

### ✨ Added

#### Ingestion Framework (RAW)

- Native RAW ingestion for REST APIs and file-based sources via `SourceDataset.ingestion_config`  
- Support for CSV, JSON, JSONL, Excel and Parquet sources  
- Environment variable expansion for file URIs (e.g. `${ELEVATA_INGEST_ROOT}/...`)  
- CSV parsing options (`delimiter`, `quotechar`, `encoding`)  
- Excel ingestion options (`sheet_name` / `sheet_index`, `header_row`, `max_rows`) with strict validation  
- Chunked file processing for large datasets with accurate total row tracking  
- Standardized RAW landing with preserved JSON `payload` (system role)  
- Deterministic JSON path–driven column mapping for REST and JSON ingestion

#### Ingestion Routing

- Unified ingestion dispatcher as single entry point (`ingest_raw_for_source_dataset(...)`)

---

### ✨ Improved

#### Dialect-Owned Merge Rendering

- Refactored merge rendering to delegate SQL shape entirely to dialects  
- `load_sql` now provides semantic ingredients only (source select, keys, columns)  
- Cross-platform MERGE fallbacks where native MERGE is not available  
- Improved dialect diagnostics and merge validation behavior

#### Fully Executable Historization (SCD Type 2)

- History datasets (`*_hist`) now generate execution-ready SQL  
- Historization pipeline fully dialect-owned (incremental history primitives)  
- Strict validation for required SCD technical columns and INSERT alignment

---

### 🛠️ Fixed

- SQLAlchemy 2.0 execution compatibility in ingestion landing paths  
- Correct cumulative row reporting for chunked RAW ingestion  
- Cross-dialect merge, historization and delete-detection edge cases  
- BigQuery hashing stability and literal rendering correctness  
- Windows file URI normalization for file-based ingestion

---

## [1.3.1] - 2026-02-18

This release stabilizes the Airflow example environment and resolves several issues  
that could prevent successful execution when testing elevata with external target  
systems.

The focus of this release is usability and reliability of the Airflow example setup,  
ensuring that users can immediately validate elevata orchestration against their own  
target database without requiring additional infrastructure.

---

### ✨ Improved

#### Airflow Example Stability

- Airflow example now installs backend-specific dependencies at container startup  
  instead of build time  
- Backend selection is now driven entirely by environment configuration
  via `ELEVATA_SQL_DIALECT`
- Removed hardcoded backend assumptions from Dockerfile and docker-compose  
- Airflow containers now run elevata inside an isolated virtual environment,  
  preventing dependency conflicts with Airflow itself  
- Improved compatibility with Databricks, PostgreSQL, MSSQL, Snowflake,  
  BigQuery and other supported targets

#### Configuration & Environment Handling

- Environment configuration now consistently sourced from `.env`  
- Removed docker-compose defaults overriding user configuration  
- Simplified switching between target systems without rebuilding images  
- Improved portability for local testing and Open Source usage

#### Dependency Handling

- Backend-specific requirements are installed only when required  
- Prevented SQLAlchemy version conflicts with Airflow dependencies  
- Optional dialect imports (e.g. DuckDB) no longer fail when backend is not active

---

### 🛠️ Fixed

- Airflow example failing to start due to dependency conflicts  
- SQLAlchemy downgrade caused by backend requirement installation  
- Incorrect backend loading when multiple dialects were present  
- Environment variable precedence issues between `.env`,  
  docker-compose defaults and container runtime  
- Multiple startup issues related to entrypoint execution  
- Improved robustness of dialect imports across environments

---

### 🔧 Internal

- Refactored Airflow entrypoint initialization logic  
- Added backend installation stamp mechanism to avoid repeated installs  
- Improved separation between Airflow runtime dependencies  
  and elevata execution dependencies

---

## [1.3.0] - 2026-02-17

This release introduces execution safety improvements, deterministic schema evolution,  
and orchestration-level predictability enhancements.

The focus of this release is long-term stability and reproducible execution behavior  
across platforms, rather than expanding modeling capabilities.

Execution planning, schema evolution, and load execution are now aligned around a  
deterministic preflight model that guarantees predictable outcomes before execution begins.

---

### ✨ Added

#### Execution Safety & Predictability

- Introduced preflight validation phase before execution  
- Deterministic failure behavior for unsafe schema evolution scenarios  
- Execution now blocks before SQL execution when unsafe changes are detected  
- Consistent error vs warning classification during materialization planning  
- Improved transparency of execution behavior and failure causes

#### Schema Evolution & Type Drift Handling

- Canonical datatype comparison across all supported dialects  
- Type drift detection during materialization planning  
- Drift classification into:  
  - equivalent  
  - widening (safe)  
  - narrowing / incompatible (unsafe)  
- Automatic schema evolution for safe widening changes  
- Dialect-aware ALTER COLUMN generation where supported  
- Deterministic rebuild fallback for dialects without ALTER support  
- Consistent schema evolution behavior across Snowflake, Databricks, PostgreSQL,  
  MSSQL and DuckDB

#### Materialization & Execution Architecture

- Materialization split into planning and application phases  
- MaterializationPlan now explicitly represents:  
  - required DDL steps  
  - warnings  
  - blocking errors  
- Structural synchronization of rawcore `_hist` datasets aligned with base datasets  
- Improved consistency between schema introspection, metadata, and execution behavior

#### Orchestration & Execution Planning

- Metadata-driven execution manifest generation  
- Deterministic dependency graph for dataset execution order  
- Airflow example DAG for lineage-based execution orchestration  
- Execution planning fingerprint for reproducibility validation

#### Query Builder UX Improvements

- Query Builder now displays inline validation and dependency conflict messages  
  directly in the editing context instead of failing silently  
- Blocking mutations caused by downstream dataset dependencies are surfaced  
  as contextual warnings in the grid UI  
- Inline editors remain open when a mutation is rejected, allowing users to  
  immediately correct input  
- Conflict warnings are automatically cleared when cancelling or completing edits  
- Improved transparency when renaming or modifying query-derived columns  
  referenced downstream

---

### 🛠️ Fixed

- Multiple edge cases where schema drift could result in inconsistent execution behavior  
- Incorrect handling of schema evolution across dialect-specific type representations  
- Improved stability of materialization planning for renamed datasets and columns  
- Various internal consistency improvements in execution planning and validation

---

### 🔒 Governance & Determinism

- Execution behavior is now fully determined before execution begins
- Unsafe schema changes are blocked deterministically
- Reduced risk of partial schema application or inconsistent load states
- Improved alignment between metadata contracts and physical schema evolution

---

## [1.2.0] - 2026-02-08

This release introduces a major upgrade to Query Builder contract handling,  
query-derived schema synchronization, and datatype inference.

It significantly improves determinism, usability, and correctness of  
metadata-driven query modeling while simplifying internal typing logic.

### ✨ Added

#### Multi-platform execution support

- Added native execution dialects for:  
  - Snowflake  
  - Databricks SQL (Unity Catalog)  
  - Microsoft Fabric Warehouse  
- Unified execution semantics across platforms using the same metadata model  
- No platform-specific modeling required  
- Architecture defined once, executed consistently across engines

This release marks the first version where elevata execution semantics are aligned across  
multiple modern cloud data warehouse platforms.

#### Query Builder & Contract Handling
- Query-derived TargetColumns are now fully synchronized with the Query Tree output contract  
- Automatic creation, rename, update and deletion of query-derived columns  
- Contract-based schema alignment between:  
  - SQL preview  
  - logical plan  
  - materialized dataset schema  
- Aggregate nodes now redefine output contracts explicitly as:  
  - group keys + measures only  
- Window and Aggregate operators expose correct upstream input columns  
  without requiring intermediate target column assignment

#### Datatype inference
- Deterministic datatype inference for:  
  - window functions (ROW_NUMBER, RANK, DENSE_RANK)  
  - aggregate measures (COUNT, SUM, MIN, MAX, AVG)  
- COUNT and ranking functions now default to INTEGER instead of BIGINT  
- Datatypes inferred from upstream input columns where possible  
- Canonical datatype resolution aligned with DATATYPE_CHOICES

#### Query Builder UX improvements
- Aggregate editor now shows input dataset columns directly  
- Window and Aggregate nodes operate on input contracts instead of
  materialized target schema  
- Eliminates unnecessary intermediate column creation

#### Databricks execution improvements
- Improved raw ingestion performance for Databricks SQL Warehouse by batching multi-row INSERT execution
- Eliminates per-row execution overhead caused by connector-level executemany behavior
- No changes required to metadata models or query definitions

### 🛠️ Fixed

- Window columns incorrectly defaulting to STRING datatype  
- Aggregate measures not inheriting input column datatype  
- Contract sync overriding inferred datatypes  
- Missing input columns in Aggregate editor selection  
- Incorrect system-managed flag for query-derived columns  
- Multiple contract sync race conditions caused by partial state inference

### 🔒 Governance & Determinism

- Query contract inference now consistently evaluates against query_head  
- Dataset schema always reflects effective query output contract  
- Deterministic datatype normalization across query-derived columns  
- Reduced risk of schema drift between metadata and generated SQL

---

## [1.1.0] - 2026-01-31

This release introduces a major upgrade to the Query Builder and UNION workflow,  
together with critical stability and governance improvements across metadata,  
schema sync, and historization.

### ✨ Added

#### Query Builder & UNION workflow
- Full UNION operator support with:  
  - Branch management  
  - Output schema contracts  
  - Column mappings per branch  
- Guided UNION toolbar with:  
  - Output / Branch navigation  
  - Schema copy from branch  
  - Auto-map by name  
  - Validation and “Set as head”  
- Contract snapshot with input → output diff  
- Determinism and governance indicators (ordering, window/aggregate checks)  
- Contextual node summaries (SELECT, AGGREGATE, WINDOW, UNION)

#### Metadata-driven governance
- Clear separation of **query_root** (anchor) and **query_head** (current output)  
- Head-based validation, SQL preview and contract inference  
- Guardrails for destructive operations with downstream dependents

#### Schema sync & historization
- Robust metadata-driven rename propagation via `former_names`  
- Orphan preservation in `_hist` (inactive, detached, not dropped)  
- Safe FK reference deletion that never removes hist columns  
- Technical tail columns always appended last  
- Deterministic ordinal normalization for system-managed schemas

### 🛠️ Fixed

- UNIQUE constraint violations in hist regeneration  
- Broken rename propagation across rawcore → hist  
- False-positive orphan deletions  
- Inconsistent candidate column inference in query builder  
- UNION validation / navigation not triggering correctly  
- Output schema copy errors due to field mismatches  
- Multiple signal / sync race conditions

### 🔒 Governance & Safety

- UNION validation enforces compatible schemas  
- Set-as-head explicitly marks final dataset output  
- Defensive rebuild of hist metadata with key lineage preservation  
- Deterministic ordering guarantees for window and aggregate functions

---

## [1.0.0] - 2026-01-25 — First stable release

This release marks the **first stable, backwards-compatible version** of elevata.

elevata has evolved from a SQL generation layer into a **metadata-driven, deterministic data platform engine**.  
From this version onwards, interfaces, metadata structures and execution semantics are considered stable.

### ✨ Highlights

#### 🪄 Query Builder (Major Feature)
- Explicit, metadata-driven **query planning** with SELECT, AGGREGATE, WINDOW and UNION nodes  
- Deterministic query execution via a formal **Query Tree**  
- Clear separation between **query structure** and generated SQL  
- Guided UI for building and evolving queries without writing SQL  
- SQL preview reflects the *exact* executed query

#### 🧠 Query Contracts
- Explicit, inspectable **output schema contracts**  
- Field-level validation and error reporting  
- Early detection of incompatible unions, missing mappings and invalid transformations

#### 🧬 Lineage & Explainability
- End-to-end lineage across datasets, fields and query nodes  
- Full traceability from source to serving layer  
- Query tree visualization for complex transformations

#### 🔒 Determinism & Governance
- Deterministic execution guarantees for aggregates and window functions  
- ORDER BY / PARTITION BY governance with clear warnings and errors  
- Clear distinction between execution errors and policy violations

#### 🧩 Platform Maturity
- Stable execution semantics  
- Backwards compatibility guarantees from this release onwards  
- Foundation for orchestration and governance integrations in future releases

---

## [0.9.1] – 2026-01-14

### ✨ Improved
- Refined lineage visualization with configurable multi-hop upstream and downstream views  
- Clearer lineage semantics distinguishing direct inputs from extended execution dependencies  
- Improved lineage UX with consistent ordering and scope labeling

### 🧠 Serving Layer
- Serving datasets now support presentation-oriented identifiers (friendly names)  
- Dataset and column naming in Serving layer allows casing, spaces, and special characters  
- Identifier handling is dialect-aware and uses proper quoting where required

### 🛡️ Validation & Health
- Extended metadata validation to distinguish blocking vs advisory findings  
- Improved health checks for Serving and Bizcore datasets  
- Validation logic consolidated and aligned across layers

### 🧩 Internal
- Improved consistency between lineage analysis, validators, and UI  
- Minor internal cleanups in metadata services and views

---

## [0.9.0] – 2026-01-12

### 🧠 Bizcore: Business Semantics as First-Class Metadata

This release introduces **Bizcore**, a dedicated layer for modeling  
business meaning, rules, and calculations as explicit metadata —  
executed deterministically alongside technical datasets.

Bizcore makes elevata **business-capable by design**, without introducing  
BI-style semantic layers or query-time abstractions.

### ✨ Added
- Bizcore datasets and columns as first-class metadata objects  
- Multi-upstream join support for Bizcore datasets  
- UI support for building and validating Bizcore structures  
- Deterministic SQL preview for Bizcore datasets  
- Lineage-driven qualification of expressions and joins  
- End-to-end traceability from Core → Bizcore → Serving

### 🔄 Changed
- SQL generation now fully respects semantic lineage in expressions  
- Join aliasing and qualification are applied consistently across layers

### 🧪 Quality & Stability
- Extensive validation of join correctness and expression rendering  
- Scoped and non-scoped UI flows aligned under a single metadata model  
- No breaking changes to existing Raw, Stage, or Core pipelines

### ✨ Improved
- Manual expressions now automatically qualify unaliased column references  
  with the correct input alias during SQL generation.  
  This ensures consistent, unambiguous SQL for Bizcore calculations  
  without requiring users to manually prefix column names.

> This release marks a major milestone: elevata now supports  
> **explicit business semantics as metadata**, not as BI-layer logic.

---

## [0.8.0] – 2026-01-04

### ⚙️ Execution & Orchestration as First-Class Architecture

This release introduces an explicit, metadata-driven execution model,  
establishing **orchestration, failure semantics, and observability** as first-class concerns in elevata.

Execution is now planned, executed, and explained independently of SQL generation,  
providing a robust foundation for platform-native orchestration and governance.

### ✨ Added
- Explicit **Execution Plan** model separating planning from execution  
- Dependency-graph–based dataset execution with deterministic ordering  
- Multi-dataset batch execution with a shared `batch_run_id`  
- Structured execution policies (`continue_on_error`, `max_retries`)  
- Retry semantics with per-attempt tracking (`attempt_no`)  
- Distinct failure semantics:  
  - `blocked` (dependency-based non-execution)  
  - `aborted` (policy-based fail-fast non-execution)  
- **Load Run Snapshot** (`meta.load_run_snapshot`)  
  - Batch-level, JSON-based execution state  
  - Captures plan, policy, dependencies, and aggregated outcomes  
- Extended **Load Run Log** (`meta.load_run_log`)  
  - Orchestration-only events (blocked / aborted)  
  - Best-effort, non-blocking meta logging  
- CLI execution diagnostics:  
  - Execution snapshot printing (`--debug-execution`)  
  - Snapshot persistence (`--write-execution-snapshot`)  
- Deterministic BigQuery table qualification for execution and metadata writes    
  (prevents sporadic cross-project `NotFound` errors during streaming inserts)  
- Global execution modes:  
  - single-dataset execution with dependencies (default)  
  - platform-wide execution in deterministic order (`--all`)  
  - optional schema-scoped execution (`--schema`)

### 🔄 Changed
- Execution semantics are no longer implicit in SQL or CLI flow  
- Load execution is now driven by an explicit execution model  
- Fail-fast behavior is deterministic and explicitly reported  
- Execution observability is metadata-first and dialect-agnostic  

### 🧪 Quality & Stability
- Extensive unit tests for execution ordering, retries, fail-fast, and blocking  
- Guardrails for orchestration-only events and best-effort persistence  
- Clear separation of execution core vs CLI and dialect adapters  
- No destructive changes to existing materialization or SQL generation logic

> This release establishes elevata as a **self-orchestrating, explainable  
> data platform core**, laying the groundwork for native scheduling,  
> governance rules, and external orchestration integrations.

---

## [0.7.1] – 2025-12-29

### 🧱 Metadata-Driven Schema Evolution

This release completes and stabilizes the first materialization layer for  
**safe, deterministic schema evolution** in target warehouses.

Schema changes are now derived explicitly from metadata and applied in a  
controlled, lineage-aware manner — without implicit inference from SQL.

### ✨ Added
- Metadata-driven materialization planning for target datasets  
- Automatic provisioning of missing target tables  
- Deterministic column synchronization (additive, non-destructive)  
- Explicit handling of dataset and column renames via `former_names`  
- Lineage-aware propagation of renames into history (`_hist`) datasets  
- Deterministic `INSERT … (column list)` generation across all dialects  
- DuckDB-native introspection via PRAGMA with execution-engine consistency

### 🔄 Changed
- Materialization is now planned separately from SQL rendering  
- Table existence is determined by effective provisioning steps, not schema creation alone  
- Incremental loads reliably auto-provision target tables when required  
- History datasets are provisioned deterministically and stay structurally aligned with base tables

### 🧪 Quality & Stability
- Extensive unit tests for materialization planning and rename scenarios  
- Guardrails for ambiguous rename situations (multiple former matches)  
- Improved separation of introspection vs execution concerns  
- Removed duplicate provisioning paths and race conditions  

> This release lays the foundation for controlled schema evolution,  
> future governance rules, and automated validation layers.

---

## [0.7.0] – 2025-12-21

### Added
- Dataset-driven, lineage-aware execution with automatic dependency resolution  
- Unified RAW execution semantics via physical ingestion (Source → RAW)  
- Stable technical column model across all layers  
- BigQuery execution backend with native ingestion support  
- Dialect-aware hashing and surrogate key generation (BigQuery, DuckDB, Postgres, MSSQL)  
- SourceDataset-level static and incremental filters with runtime {{DELTA_CUTOFF}} resolution  

### Changed
- Execution operates on datasets rather than layers  
- RAW datasets are treated as an optional ingestion layer  
- RAW ingestion always rebuilds tables, while source extraction may be incrementally scoped  

### Fixed
- Load plan debug output and execution logging consistency  
- Signal handling for historization execution order  
- Correct application of incremental filters during ingestion and delete detection  
- Lineage-based translation of incremental scope filters across renamed columns  
- Cross-dialect consistency for incremental execution (DuckDB, MSSQL, Postgres, BigQuery)  

---

## [0.6.1] – 2025-12-15

### Fixed
- Correct introspection of SQL Server alias types (e.g. `dbo.Name`, `dbo.Flag`)
- Proper handling of `bit` columns during ingestion (no fallback to string types)
- Correct precision and scale mapping for `money` and `smallmoney` columns
- Stable and deterministic column ordering during metadata import

### Improved
- Lossless ingestion of source datatypes via `source_datatype_raw`
- Strict, fail-fast dialect-specific type rendering to prevent silent fallbacks

### Notes
- This release significantly improves correctness for SQL Server as a source system.
- Re-importing source metadata is recommended to benefit from the improved typing behavior.

---

## [0.6.0] – 2025-12-14

### 🚀 Warehouse-Native Execution & SCD Historization
This release introduces the foundation for a fully warehouse-native execution framework.  
elevata now manages entire data load pipelines end-to-end — from metadata to SQL generation to execution, historization and observability.

### ✨ Major Features

#### 1. Execution Engine (`--execute`)
elevata can now execute rendered SQL directly against target systems, measure performance, record affected rows, and log complete run metadata.  
This shifts elevata beyond SQL rendering into a full pipeline engine.

#### 2. Full SCD Type 2 Historization
A deterministic, metadata-driven historization framework:  
- automatic change detection via row-hash  
- version closing for changed and deleted keys  
- insertion of new and changed versions  
- lineage-aware attribute propagation  

#### 3. Metadata-Driven Incremental Merge Loads
Complete incremental pipeline including:  
- new-row inserts  
- changed-row updates  
- delete detection  
- MERGE or UPDATE+INSERT fallback depending on dialect

#### 4. Auto-Provisioning of Warehouse Structures
elevata can automatically create:  
- target schemas (raw, stage, rawcore, ...)  
- the `meta.load_run_log` table  
- all required objects for execution and logging  

Controlled via `.env` flags.

#### 5. Warehouse-Level Load Logging
A new table `meta.load_run_log` provides full observability into load executions:  
- load mode, historization flags, dialect  
- start/end timestamps, render/execution duration  
- rows affected, error messages, status  
- batch and run identifiers  

#### 6. Documentation Expansion
- New historization architecture document  
- Extended execution, logging, and provisioning sections  
- Revised dialect and SQL generation chapters  

### 🧪 Testing Improvements
- Deterministic SQL tests for merge and historization pipelines  
- Combined historization pipeline tests  
- Prepared E2E execution flow for dialect-specific execution engines  

> This release establishes the execution foundation on which future orchestration, validation and automation layers will be built.

---

## [0.5.3] — 2025-12-10
### 🔹 Historization Structure & Dialect Engine Enhancements

This release completes the metadata foundation required for full historized
incremental loading in v0.6.0. It finalizes *_hist dataset structure, ensures
cross-dialect consistency, and extends SQL rendering to use dialect-driven
identifier rules.

## ✨ Highlights

### Metadata / Historization
- Automatic creation and maintenance of `<dataset>_hist` datasets in RAWCORE
- Full rename propagation for datasets and columns
- All *_hist fields are system-managed and read-only
- New technical field in RAWCORE: `row_hash` for change detection (persisted expression)
- Versioning strategy established:
  - `version_started_at` inclusive, `version_ended_at` exclusive
  - open-ended validity via max timestamp
  - `version_state` (`current`, `changed`, `deleted`)

### SQL Generation / Dialects
- Unified `render_identifier()` and `render_table_identifier()` for consistent quoting
- All SQL generation now uses dialect identifier rendering
- Delete detection routing tested and guarded per dialect capability

### Load Runner
- `elevata_load` supports `--execute` with safe stub execution via `ExecutionEngine`
- Logging improvements and full dry-run support remain functional

### Testing & Stability
- Expanded test coverage for historization and dialect routing
- Full suite green across merge, delete detection & *_hist scenarios

---

## [0.5.2] — 2025-12-07
### 🛠️️ Metadata stability & History (HIST) foundation

This release significantly improves the robustness, determinism, and safety of 
history metadata generation in the RAWCORE schema.

## ✨ Highlights

### Metadata / Historization
- Deterministic generation of *_hist datasets based on lineage_key.  
- Robust schema sync between RAWCORE and *_hist (idempotent, safe deletes).  
- History SK expression based on rawcore SK + version_started_at.  
- History BK definition: rawcore SK + version_started_at.  
- History datasets and columns are fully system-managed (no UI unlock).

### Signals & UI
- Automatic *_hist sync on dataset rename and column changes in rawcore.  
- Inline rename refreshes both rawcore and corresponding *_hist rows.  
- Inline editing is disabled for *_hist datasets and columns.  

### SQL Preview
- build_sql_preview_for_target returns a clear comment for history targets instead of misleading SQL.
- Tests added to guard the _hist-preview behaviour.

---

## [0.5.1] — 2025-12-04
### 🧹 Documentation & Consistency Release

This patch focuses on improving the clarity, coherence, and structure of elevata’s developer documentation.

## ✨ Highlights
- Full harmonization of all architecture documents  
- Removal of outdated version references and legacy wording  
- Unified heading and layout style across all Markdown files  
- Consistent terminology for LogicalPlan, Expression DSL, Dialects, and Load SQL  
- Improved mkdocs navigation structure  
- Minor text corrections and consistency fixes across the docs

## 🚫 No functional changes
This release does not modify the SQL engine, metadata model, or any public API surface.  
All test suites remain unchanged and green.

---

## [0.5.0] — 2025-12-01
### 🛠️️ Multi-Dialect Engine, MSSQL Support & Deterministic FK Hashing

This release delivers the next major milestone of elevata’s SQL engine:
full **multi-dialect SQL generation**, an extensible dialect factory,
runtime dialect switching in the UI, and a complete rewrite of the
surrogate-key and foreign-key hashing system using a vendor-neutral DSL AST.

---

## 🚀 Major Features

### **1. Multi-Dialect SQL Rendering (Postgres, DuckDB, MSSQL)**
- New pluggable dialect architecture (`SqlDialect`, `dialect_factory`).
- Three fully operational dialects:
  - **DuckDBDialect**
  - **PostgresDialect**
  - **MssqlDialect** (new)
- Centralised dialect registry & runtime resolution via:
  - profile  
  - env (`ELEVATA_SQL_DIALECT`)  
  - URL parameter in SQL preview  

All SQL generation (preview + Load Runner) now passes through a unified,
dialect-aware pipeline.

---

### **2. SQL Preview Dialect Selector (UI)**
- New dropdown in TargetDataset detail view.
- Instant SQL refresh via HTMX request.
- Clean display of dialect-specific SQL functions (quoting, hashing, concat, types).

---

### **3. Deterministic, Cross-Dialect Hashing via DSL AST**
A full rewrite of surrogate-key and FK hashing:

- New DSL expression system (`Hash256Expression`, `ConcatWsExpression`, `Literal`, `ColumnRef`).
- Dialect-specific SQL rendering happens **exclusively in dialect classes**.
- Identical logical lineage yields identical hash values across vendors.
- Fully deterministic ordering + null replacement semantics.
- Clean child-lineage FK hashing:
  - BK1, child BK1, BK2, child BK2…
  - `~` and `|` literal separators, ordered alphabetically

All existing hashing tests green after the rewrite.

---

### **4. Multi-Source Stage Identity Mode**
- Correct logical union builder for Stage datasets with multiple upstream sources.
- Clean identity (no ranking) vs. non-identity (ranking) handling.
- Injected `source_identity_id` literal per upstream branch.
- All multi-source identity tests fully passing.

---

### **5. Dialect-Aware FK Rendering**
- Parent surrogate keys and child FK keys now rendered via DSL → dialect.
- MSSQL: `CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', …), 2)`
- Postgres: `ENCODE(DIGEST(CONCAT_WS(...), 'sha256'), 'hex')`
- DuckDB: `SHA256(CONCAT_WS(...))`

---

## 🔧 Internal Improvements
- Entire `builder.py` cleaned, simplified, and refactored.
- Unified `render_select_for_target()` and load-SQL paths.
- Removed legacy manual hashing logic.
- No raw SQL string assembly left in hashing pipeline.
- Strict quoting rules per dialect.
- Sauber extrahierte DSL operators (`col()`, `lit()`, `concat_ws()`, `hash256()`).

---

## 🧪 Testing
- New tests:
  - `test_dialect_postgres.py`
  - `test_hashing_dialects.py`
  - `test_fk_hashing.py`
  - Full MSSQL hashing coverage
- Updated test helpers for DSL AST inspection.
- All Stage multi-source tests green after identity-mode rewrite.

---

## 📘 Documentation
- Updated architecture docs:
  - *Dialect System*
  - *SQL Rendering Conventions*
  - *Hashing Architecture*
- README modernised with new capabilities and architecture.

---

## 🧭 Roadmap Shift
With the 0.5.0 SQL backend complete, the next stage focuses on execution:

- Load Runner CLI (Full, Merge, Dry-Run)
- Caching & improved SQL formatting
- Multi-source incremental merges
- Additional dialects (Snowflake, BigQuery, Databricks)

---

**Impact**  
Version **0.5.0** transforms elevata into a **true multi-backend SQL generator**  
with deterministic hashing, dialect-specific rendering, and a stable architectural core  
for future execution engines.

---

## [0.4.0] — 2025-11-20
### 🧠 Dialect Architecture & Load SQL Modernization

This release marks a major leap for elevata:  
a complete SQL dialect abstraction layer, a unified Load-SQL pipeline,  
and extensive new documentation that sets the foundation for future multi-backend support.

---

### 🚀 Core Features

#### **Fully Modular SQL Dialect System**
A new, extensible dialect layer powers all SQL generation:

- Central `SqlDialect` base class  
- Concrete `DuckDBDialect` reference implementation  
- Dialect resolution via `ELEVATA_SQL_DIALECT`, `ELEVATA_DIALECT`, and active profile  
- Dialect capabilities:
  - `supports_merge`
  - `supports_delete_detection`
- Expression-level hooks:
  - `concat_expression()`
  - `hash_expression()`
  - `cast_expression()`
  - `render_literal()`

This architecture enables clean, vendor-neutral SQL generation for future backends  
(Postgres, MSSQL, Snowflake, BigQuery, Databricks).

---

### 🔧 Load SQL Architecture 2.0

A fully redesigned, dialect-aware Load SQL engine:

#### **Full Load**
- `render_create_replace_table`  
- `render_insert_into_table`  
- Uses dialect quoting, casting, literal handling

#### **Incremental Merge Load**
- Native dialect-specific `MERGE` for DuckDB  
- Clean failure modes for dialects without merge support  
- Deterministic key handling  
- Automatic update/insert column mapping

#### **Delete Detection**
- Dialect-specific implementation (`DELETE … WHERE NOT EXISTS`)  
- Guardrails when delete detection is requested but dialect does not support it

All Load SQL now flows through a single, coherent pipeline via `load_sql.py`.

---

### 🧪 Testing Enhancements

- New test suite for:
  - literal rendering (`NULL`, booleans, strings, dates, datetimes)
  - cast expression rendering
  - concat & hash expression helpers
  - merge & delete detection dialect hooks
- End-to-end tests for Full and Merge load generation
- All tests green across the refactor

This ensures reliable future extensions to new SQL dialects.

---

### 📘 Documentation

Three major new documents added:

- **Dialect System** — full architectural overview of dialect abstraction  
- **Load SQL Architecture** — how Full, Merge, and Delete Detection SQL are generated  
- **Incremental Load Architecture** — planner, merge semantics, delete detection

All are linked from:
- `index.md`
- `README_docs.md`
- `mkdocs.yml` navigation

---

### 🔍 Internal Improvements

- Harmonized `get_active_dialect()` with environment and profile resolution  
- Consolidated SQL preview and load paths to use the same dialect entrypoints  
- Removed legacy assumptions and duplicated logic  
- Fully revised DuckDB implementation as reference for new dialects

---

### 🗺️ Roadmap Impact

With 0.4.0 released, the following items shift to **0.5.x**:

- Target System Selector (Profiles → target backend)  
- Additional SQL dialects (MSSQL, Postgres, Snowflake)  
- Pseudo-Lineage Graph in UI  
- Multi-Source Incremental Loads  
- Load-Runner CLI

These features build directly on the new architecture introduced in 0.4.0.

---

**Impact**  
Version 0.4.0 delivers the foundational SQL engine for elevata’s future:  
clean, extensible, and ready for multiple SQL backends.  
It stabilizes the path toward 0.5.x — where elevata becomes a multi-dialect metadata-driven ETL generator.

---

## [0.3.0] — 2025-11-12
### Lineage-Aware Target Generation & SQL Preview

#### 🚀 Core Features

**Lineage-Driven Target Generation**
- Added a stable `lineage_key` to both `TargetDataset` and `TargetColumn`:
  - Enables fully **idempotent target generation**.
  - Prevents duplicate targets after renaming (`lineage_key` is preserved).
- `TargetGenerationService.apply_all()` refactored into modular steps:
  - Existing datasets are now matched and updated via `lineage_key` instead of physical names.
  - Clean dataset-level and column-level re-numbering during regeneration.

**Three-Layer Data Lineage**
- Explicit dataset-level lineage:
  - `TargetDatasetInput` defines upstream relationships (`source_dataset` and/or `upstream_target_dataset`).
  - `combination_mode` (`single` or `union`) indicates how multiple inputs are combined.

- Explicit column-level lineage:
  - `TargetColumnInput` mirrors the same relationships for individual columns.
  - `upstream_columns` now correctly map transformations between layers.

- Layer-specific rules:
  - **Raw** = only `source_datasets`
  - **Stage** = prefers Raw as upstream (or Source directly if `generate_raw_tables=False`)
  - **Rawcore** = always built from Stage

**Multi-Source Consolidation**
- New `SourceDatasetGroup` + `SourceDatasetGroupMembership` model:
  - Supports joining multiple SourceDatasets into a single Stage target.
  - The “primary system” flag defines which source drives column order.
- `TargetDatasetInput.role` classifies inputs as:
  - `primary`, `enrichment`, `reference_lookup`, or `audit_only`.

**Surrogate & Business Keys**
- Surrogate key columns are automatically renamed when their dataset is renamed  
  → e.g. renaming `rc_aw_productmodel` → `rc_aw_product_model` auto-renames the key column to `rc_aw_product_model_key`.
- Surrogate key expressions now reference **upstream column names** (Raw or Stage), not renamed targets.
- Deterministic column ordering:
  1. Surrogate keys  
  2. Business keys  
  3. Integrated source columns  
  4. Artificial columns

**Column Generation Enhancements**
- Automatic assignment of `ordinal_position` on save:
  - Newly created columns append at the end in numeric sequence.
  - Safe against manual reordering.
- Integrated columns added after initial generation are correctly appended and re-numbered without violating unique constraints.

---

#### 🧠 Logical Query Model & SQL Preview

**Logical Plan Layer**
- New internal model (`logical_plan.py`) represents canonical SQL structure for a target dataset:
  - Supports `LogicalSelect`, `LogicalUnion`, `LogicalExpression`, and lineage mapping.
- `builder.py` now constructs expressions (Surrogate Key, BK, and regular fields) from `TargetColumnInput` lineage.
- Dialect-specific type mapping handled cleanly via `map_logical_to_duckdb_type`.

**SQL Preview 2.0**
- SQL preview now generates **true lineage-based SELECT statements**, e.g.:
  - **Stage:**  
    ```sql
    SELECT … FROM "raw"."raw_aw1_person"
    UNION ALL
    SELECT … FROM "raw"."raw_aw2_person"
    ```
  - **Rawcore:**  
    ```sql
    SELECT hash256(…) AS rc_aw_person_key, … 
    FROM "stage"."stg_aw_person"
    ```
- Automatic field alignment:
  - Columns missing in one upstream are rendered as `NULL AS <column>`.
  - Integrated columns retain their target aliases.
- Supports both `manual_expression` and templated (`{{ … }}`) syntax.
- New visually distinct **green preview box** in UI with proper formatting:
  - Keywords capitalized  
  - Indentation after `SELECT`  
  - Clean separation before `FROM`

---

#### 🧩 UI, Governance & Behavior

- **Context-aware lineage display** in detail views:
  - `Source Datasets` and `Upstream Datasets` shown based on layer.
  - Input relations now read like:
    ```
    raw_aw1_person · businessentityid -> stg_aw_person · businessentityid
    ```
- **System-managed field handling** refined:
  - Layer-specific read-only fields controlled by settings.
  - `lineage_key` treated as an internal system field (hidden in forms and lists).
  - Surrogate key names locked for user editing but updated automatically when renaming datasets.

---

#### 🧪 Testing & Quality

**Structured Testing Foundation**
- Introduced the first complete automated test framework for the metadata generation platform.
- Added dedicated `runtests.py` launcher for reliable execution across environments.
- Integrated **realistic DB-based lineage tests** (`Raw → Stage → Rawcore`).
- Added **logic-only tests** for hashing, naming, and validators.
- Prepared **SQL Preview test templates** for the future rendering pipeline.
- New documentation: [🧪 Testing & Quality](docs/tests.md)

**Impact**  
This milestone establishes a solid foundation for test coverage,  
ensuring safe refactoring, reproducibility, and confidence in every release.

---

## [0.2.6] — 2025-11-03
### ⚙️ Target Generation & Surrogate Key Implementation

**Core Features**
- Introduced fully automated `TargetDataset` and `TargetColumn` generation service (`TargetGenerationService`).
- Deterministic surrogate key creation using SHA-256 and runtime-loaded pepper.
- Added `business_key_column` and `surrogate_key_column` flags to differentiate logical vs. physical keys.
- Layer-aware naming now based on `TargetSchema.physical_prefix` (no hardcoded prefixes).
- Integrated filtering: only `integrate=True` columns included across all layers.

**UI Enhancements**
- Added **“Generate Targets”** button to SourceDataset list with progress spinner & success message.
- Improved error feedback and runtime validation for pepper and target schema scope.
- Consistent Bootstrap iconography (`bi-lightning-charge`) and visual feedback for active operations.

**Technical Refinements**
- Surrogate key expressions persisted in metadata for transparency and traceability.
- Environment-based pepper resolution via `.env` and `get_runtime_pepper()`.
- Refactored naming logic (`naming.py`, `rules.py`, `mappers.py`) for consistent layer-specific conventions.

**Impact**  
This release completes the **Target Automation foundation** for elevata —  
paving the way for v0.3.0’s Meta-SQL and rendering engine. 🚀

---

## [0.2.5] — 2025-10-27  
### 🧩 Metadata Model Finalization & UI Polish  

**Core Enhancements**  
- Completed redesign of the **core metadata model** — fully aligned with the 0.3.x architecture.  
- Added **TargetSchema** as a first-class model defining platform layers (`raw`, `stage`, `rawcore`, `bizcore`, `serving`).  
- Introduced **TargetDatasetInput** and **TargetColumnInput** for multi-source mappings and lineage tracking.  
- Added lifecycle flags (`active`, `retired_at`) for controlled dataset and column deprecation.  
- Simplified incremental-load logic (`increment_filter` placeholder on SourceDataset).  
- Unified naming conventions (`*_schema_name`, `*_dataset_name`) across all models.  
- Extended governance primitives (`sensitivity`, `access_intent`) and surrogate-key configuration per layer.  
- Removed obsolete fields (`get_metadata`, `stage_dataset`, etc.) and harmonized field semantics.  

**UI & Usability**  
- Introduced **SourceDatasetGroup** for managing groups of structurally identical source tables.  
- Added governance badges and toggles for better lineage and visibility cues.  
- Revised navigation order for more natural workflows.  
- Improved help texts, icons, and consistent color themes across all metadata entities.  

**Impact**  
This release finalizes the **metadata foundation** for elevata — stable enough for automation development in 0.3.x.  
No breaking structural changes expected before 0.3.0.  

---

### 🪶 UI Comfort Continuation  

- Unified color scheme for governance badges (`badge-pii-high`, `badge-pk`, …).  
- Improved hover feedback and spacing in list views.  
- All badges defined declaratively via `ELEVATA_CRUD` — no model-specific logic required.  
- Updated `elevata-theme.css` for consistent badge geometry and hover states.  

---

**Why it matters**  
Version 0.2.5 concludes the **“Model & Comfort”** milestone:  
the framework now combines a stable metadata core, polished UI, and ready groundwork for automated target generation in 0.3.x. 🚀  

---

## [0.2.4] — 2025-10-26
### Strategic Documentation & Architecture Alignment

This release finalizes the strategic and architectural foundation for the upcoming **metadata model freeze (v0.3.x)**.  
It does not yet include model changes — instead, it defines the *why* and *how* for the next major milestone.

**Highlights**
- New and refined **README** with philosophy, vision, and AGPLv3 licensing
- Updated **roadmap** outlining the transition toward declarative architecture
- Strategic **dbt decoupling paper**, defining the new “governed SQL through architecture” direction
- Preparations for **TargetSchema**, **TargetDatasetReference**, and **deterministic key generation** to follow in v0.3.x

**Why it matters**  
This release marks the calm before the model storm — the documentation is ready, the vision is clear, and the next step is building it. 🚀


## [0.2.3] – 2025-10-25
### 🪶 UI Comfort Release 

**Highlights**
- Added generic, reusable filter bar for all CRUD list views  
- Added dynamic toggle buttons for boolean fields  
- Improved badge rendering for PII & PK indicators  
- Added sticky table headers for long datasets

**Why it matters**  
This release focuses purely on usability and governance visibility.  
It lays the groundwork for 0.3.0 (TargetDataset automation and lineage features).

---

## [0.2.2] – 2025-10-25
### 🧹 Maintenance Release — dbt Dependency Cleanup

#### Summary
This minor maintenance release removes all remaining dbt-related artefacts and clarifies elevata’s independent direction ahead of the 0.3.x milestone.

#### 🔧 Changes
- Removed unused `dbt_project/` folder from repository.
- Deleted all `DBT_*` variables from `.env` and example configuration files.
- Removed dbt references from `NOTICE.md` and documentation.
- Updated `README.md` and `dbt_decoupling.md` to reflect full **runtime independence**.
- Adjusted Roadmap and strategy wording in `CHANGELOG.md` (dbt now optional adapter, not dependency).
- Minor documentation clean-ups and license consistency fixes (MIT → AGPL v3 in trademark notice).

#### 💡 Notes
This release does **not** introduce new features but marks an important architectural boundary:
elevata ≥ 0.2.2 operates entirely without dbt or its configuration files.  
The foundation for native rendering and execution begins with v0.3.x.

---

## [0.2.1] – 2025-10-23
### 🪶 Improved
- Added truncation for long text fields (`Description`, `Remark`) in list views to improve readability.  
- Full text now appears on hover for better UX.  
- Refined visual highlighting for primary and integrate columns.  
- Minor CSS polish and layout consistency fixes across metadata tables.  

---

## [0.2.0] - 2025-10-22

### 🧩 Metadata Introspection & Profiles Integration

#### Overview  
This release marks a major milestone – elevata now connects to relational sources via SQLAlchemy and imports full schema metadata directly into its core models. The new profile and secret management architecture lays the foundation for secure, declarative, and environment-aware metadata operations.

---

#### 🚀 Highlights  

- **Generic Metadata Import via SQLAlchemy**  
  - Engine factory supporting multiple relational backends (MSSQL, Postgres, SQLite).  
  - Reads column definitions, data types, PK information from `SourceDataset` entries.  
  - Automatic datatype normalization across dialects (e.g. `NVARCHAR` → STRING, `BIT` → BOOLEAN).  

- **Flexible Secrets & Profiles**  
  - Unified `elevata_profiles.yaml` config with environment-based secret resolution.  
  - Connection references derived convention-based from `type` and `short_name`.  
  - Optional Azure Key Vault integration.  

- **Security & Configuration**  
  - Sensitive data never stored in the database.  
  - Secrets resolved dynamically at runtime via `.env` or Key Vault.  
  - Clear separation of metadata and operational configuration.  

- **Developer Experience**  
  - Simplified connector interfaces and improved error reporting.  
  - Cleaner model relationships for `SourceSystem` and `SourceDataset`.  
  - New code organization: `connectors.py`, `resolver.py`, `ref_builder.py`.

---

#### 🧭 Next: v0.3.0 will focus on automated target model generation and metadata lineage.

---

#### 🧠 Technical Notes  

- Fully decoupled from dbt profiles; all runtime connections and secrets resolved through `elevata_profiles.yaml`.  
- All SQL renderers return expressions as plain text templates — ready for downstream ELT tools or custom runners.  
- Surrogate Key hashing implemented engine-specifically (Postgres pgcrypto / MSSQL HASHBYTES).  
- Supports per-profile Overrides for multi-DB systems (e.g. `sap1`, `sap2` → `sap`).  
- Improved ordering, idempotency and error reporting in import and generation routines.  

---

## [0.1.1] - 2025-10-19

### 🪶 *UI Polish & PostgreSQL Power*

#### Overview  
A refinement release that makes **elevata** smoother and more flexible:  
a polished Django UI meets full **PostgreSQL** support — available via Docker or your own setup.  
Better visuals, faster workflows, and real database choice.

---

#### ✨ Improvements  

- **UI & UX Enhancements**  
  - Polished Django interface with cleaner layouts and spacing  
  - Improved responsiveness and overall visual consistency  
  - Optimized inline interactions and usability tweaks  

- **Database Support**  
  - Full PostgreSQL backend support  
  - Works with Docker Compose or a user-provided instance  
  - Updated settings for seamless configuration and migrations  

- **Developer Experience**  
  - Simplified environment setup (SQLite or PostgreSQL)  
  - Improved local testing through Docker Compose  

---

## [0.1.0] - 2025-10-14

### 🧩 *Metadata Management Comes Alive*

#### Overview  
This release marks a major milestone:  
**elevata** now provides a fully functional, metadata-driven web interface for managing your data platform’s core structures — built with **Django**, **HTMX**, and a clean **Bootstrap 5** theme.  

It’s the first end-to-end usable version:  
from user login → to inline editing → to audit tracking — all running securely and responsively out of the box.

---

#### 🚀 Highlights  

- **Complete Metadata Management Module**  
  - Inline CRUD with audit fields and user tracking  
  - Automatic URL & view generation for all models  

- **Modern UI & UX**  
  - Responsive elevata theme (Bootstrap 5.3)  
  - Autofocus & usability improvements for inline editing  
  - Unified form and grid styling  

- **Security & Reliability**  
  - Integrated authentication (login, logout, password change)  
  - Safe CSRF handling for all HTMX requests  

- **Developer Experience**  
  - Default SQLite backend for easy setup  
  - Clean folder structure: `core/`, `metadata/`, `dbt_project/`  
  - Ready for future extensions (PostgreSQL, dbt, etc.)

---

## [0.0.1] - 2025-10-06

### Added
- Project documentation scaffold (`README.md`)
- License file (`LICENSE`) under AGPLv3
- Notice file (`NOTICE.md`) for third-party licenses
- `.gitignore` for Python and dbt projects
- Placeholder `requirements/base.txt`
- Initial backend support for **DuckDB** (`requirements/duckdb.txt`)
- Base `dbt_project/` folder
