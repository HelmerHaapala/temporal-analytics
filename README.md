# Temporal Analytics Case Study: Documentation

An empirical study examining how analytics-layer data modeling changes when batch-based reconstruction is reduced or removed.
The observations are made across four thesis-aligned architectural patterns (A/B/C/E), plus a batch benchmark and a `ground_truth` reference. 

---

## Overview

The source is treated as a virtualized evolving operational database observed over time, not as a production CDC transport stream. 
Each architecture represents a different point on the spectrum of temporal correctness, consistency, and stability.

### Core Research Question

**How do choices in temporal semantics (closure, mutability, correctness guarantees) impact analytics results?**

### Key Architectures

| Architecture | Pattern | Temporal Closure | History Mutability | Results Stability |
|---|---|---|---|---|
| **BATCH Reference** | Traditional full-batch reload | Batch boundary | Immutable between reloads | High (reference baseline) |
| **A: Closed Snapshot Warehouse** | Hot/cold pull-window with bounded correction horizon | Micro-batch + hot pull window | Selective within hot partition | Medium (policy bounded) |
| **B: Open Evolving Stream** | Virtualized source + lagged reconciliation | Periodic reconcile checkpoints | Mutable served state | Medium (bounded inconsistency) |
| **C: Window-Bounded Stream** | Event-time windows on observed source changes | Window + watermark | Finalized windows immutable | Medium (per window) |
| **E: Virtual Semantic Snapshot** | Logical snapshot via semantic layer over observed source | Logical checkpoint closure | Fully recomputable from source observations | High (deterministic checkpoints) |

Different architectures answer the same question ("what were my sales?") differently:
- **BATCH reference**: "Sales from full deduplicated snapshot at each reload"
- **A**: "Sales from deduplicated rows in the hot pull window, while cold state stays fixed"
- **B**: "Sales from a lagged served state reconciled periodically from observed source changes"
- **C**: "Sales from observed source changes that fall in windows already finalized by watermark rules"
- **E**: "Sales from semantic-layer logical snapshot at each checkpoint"


---

## Source Model Assumption

The source in this case study is modeled as a **virtualized evolving operational database**:

- Rows evolve over time, and queries observe "latest visible state" at each pull boundary.
- The generated `events_source` table is a reproducible change-history artifact used to emulate those observations.
- This is intentionally different from a production CDC stream where every change is delivered as a transport event.

This assumption is especially important for Architecture A and for how B/C/E are interpreted:

- **A** models hot/cold pull-window behavior over an operational source, not full-history CDC restatement.
- **B**, **C**, and **E** ingest ordered source observations (virtualized pull semantics), then apply their own serving/finality policies.

---

## Architecture Patterns

### Benchmark: BATCH_reference (Traditional Full Batch)

**File**: `src/architectures/batch.py` (`BatchReference`)

**Pattern**: Full batch warehouse with continuous recomputation every cycle

**Key Characteristics**:
- Events processed in batches every N events (by arrival time)
- Full batch recomputation: all events so far are deduplicated and recomputed
- Once snapshot published, results are immutable within the batch
- No historical recomputation, each batch cycle represents truth at the time of processing

**Temporal Semantics**:
- **Analytical Truth**: Snapshot-correct (100% accurate for processed data within batch)
- **History Mutability**: Immutable snapshots (corrections cause churn between batches, not within)
- **Semantic Stabilization**: ETL/preprocessing with full recomputation
- **Analytical Abstraction**: Star schema (dedup events -> fact table -> query)
- **Semantic Consistency**: Deterministic and repeatable per snapshot


---


### Architecture A: Closed Snapshot Warehouse

**File**: `src/architectures/closed_with_backfill.py`

**Pattern**: Batch pull from virtualized operational source with hot/cold partitioning
- Source is queried periodically by arrival-time pull boundaries
- Hot partition is recomputed from latest visible source rows
- Cold partition remains immutable by policy
- Hot window size is configurable via `backfill-hot-days`

**Temporal Semantics**:
- **Analytical Truth**: Snapshot + bounded correction horizon
- **Temporal Closure**: Micro-batch + hot pull window
- **Semantic Consistency**: Deterministic per cycle inside policy bounds


### Architecture B: Open Evolving Stream

**File**: `src/architectures/open_evolving.py`

**Pattern**: Virtualized operational observations + lagged periodic reconciliation

**Key Characteristics**:
- Ordered source observations are ingested into an internal log
- Metrics are served from a periodically reconciled table
- Reconciliation is configurable by arrival-time cadence
- Propagation lag delays visibility of newest events
- Snapshots captured after each source observation boundary (arrival-time order)

**Temporal Semantics**:
- **Analytical Truth**: Provisional/evolving between reconcile checkpoints
- **Temporal Closure**: No hard closure during ingestion; checkpoint closure on reconcile
- **History Mutability**: Served state is mutable at reconcile points
- **Semantic Stabilization**: Reconcile policy over observed source changes
- **Analytical Abstraction**: Observation log + lagged served state
- **Semantic Consistency**: Bounded inconsistency (queries may see partial updates)

---

### Architecture C: Window-Bounded Stream

**File**: `src/architectures/window_bounded.py`

**Pattern**: Event-time windowing over virtualized source observations with watermark finalization

**Key Characteristics**:
- Events assigned to event-time windows (e.g., 1-hour)
- Watermark tracks event-time progress with grace period
- Windows before watermark marked final/immutable
- Source observations targeting already closed windows are dropped
- Snapshots captured after each source observation boundary (arrival-time order)

**Temporal Semantics**:
- **Analytical Truth**: Window-correct (accurate within window bounds)
- **Temporal Closure**: Event-time window + watermark
- **History Mutability**: Append-only (per window)
- **Semantic Stabilization**: Engine window semantics
- **Analytical Abstraction**: Windowed OLAP
- **Semantic Consistency**: Bounded (window-level)

---

### Architecture E: Virtual Semantic Snapshot

**File**: `src/architectures/virtual_semantic_snapshot.py`

**Pattern**: Logical semantic snapshots over observed operational source changes

**Key Characteristics**:
- Source observations are loaded as raw operational history
- Semantic layer exposes latest visible business state as-of a logical cutoff
- Snapshot cutoff is advanced at configurable checkpoint intervals
- No physical fact-table rewrite is required for each checkpoint

**Temporal Semantics**:
- **Analytical Truth**: Snapshot-correct at each logical checkpoint
- **Temporal Closure**: Logical snapshot cutoff
- **History Mutability**: Recomputable from observed source history
- **Semantic Stabilization**: Semantic layer definitions + cutoff contract
- **Analytical Abstraction**: Virtual semantic snapshot view
- **Semantic Consistency**: Deterministic and repeatable per checkpoint

---

## Measures & Metrics

### Measures (Business Metrics)

The case study computes **1 business measure**:

#### 1. Total Sales
- **Type**: Simple Aggregate
- **Calculation**: SUM(amount*quantity)
- **Grain**: Single row (all events)

## Data Model

### Source Schema

For simplicity, we assume a denormalized source schema with embedded dimensional values.

```sql
event_id           -- Unique event row identifier
sale_id            -- Business key (may repeat for corrections)
event_time         -- When the event occurred (business time)
arrival_time       -- When the event arrived at system (processing time)
product_name       -- Picked randomly from a group of possible values
category           -- The product category in which the product belongs to
region_name        -- Picked randomly from a group of possible values
country            -- The country that corresponds to the region
quantity           -- Units sold
amount             -- Revenue amount
is_update          -- Whether this is a correction to prior sale_id
is_deleted         -- Tombstone delete (soft delete)
```

### Synthetic Data Characteristics
- **Event count**: Configurable via `--n-events`
- **Update/delete ratio**: Configurable via `--anomaly-ratio` (split across update/delete/late)
- **Time span**: Configurable via `--time-span`
- **Late arrivals**: Some events arrive after event_time, with randomized delays
- **Products/regions**: Fixed small catalogs for reproducibility
- **Interpretation**: This table emulates an evolving operational source viewed over time; it is not a production transport stream.

