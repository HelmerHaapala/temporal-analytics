# Quick Start Guide

## Installation

```bash
pip install -r requirements.txt
```

## Run the Case Study

```bash
python src/run_case_study.py
```

This generates synthetic operational change history, runs all architecture pipelines (including the `ground_truth` reference), and records measure snapshots.

Source model note: `events_source` emulates a virtualized evolving operational DB observed over time. It is not a production CDC transport stream.

Common CLI parameters (from `src/run_case_study.py`) include:
- `--n-events`
- `--time-span`
- `--anomaly-ratio`
- `--closed-snapshot-days`
- `--backfill-hot-days`
- `--backfill-hot-refresh-hours`
- `--backfill-full-recompute-every-days`
- `--open-reconcile-every-hours`
- `--open-propagation-lag-hours`
- `--window-hours`
- `--allowed-lateness-days`
- `--semantic-refresh-hours`

Example:

```bash
python src/run_case_study.py --n-events 100000 --anomaly-ratio 0.30 --window-hours 6
```

## View Results

**Snapshots file**: `results/measure_snapshots.csv`
**Source history file**: `results/events_source.csv`

`results/` is generated at runtime and ignored by `.gitignore`.

## What It Does

1. Generates synthetic operational change history (counts and anomaly ratio are configurable)
2. Runs 6 architecture pipelines (5 compared outputs + `ground_truth` reference):
   - **ground_truth**: Reference truth snapshot for deviation comparisons
   - **BATCH_reference**: Traditional full-batch benchmark
   - **A_closed_snapshot_warehouse**: Hot/cold pull-window over virtualized source state
   - **B_open_evolving_stream**: Lagged periodic reconciliation from observed source changes
   - **C_window_bounded_stream**: Event-time windows with watermark finalization
   - **E_virtual_semantic_snapshot**: Logical semantic-layer snapshots from observed source changes
3. Measures 1 value for each architecture:
   - **total_sales**: Baseline control (simple aggregate)


