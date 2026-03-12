# Analytics Architecture Simulation

This repository implements the analytics architecture simulation used in a thesis on temporal analytics behavior across data architecture patterns.

## Scope

The study compares one reference architecture, five thesis architectures (A-E), and a ground-truth implementation against the same synthetic source history.

- `ground_truth`
- `BATCH_reference`
- `A_closed_snapshot_warehouse`
- `B_open_evolving_stream`
- `C_window_bounded_stream`
- `D_log_consistent_htap`
- `E_virtual_semantic_snapshot`

In the current implementation, `B_open_evolving_stream` maintains an observed-change log and updates its served state through lagged incremental reconciliation.

Scenarios are defined in `src/scenarios/scenario_definitions.py`.

Current simulation scenarios:

- `B0`: baseline reference scenario with fixed architecture parameters and no business targets
- `S1`: Live Sales Dashboard: freshness <= 10 minutes, point-in-time accuracy >= 80%
- `S2`: Daily Sales Dashboard: freshness <= 24 hours, latest closed 24-hour window accuracy >= 99% after 8 hours
- `S3`: Weekly Partner Payout Report: freshness <= 7 days, latest closed 7-day window accuracy >= 99% after 1 day, same-horizon restatement ratio <= 10%
- `S4`: Monthly Management Review: freshness <= 7 days, monthly accuracy >= 99.9%, same-horizon restatement ratio <= 0%

## Metrics

The outcome tables track:

- Freshness (`freshness_max_minutes`, pass/fail vs target)
- Point-in-time accuracy (`accuracy_ratio`, pass/fail vs target)
- Stability (`stability_revision_ratio`, pass/fail vs target)
- Monthly accuracy when required (`monthly_accuracy`, pass/fail vs target)
- Load volume (`rows_loaded_count`, `write_amplification`)
- Runtime context (`processing_time_seconds`)

`rows_loaded_count` is cumulative loaded rows (not distinct rows).
For `S2`, `accuracy_ratio` is evaluated on the 24-hour event-time window that ended 8 hours before observation, not on the all-time total.
For `S3`, `accuracy_ratio` is evaluated on the 7-day event-time window that ended 1 day before observation, not on the all-time total.
Pass/fail checks use small numeric tolerances, and stability restatements ignore tiny floating-point jitter.

## Run

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the simulation:

```bash
python src/run_simulation.py
```

Default arguments:

- `--n-events 100000`
- `--time-span 95`
- `--anomaly-ratio 0.65`
- `--seed 42`

Notes:

- Each run executes one shared simulation with `B0` plus `S1`-`S4`.
- Simulation outputs are always written to `results/`.
- Tuning and scenario execution both use one hybrid parallelization strategy: scenarios are distributed across worker processes, and each scenario distributes architectures across its own worker pool.
- The terminal prints scenario x architecture status matrices during both tuning and execution.
- `B0` always uses one fixed baseline parameter set anchored to the generated source time span and exported to `parameters/baseline_params.json`.
- `S1`-`S4` reuse tuned parameters only when `parameters/scenarios_tuned_params.json` contains a profile matching `n-events`, `time-span`, and `anomaly-ratio`.
- The published repository ships cached scenario profiles for both `10000` and `100000` events, each with `time-span=95` and `anomaly-ratio=0.65`.
- If the cache is missing or incomplete for any business scenario, only those scenarios are retuned and then appended to the cache.
- Tuning chooses the slowest cadence that still meets active targets when a pass candidate exists.
- A minimum effective `time_span` of 45 days is enforced so monthly evaluation remains meaningful.
- Current checked-in `results/` outputs and thesis assets predate the incremental-reconcile change for Architecture B and should be treated as stale for B until the simulation is rerun.

## Output Files

Simulation run:

- `results/scenarios_events_source.csv`
- `results/scenarios_snapshots.csv`
- `results/scenarios_outcomes.csv`
- `databases/scenarios_source.duckdb`
- `parameters/scenarios_tuned_params.json`
- `parameters/baseline_params.json`

`results/scenarios_outcomes.csv` contains one baseline reference scenario (`B0`) and four business scenarios (`S1`-`S4`).

