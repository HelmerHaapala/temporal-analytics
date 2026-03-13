# Analytics Architecture Simulation

This repository implements the analytics architecture simulation used in a thesis on temporal analytics behavior across data architecture patterns.

## Scope

The study compares one batch reference architecture, five thesis architectures (A-E), and a ground-truth implementation against the same synthetic source history.

- `ground_truth`
- `BATCH_reference`
- `A_closed_snapshot_warehouse`
- `B_open_evolving_stream`
- `C_window_bounded_stream`
- `D_log_consistent_htap`
- `E_virtual_semantic_snapshot`

Scenarios are defined in `src/scenarios/scenario_definitions.py`.

Simulation scenarios:

- `B0`: baseline reference scenario with fixed architecture parameters and no business targets
- `S1`: Live Sales Dashboard: freshness <= 10 minutes, live point-in-time accuracy >= 60%
- `S2`: Hourly Sales Dashboard: freshness <= 1 hour, live point-in-time accuracy >= 80%
- `S3`: Daily Sales Dashboard: freshness <= 24 hours, latest closed 24-hour window accuracy >= 99% after 8 hours
- `S4`: Weekly Management Review: freshness <= 7 days, latest closed 7-day window accuracy >= 99.9%, same-horizon restatement ratio <= 0%
- `S5`: Combined Requirements: freshness <= 10 minutes, live point-in-time accuracy >= 80%, latest closed 24-hour window accuracy >= 99% after 8 hours, latest closed 7-day window accuracy >= 99.9%, same-horizon restatement ratio <= 0%

## Metrics

The outcome tables track:

- Freshness (`freshness_max_minutes`, pass/fail vs target)
- Live point-in-time accuracy (`live_accuracy_ratio`, pass/fail vs target)
- Closed daily-window accuracy (`daily_window_accuracy_ratio`, pass/fail vs target)
- Closed weekly-window accuracy (`weekly_window_accuracy_ratio`, pass/fail vs target)
- Same-horizon restatement ratio (`stability_revision_ratio`, pass/fail vs target)
- Load volume (`rows_loaded_count`, `write_amplification`)
- Runtime context (`processing_time_seconds`)

`rows_loaded_count` is cumulative loaded rows (not distinct rows).
`S3` evaluates the latest closed 24-hour event-time window after an 8-hour settling delay.
`S4` evaluates the latest closed 7-day event-time window.
`S5` combines the live, daily-window, weekly-window, and restatement constraints in one single-model scenario.
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

- Each run executes one shared simulation with `B0` plus `S1`-`S5`.
- Simulation outputs are always written to `results/`.
- Tuning and scenario execution both use one hybrid parallelization strategy: scenarios are distributed across worker processes, and each scenario distributes architectures across its own worker pool.
- The terminal prints scenario x architecture status matrices during both tuning and execution.
- `B0` always uses one fixed baseline parameter set anchored to the generated source time span and exported to `parameters/baseline_params.json`.
- `S1`-`S5` reuse tuned parameters only when `parameters/scenarios_tuned_params.json` contains a profile matching `n-events`, `time-span`, and `anomaly-ratio`.
- If the cache is missing or incomplete for any business scenario, only those scenarios are retuned and then appended to the cache.
- Architectures with a direct freshness parameter apply the scenario freshness target directly. Architecture A and B keep their primary freshness cadence fixed and tune only secondary parameters, while Architecture C still searches its freshness-related settings.
- Where multiple remaining candidates pass, the slowest passing candidate is retained.
- Direct freshness mapping preserves slower-than-daily targets where the architecture exposes them. `BATCH_reference` is still clamped between 1 minute and 7 days.
- A minimum effective `time_span` of 14 days is enforced so weekly scenario evaluation remains non-trivial.

## Output Files

Simulation run:

- `results/scenarios_events_source.csv`
- `results/scenarios_snapshots.csv`
- `results/scenarios_outcomes.csv`
- `databases/scenarios_source.duckdb`
- `parameters/scenarios_tuned_params.json`
- `parameters/baseline_params.json`

`results/scenarios_outcomes.csv` contains one baseline reference scenario (`B0`) and five business scenarios (`S1`-`S5`).
