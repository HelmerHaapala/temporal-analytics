# Analytics Architecture Simulation

This repository implements the analytics architecture simulation used in a thesis on temporal analytics behavior across data architecture patterns.

## Documentation split

- This repository documents the `how`: source generation, architecture implementations, scenario execution, evaluation, and asset generation.
- The thesis documents the `why`: why the simulation was designed this way, why the scenarios were selected, and how the findings are interpreted.
- The current thesis-facing writing plan lives in `results/thesis_assets/implementation_framing_plan.md`.

## Scope

The study compares one reference architecture, five thesis architectures (A-E), and a ground-truth implementation against the same synthetic source history.

- `ground_truth`
- `BATCH_reference`
- `A_closed_snapshot_warehouse`
- `B_open_evolving_stream`
- `C_window_bounded_stream`
- `D_log_consistent_htap`
- `E_virtual_semantic_snapshot`

Scenarios are defined in `src/scenarios/scenario_definitions.py`.

Current simulation scenarios:

- `B0`: baseline reference scenario with fixed architecture parameters and no business targets
- `S1`: freshness <= 10 minutes, point-in-time accuracy >= 80%
- `S2`: freshness <= 8 hours, point-in-time accuracy >= 90%
- `S3`: freshness <= 24 hours, monthly accuracy >= 99%
- `S4`: freshness <= 6 hours, point-in-time accuracy >= 97%, same-horizon restatement ratio <= 10%

## Metrics

The outcome tables track:

- Freshness (`freshness_max_minutes`, pass/fail vs target)
- Point-in-time accuracy (`accuracy_ratio`, pass/fail vs target)
- Stability (`stability_revision_ratio`, pass/fail vs target)
- Monthly accuracy when required (`monthly_accuracy`, pass/fail vs target)
- Load volume (`rows_loaded_count`, `write_amplification`)
- Runtime context (`processing_time_seconds`)

`rows_loaded_count` is cumulative loaded rows (not distinct rows).
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
- `B0` always uses one fixed baseline parameter set anchored to the generated source time span and exported to `parameters/baseline_params.json`.
- `S1`-`S4` reuse tuned parameters only when `parameters/scenarios_tuned_params.json` contains a profile matching `n-events`, `time-span`, and `anomaly-ratio`.
- The published repository currently ships cached scenario profiles for `10000` and `100000` events with `time-span=95` and `anomaly-ratio=0.65`.
- If the cache is missing business scenarios, only the missing scenarios are tuned and then appended to the cache.
- Tuning chooses the slowest cadence that still meets active targets when a pass candidate exists.
- A minimum effective `time_span` of 45 days is enforced so monthly evaluation remains meaningful.

## Output Files

Simulation run:

- `results/scenarios_events_source.csv`
- `results/scenarios_snapshots.csv`
- `results/scenarios_outcomes.csv`
- `databases/scenarios_source.duckdb`
- `parameters/scenarios_tuned_params.json`
- `parameters/baseline_params.json`

`results/scenarios_outcomes.csv` contains one baseline reference scenario (`B0`) and four business scenarios (`S1`-`S4`).

## Thesis Assets

Generate all thesis figures and tables in one run:

```bash
python scripts/business_scenarios_visualize.py --results-dir results --output-dir results/thesis_assets
```

Outputs are written under:

- `results/thesis_assets/figures/`
- `results/thesis_assets/tables/`
- `results/thesis_assets/thesis_outputs.md` (index of generated assets)

Notes:

- `thesis_outputs.md` is an inventory of generated assets, not a recommendation for which figures belong in the thesis body.
- The current thesis-body selection plan is documented in `results/thesis_assets/implementation_framing_plan.md`.
