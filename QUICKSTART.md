# Quick Start

This file covers the implementation workflow only. Methodological justification and thesis-text framing are documented separately in `results/thesis_assets/implementation_framing_plan.md`.

## 1. Install

```bash
pip install -r requirements.txt
```

## 2. Run

```bash
python src/run_simulation.py
```

Default arguments:

- `--n-events 10000`
- `--time-span 95`
- `--anomaly-ratio 0.65`
- `--seed 42`

## 3. Key outputs

Scenarios mode:

- `results/scenarios_events_source.csv`
- `results/scenarios_snapshots.csv`
- `results/scenarios_outcomes.csv`
- `databases/scenarios_source.duckdb`
- `parameters/scenarios_tuned_params.json`

Baseline mode:

- `results/baseline_events_source.csv`
- `results/baseline_snapshots.csv`
- `results/baseline_outcomes.csv`
- `databases/baseline_source.duckdb`
- `parameters/baseline_params.json`

## 4. Thesis figures and tables

```bash
python scripts/business_scenarios_visualize.py --results-dir results --output-dir results/thesis_assets
```

Notes:

- `run_simulation.py` always runs both scenario types (scenarios + baseline).
- Simulation outputs are always written to `results/`.
- Scenarios mode reuses tuned parameters only when `parameters/scenarios_tuned_params.json` contains a profile matching `n-events`, `time-span`, and `anomaly-ratio`.
- The published repository currently ships cached scenario profiles for `10000` and `100000` events with `time-span=95` and `anomaly-ratio=0.65`.
- If the cache is missing scenarios, only missing scenarios are tuned and appended.
- Scenarios are defined in `src/scenarios/scenario_definitions.py` and include:
  `S1`, `S2`, `S3`, and `S4`.
- `processing_time_seconds` is environment-dependent; use it as context, not as primary evidence.
- Generated thesis assets are indexed in `results/thesis_assets/thesis_outputs.md`.
- Pass/fail checks use small tolerances, and stability restatements ignore tiny floating-point jitter.
