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

- `--n-events 100000`
- `--time-span 95`
- `--anomaly-ratio 0.65`
- `--seed 42`

## 3. Key outputs

Simulation outputs:

- `results/scenarios_events_source.csv`
- `results/scenarios_snapshots.csv`
- `results/scenarios_outcomes.csv`
- `databases/scenarios_source.duckdb`
- `parameters/scenarios_tuned_params.json`
- `parameters/baseline_params.json`

## 4. Thesis figures and tables

```bash
python scripts/business_scenarios_visualize.py --results-dir results --output-dir results/thesis_assets
```

Notes:

- `run_simulation.py` always runs one shared simulation with `B0` plus `S1`-`S4`.
- Simulation outputs are always written to `results/`.
- Tuning and scenario execution both use one hybrid parallelization strategy: scenarios are distributed across worker processes, and each scenario distributes architectures across its own worker pool.
- The terminal prints scenario x architecture status matrices during both tuning and execution.
- `B0` always uses one fixed baseline parameter set anchored to the generated source time span and exported to `parameters/baseline_params.json`.
- `S1`-`S4` reuse tuned parameters only when `parameters/scenarios_tuned_params.json` contains a profile matching `n-events`, `time-span`, and `anomaly-ratio`.
- The published repository currently ships cached scenario profiles for `10000` and `100000` events with `time-span=95` and `anomaly-ratio=0.65`.
- If the cache is missing business scenarios, only the missing scenarios are tuned and appended.
- Scenarios are defined in `src/scenarios/scenario_definitions.py` and include:
  `B0`, `S1`, `S2`, `S3`, and `S4`.
- `processing_time_seconds` is environment-dependent; use it as context, not as primary evidence.
- Generated thesis assets are indexed in `results/thesis_assets/thesis_outputs.md`.
- For `S2`, `accuracy_ratio` is evaluated on the closed 24-hour event window after an 8-hour settling delay.
- For `S3`, `accuracy_ratio` is evaluated on the closed 7-day event window after a 1-day settling delay.
- Pass/fail checks use small tolerances, and stability restatements ignore tiny floating-point jitter.
