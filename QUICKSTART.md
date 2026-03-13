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

## 3. Business scenarios

- `S1`: Live Sales Dashboard
- `S2`: Hourly Sales Dashboard
- `S3`: Daily Sales Dashboard
- `S4`: Weekly Management Review
- `S5`: Combined Requirements

## 4. Key outputs

Simulation outputs:

- `results/scenarios_events_source.csv`
- `results/scenarios_snapshots.csv`
- `results/scenarios_outcomes.csv`
- `databases/scenarios_source.duckdb`
- `parameters/scenarios_tuned_params.json`
- `parameters/baseline_params.json`