"""
Run the analytics architecture simulation on shared source data.
"""

from concurrent.futures import ProcessPoolExecutor
import queue as queue_module
import json
import os
from pathlib import Path

import pandas as pd

from scenarios.architecture_factory import (
    architecture_params_for_reporting,
    baseline_architecture_params,
    build_source_conn_from_db,
    default_architecture_params,
)
from scenarios.scenario_executor import run_one_scenario, run_scenario_with_params
from scenarios.scenario_definitions import ARCHITECTURE_ORDER, SCENARIOS, normalize_scenario_id
from scenarios.tuning_search import architecture_initial
from data_generator import TemporalEventGenerator  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = REPO_ROOT / "results"
PARAMETERS_DIR = REPO_ROOT / "parameters"
DATABASES_DIR = REPO_ROOT / "databases"

MIN_TIME_SPAN_DAYS_FOR_MONTHLY_EVAL = 45
BASELINE_RUN_TYPE = "baseline"
SCENARIOS_RUN_TYPE = "scenarios"


def _normalize_anomaly_ratio(value: float) -> float:
    return round(float(value), 6)


def _simulation_profile(
    n_events: int,
    time_span: int,
    anomaly_ratio: float,
) -> dict[str, float | int]:
    return {
        "n_events": int(n_events),
        "time_span": int(time_span),
        "anomaly_ratio": _normalize_anomaly_ratio(anomaly_ratio),
    }


def _simulation_profile_key(
    n_events: int,
    time_span: int,
    anomaly_ratio: float,
) -> str:
    profile = _simulation_profile(
        n_events=n_events,
        time_span=time_span,
        anomaly_ratio=anomaly_ratio,
    )
    return (
        f"n_events={profile['n_events']}|"
        f"time_span={profile['time_span']}|"
        f"anomaly_ratio={profile['anomaly_ratio']:.6f}"
    )


def _normalize_tuned_scenarios(raw_payload: dict) -> dict:
    return {
        normalize_scenario_id(scenario_id): params
        for scenario_id, params in raw_payload.items()
    }


def _load_tuned_params_cache(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}

    profiles_payload = payload.get("profiles")
    if not isinstance(profiles_payload, dict):
        return {}

    cache = {}
    for profile_key, profile_payload in profiles_payload.items():
        if not isinstance(profile_payload, dict):
            continue
        scenarios_payload = profile_payload.get("scenarios", {})
        if not isinstance(scenarios_payload, dict):
            continue
        simulation_parameters = profile_payload.get("simulation_parameters")
        cache[str(profile_key)] = {
            "simulation_parameters": (
                simulation_parameters if isinstance(simulation_parameters, dict) else None
            ),
            "scenarios": _normalize_tuned_scenarios(scenarios_payload),
        }
    return cache


def _save_tuned_params_cache(path: Path, tuned_params_cache: dict) -> None:
    serializable_profiles = {}
    for profile_key, profile_payload in tuned_params_cache.items():
        if not isinstance(profile_payload, dict):
            continue
        scenarios_payload = profile_payload.get("scenarios", {})
        if not isinstance(scenarios_payload, dict):
            continue
        serializable_profiles[str(profile_key)] = {
            "simulation_parameters": profile_payload.get("simulation_parameters"),
            "scenarios": scenarios_payload,
        }
    payload = {"profiles": serializable_profiles}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _normalize_scenario_params(raw_params_by_arch: dict) -> dict:
    normalized = {}
    for arch_name in ARCHITECTURE_ORDER:
        base = default_architecture_params()
        raw_arch = raw_params_by_arch.get(arch_name, {})
        if isinstance(raw_arch, dict):
            base.update(raw_arch)
        normalized[arch_name] = base
    return normalized


def _baseline_params_by_arch() -> dict:
    return {
        arch_name: baseline_architecture_params(arch_name)
        for arch_name in ARCHITECTURE_ORDER
    }


def _append_derived_metrics(
    outcomes_df: pd.DataFrame,
    source_row_count: int,
) -> pd.DataFrame:
    augmented = outcomes_df.copy()
    safe_source_rows = max(1, int(source_row_count))
    augmented["source_row_count"] = safe_source_rows
    if "rows_loaded_count" in augmented.columns:
        augmented["write_amplification"] = (
            pd.to_numeric(augmented["rows_loaded_count"], errors="coerce")
            / float(safe_source_rows)
        )
    else:
        augmented["write_amplification"] = None

    if {"freshness_target_minutes", "freshness_max_minutes"}.issubset(augmented.columns):
        target = pd.to_numeric(augmented["freshness_target_minutes"], errors="coerce")
        observed = pd.to_numeric(augmented["freshness_max_minutes"], errors="coerce")
        augmented["freshness_headroom_minutes"] = target - observed
    else:
        augmented["freshness_headroom_minutes"] = None

    if {"accuracy_target_ratio", "accuracy_ratio"}.issubset(augmented.columns):
        target = pd.to_numeric(augmented["accuracy_target_ratio"], errors="coerce")
        observed = pd.to_numeric(augmented["accuracy_ratio"], errors="coerce")
        augmented["accuracy_headroom_ratio"] = observed - target
    else:
        augmented["accuracy_headroom_ratio"] = None

    if {"monthly_accuracy_target_ratio", "monthly_accuracy"}.issubset(augmented.columns):
        target = pd.to_numeric(augmented["monthly_accuracy_target_ratio"], errors="coerce")
        observed = pd.to_numeric(augmented["monthly_accuracy"], errors="coerce")
        augmented["monthly_accuracy_headroom_ratio"] = observed - target
    else:
        augmented["monthly_accuracy_headroom_ratio"] = None

    if {"stability_target_revision_ratio", "stability_revision_ratio"}.issubset(augmented.columns):
        target = pd.to_numeric(augmented["stability_target_revision_ratio"], errors="coerce")
        observed = pd.to_numeric(augmented["stability_revision_ratio"], errors="coerce")
        augmented["stability_headroom_ratio"] = target - observed
    else:
        augmented["stability_headroom_ratio"] = None

    return augmented


def _parallel_worker_count() -> int:
    return max(1, int(os.cpu_count() or 1))


def _extract_raw_params_from_tuning_outcomes(tuning_outcomes_df: pd.DataFrame) -> dict:
    raw_arch_params = {}
    for _, row in tuning_outcomes_df.iterrows():
        params_json = row.get("tuning_candidate_params")
        if isinstance(params_json, str) and params_json.strip():
            arch_name = str(row["architecture"])
            raw_arch_params[arch_name] = json.loads(params_json)
    return raw_arch_params


def _tune_single_scenario_worker(
    scenario,
    source_db_path: str,
    source_table: str,
    architecture_parallel_workers: int,
    progress_queue=None,
) -> dict:
    source_conn = build_source_conn_from_db(
        source_db_path=source_db_path,
        read_only=True,
    )
    try:
        callback = None
        if progress_queue is not None:
            def callback(scenario_id: str, arch_name: str, met_target: bool, params: dict) -> None:
                progress_queue.put(
                    {
                        "scenario_id": scenario_id,
                        "architecture": arch_name,
                        "met_target": bool(met_target),
                        "params": dict(params),
                    }
                )
        _, tuning_outcomes_df = run_one_scenario(
            scenario=scenario,
            source_conn=source_conn,
            source_table=source_table,
            source_db_path=source_db_path,
            parallel_workers=architecture_parallel_workers,
            on_architecture_selected=callback,
            quiet=True,
        )
        return {
            "scenario_id": scenario.scenario_id,
            "raw_arch_params": _extract_raw_params_from_tuning_outcomes(tuning_outcomes_df),
            "met_target_by_arch": {
                str(row["architecture"]): bool(row.get("tuning_met_target"))
                for _, row in tuning_outcomes_df.iterrows()
            },
        }
    finally:
        source_conn.close()


def _render_tuning_status_lines(
    scenarios_to_tune: list,
    status_grid: dict[str, dict[str, str]],
) -> list[str]:
    arch_labels = [architecture_initial(name) for name in ARCHITECTURE_ORDER]
    lines = ["", "Tuning status:"]
    header = ["Scenario"] + arch_labels
    lines.append(" | ".join(f"{item:>8}" for item in header))
    lines.append("-" * len(lines[-1]))
    for scenario in scenarios_to_tune:
        scenario_id = str(scenario.scenario_id)
        row = [scenario_id]
        for arch_name in ARCHITECTURE_ORDER:
            row.append(status_grid[scenario_id][arch_name])
        lines.append(" | ".join(f"{item:>8}" for item in row))
    return lines


def _print_tuning_status_table(
    lines: list[str],
    previous_line_count: int,
) -> int:
    text = "\n".join(lines)
    if previous_line_count > 0:
        print(f"\x1b[{previous_line_count}F", end="")
    print(text, end="\n", flush=True)
    return len(lines)


def _materialize_source_db(
    source_conn,
    source_db_path: Path,
    table_names: list[str],
) -> None:
    if source_db_path.exists():
        source_db_path.unlink()
    escaped_path = str(source_db_path).replace("'", "''")
    source_conn.execute(f"ATTACH '{escaped_path}' AS source_db_file")
    try:
        for table_name in table_names:
            source_conn.execute(
                f"""
                CREATE OR REPLACE TABLE source_db_file.{table_name} AS
                SELECT * FROM {table_name}
                """
            )
    finally:
        source_conn.execute("DETACH source_db_file")


def _print_baseline_parameters(selected_params_by_arch: dict) -> None:
    print("\nBaseline parameters used:")
    for arch_name in ARCHITECTURE_ORDER:
        full_params = selected_params_by_arch.get(arch_name, {})
        arch_params = architecture_params_for_reporting(full_params).get(arch_name, {})
        print(f"  - {arch_name}: {json.dumps(arch_params, sort_keys=True)}")


def _baseline_params_for_reporting(selected_params_by_arch: dict) -> dict:
    return {
        arch_name: architecture_params_for_reporting(
            selected_params_by_arch.get(arch_name, {})
        ).get(arch_name, {})
        for arch_name in ARCHITECTURE_ORDER
    }


def run_scenarios(
    n_events: int = 1500,
    time_span: int = 45,
    anomaly_ratio: float = 0.65,
    seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PARAMETERS_DIR.mkdir(parents=True, exist_ok=True)
    DATABASES_DIR.mkdir(parents=True, exist_ok=True)
    parallel_workers = _parallel_worker_count()
    effective_time_span = max(int(time_span), MIN_TIME_SPAN_DAYS_FOR_MONTHLY_EVAL)
    profile_key = _simulation_profile_key(
        n_events=n_events,
        time_span=time_span,
        anomaly_ratio=anomaly_ratio,
    )
    simulation_parameters = _simulation_profile(
        n_events=n_events,
        time_span=time_span,
        anomaly_ratio=anomaly_ratio,
    )

    print("Generating shared source data once for all scenarios...")

    if effective_time_span != int(time_span):
        print(
            "Adjusted time_span from "
            f"{time_span} to {effective_time_span} days "
            "to keep monthly scenario evaluation non-trivial."
        )
    generator = TemporalEventGenerator(
        n_events=n_events,
        anomaly_ratio=anomaly_ratio,
        time_span_days=effective_time_span,
        seed=seed,
    )
    source_conn, _ = generator.create_source_table(table_name="events_source")
    source_csv_path = RESULTS_DIR / "scenarios_events_source.csv"
    source_conn.execute("SELECT * FROM events_source").df().to_csv(source_csv_path, index=False)
    total_rows = int(source_conn.execute("SELECT COUNT(*) FROM events_source").fetchone()[0])
    source_db_path = DATABASES_DIR / "scenarios_source.duckdb"
    _materialize_source_db(
        source_conn=source_conn,
        source_db_path=source_db_path,
        table_names=["events_source"],
    )
    print(f"Saved scenarios source database: {source_db_path}")

    tuned_params_path = PARAMETERS_DIR / "scenarios_tuned_params.json"
    tuned_params_cache = {}
    tuned_params_by_scenario = {}
    if tuned_params_path.exists():
        try:
            tuned_params_cache = _load_tuned_params_cache(tuned_params_path)
            profile_payload = tuned_params_cache.get(profile_key, {})
            tuned_params_by_scenario = dict(profile_payload.get("scenarios", {}))
            if tuned_params_by_scenario:
                print(
                    "\nUsing cached tuned parameters from "
                    f"{tuned_params_path} for {profile_key}"
                )
        except (json.JSONDecodeError, OSError) as exc:
            print(f"\nFailed to read tuned-parameter cache ({exc}); running tuning.")
            tuned_params_cache = {}
            tuned_params_by_scenario = {}

    missing_scenario_ids = [
        scenario.scenario_id
        for scenario in SCENARIOS
        if scenario.scenario_id not in tuned_params_by_scenario
    ]

    if missing_scenario_ids:
        print("\nTuning architecture parameters per scenario...")
        scenarios_to_tune = [s for s in SCENARIOS if s.scenario_id in missing_scenario_ids]
        scenario_workers = min(parallel_workers, len(scenarios_to_tune))
        architecture_parallel_workers = max(
            1,
            min(len(ARCHITECTURE_ORDER), parallel_workers // max(1, scenario_workers)),
        )
        print(
            "Using tuning mode: full-data, slowest-cadence-first, "
            f"scenario_workers={scenario_workers}, "
            f"architecture_workers={architecture_parallel_workers}"
        )

        status_grid = {
            str(scenario.scenario_id): {arch_name: "..." for arch_name in ARCHITECTURE_ORDER}
            for scenario in scenarios_to_tune
        }
        table_line_count = _print_tuning_status_table(
            _render_tuning_status_lines(scenarios_to_tune, status_grid),
            previous_line_count=0,
        )

        def update_status(scenario_id: str, arch_name: str, met_target: bool) -> None:
            if scenario_id not in status_grid or arch_name not in status_grid[scenario_id]:
                return
            status_grid[scenario_id][arch_name] = "PASS" if met_target else "FAIL"

        tuned_in_parallel = False
        if scenario_workers > 1:
            try:
                by_scenario = {}
                from multiprocessing import Manager
                with Manager() as manager:
                    progress_queue = manager.Queue()
                    with ProcessPoolExecutor(max_workers=scenario_workers) as executor:
                        future_by_scenario = {
                            executor.submit(
                                _tune_single_scenario_worker,
                                scenario,
                                str(source_db_path),
                                "events_source",
                                architecture_parallel_workers,
                                progress_queue,
                            ): scenario.scenario_id
                            for scenario in scenarios_to_tune
                        }
                        pending = set(future_by_scenario.keys())
                        while pending:
                            table_dirty = False
                            while True:
                                try:
                                    event = progress_queue.get(timeout=0.05)
                                except queue_module.Empty:
                                    break
                                update_status(
                                    scenario_id=str(event.get("scenario_id")),
                                    arch_name=str(event.get("architecture")),
                                    met_target=bool(event.get("met_target")),
                                )
                                table_dirty = True
                            done_now = [future for future in list(pending) if future.done()]
                            for future in done_now:
                                pending.remove(future)
                                item = future.result()
                                by_scenario[str(item["scenario_id"])] = item
                                for arch_name, met_target in dict(item.get("met_target_by_arch", {})).items():
                                    update_status(
                                        scenario_id=str(item["scenario_id"]),
                                        arch_name=str(arch_name),
                                        met_target=bool(met_target),
                                    )
                                    table_dirty = True
                            if table_dirty:
                                table_line_count = _print_tuning_status_table(
                                    _render_tuning_status_lines(scenarios_to_tune, status_grid),
                                    previous_line_count=table_line_count,
                                )
                for scenario in scenarios_to_tune:
                    scenario_id = scenario.scenario_id
                    item = by_scenario.get(scenario_id)
                    if item is None:
                        continue
                    tuned_params_by_scenario[scenario_id] = dict(item["raw_arch_params"])
                tuned_in_parallel = True
            except (PermissionError, OSError) as exc:
                print(f"Scenario-parallel tuning unavailable ({exc}); falling back to sequential")

        if not tuned_in_parallel:
            for scenario in scenarios_to_tune:
                def on_arch_selected(
                    scenario_id: str,
                    arch_name: str,
                    met_target: bool,
                    _params: dict,
                ) -> None:
                    nonlocal table_line_count
                    update_status(scenario_id=scenario_id, arch_name=arch_name, met_target=met_target)
                    table_line_count = _print_tuning_status_table(
                        _render_tuning_status_lines(scenarios_to_tune, status_grid),
                        previous_line_count=table_line_count,
                    )

                _, tuning_outcomes_df = run_one_scenario(
                    scenario=scenario,
                    source_conn=source_conn,
                    source_table="events_source",
                    source_db_path=str(source_db_path),
                    parallel_workers=parallel_workers,
                    on_architecture_selected=on_arch_selected,
                    quiet=True,
                )
                tuned_params_by_scenario[scenario.scenario_id] = _extract_raw_params_from_tuning_outcomes(
                    tuning_outcomes_df
                )
        tuned_params_cache[profile_key] = {
            "simulation_parameters": simulation_parameters,
            "scenarios": tuned_params_by_scenario,
        }
        _save_tuned_params_cache(tuned_params_path, tuned_params_cache)
        print(f"Saved tuned parameters to: {tuned_params_path} ({profile_key})")

    print("\nRunning scenarios with tuned parameters...")
    all_snapshots = []
    all_outcomes = []
    for scenario in SCENARIOS:
        raw_arch_params = tuned_params_by_scenario.get(scenario.scenario_id, {})
        selected_params_by_arch = _normalize_scenario_params(raw_arch_params)
        snapshots_df, outcomes_df = run_scenario_with_params(
            scenario=scenario,
            source_conn=source_conn,
            source_table="events_source",
            selected_params_by_arch=selected_params_by_arch,
            parallel_workers=parallel_workers,
            source_db_path=str(source_db_path),
        )
        all_snapshots.append(snapshots_df)
        all_outcomes.append(outcomes_df)

    snapshots_result = pd.concat(all_snapshots, ignore_index=True)
    outcomes_result = pd.concat(all_outcomes, ignore_index=True)
    snapshots_result["run_type"] = SCENARIOS_RUN_TYPE
    outcomes_result["run_type"] = SCENARIOS_RUN_TYPE
    outcomes_result = _append_derived_metrics(outcomes_result, source_row_count=total_rows)

    snapshots_path = RESULTS_DIR / "scenarios_snapshots.csv"
    outcomes_path = RESULTS_DIR / "scenarios_outcomes.csv"
    snapshots_result.to_csv(snapshots_path, index=False)
    outcomes_result.to_csv(outcomes_path, index=False)

    print(f"\nSaved snapshots: {snapshots_path}")
    print(f"Saved outcomes:  {outcomes_path}")
    return snapshots_result, outcomes_result


def run_baseline(
    n_events: int = 1500,
    time_span: int = 45,
    anomaly_ratio: float = 0.65,
    seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PARAMETERS_DIR.mkdir(parents=True, exist_ok=True)
    DATABASES_DIR.mkdir(parents=True, exist_ok=True)
    parallel_workers = _parallel_worker_count()
    effective_time_span = max(int(time_span), MIN_TIME_SPAN_DAYS_FOR_MONTHLY_EVAL)
    selected_params_by_arch = _baseline_params_by_arch()
    _print_baseline_parameters(selected_params_by_arch)

    print("\nGenerating shared source data for baseline run...")
    if parallel_workers > 1:
        print(
            "Baseline execution mode: parallel "
            f"(workers={parallel_workers})"
        )
    else:
        print(
            "Baseline execution mode: sequential "
            "(workers=1)"
        )
    if effective_time_span != int(time_span):
        print(
            "Adjusted time_span from "
            f"{time_span} to {effective_time_span} days "
            "to keep monthly scenario evaluation non-trivial."
        )
    generator = TemporalEventGenerator(
        n_events=n_events,
        anomaly_ratio=anomaly_ratio,
        time_span_days=effective_time_span,
        seed=seed,
    )
    source_conn, _ = generator.create_source_table(table_name="events_source")
    total_rows = int(source_conn.execute("SELECT COUNT(*) FROM events_source").fetchone()[0])
    source_csv_path = RESULTS_DIR / "baseline_events_source.csv"
    source_conn.execute("SELECT * FROM events_source").df().to_csv(source_csv_path, index=False)
    source_db_path = DATABASES_DIR / "baseline_source.duckdb"
    _materialize_source_db(
        source_conn=source_conn,
        source_db_path=source_db_path,
        table_names=["events_source"],
    )
    params_path = PARAMETERS_DIR / "baseline_params.json"
    params_for_reporting = _baseline_params_for_reporting(selected_params_by_arch)
    params_path.write_text(
        json.dumps(params_for_reporting, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"Saved baseline source data: {source_csv_path}")
    print(f"Saved baseline source database: {source_db_path}")
    print(f"Saved baseline parameters: {params_path}")

    print("\nRunning scenarios with fixed baseline parameters...")
    all_snapshots = []
    all_outcomes = []
    for scenario in SCENARIOS:
        snapshots_df, outcomes_df = run_scenario_with_params(
            scenario=scenario,
            source_conn=source_conn,
            source_table="events_source",
            selected_params_by_arch=selected_params_by_arch,
            parallel_workers=parallel_workers,
            source_db_path=str(source_db_path),
        )
        all_snapshots.append(snapshots_df)
        all_outcomes.append(outcomes_df)

    snapshots_result = pd.concat(all_snapshots, ignore_index=True)
    outcomes_result = pd.concat(all_outcomes, ignore_index=True)
    snapshots_result["run_type"] = BASELINE_RUN_TYPE
    outcomes_result["run_type"] = BASELINE_RUN_TYPE
    outcomes_result = _append_derived_metrics(outcomes_result, source_row_count=total_rows)

    snapshots_path = RESULTS_DIR / "baseline_snapshots.csv"
    outcomes_path = RESULTS_DIR / "baseline_outcomes.csv"
    snapshots_result.to_csv(snapshots_path, index=False)
    outcomes_result.to_csv(outcomes_path, index=False)

    print(f"\nSaved baseline snapshots: {snapshots_path}")
    print(f"Saved baseline outcomes:  {outcomes_path}")
    return snapshots_result, outcomes_result
