"""
Execution and tuning orchestration helpers for simulation scenarios.
"""

from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable, Dict, List, Tuple
import json

import pandas as pd

from scenarios.architecture_factory import (
    architecture_params_for_reporting,
    build_source_conn_from_db,
    build_single_architecture,
)
from scenarios.scenario_evaluator import evaluate_scenario
from scenarios.scenario_definitions import ARCHITECTURE_ORDER, Scenario
from scenarios.tuning_search import tune_architectures_for_scenario

from measures import get_measures  # noqa: E402


def run_architecture_once(
    scenario: Scenario,
    source_conn,
    source_table: str,
    arch_name: str,
    params: Dict[str, float],
    measure_functions: Dict[str, object],
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    effective_params = dict(params)
    arch = build_single_architecture(arch_name, effective_params)
    snapshots = arch.process_source(
        source_conn=source_conn,
        source_table=source_table,
        measure_functions=measure_functions,
        arch_name=arch_name,
    )
    snapshots_df = pd.DataFrame(snapshots)
    snapshots_df["scenario"] = scenario.scenario_id

    arch_params = architecture_params_for_reporting(effective_params)
    outcome_df = evaluate_scenario(
        scenario=scenario,
        source_conn=source_conn,
        source_table=source_table,
        snapshots_df=snapshots_df,
        architectures={arch_name: arch},
        architecture_params={arch_name: arch_params.get(arch_name, {})},
    )
    outcome = outcome_df.iloc[0].to_dict()
    return snapshots_df, outcome


def _tune_single_architecture_worker(
    scenario: Scenario,
    arch_name: str,
    source_db_path: str,
    source_table: str,
    quiet: bool = False,
) -> Dict[str, object]:
    source_conn = build_source_conn_from_db(
        source_db_path=source_db_path,
        read_only=True,
    )
    try:
        measure_functions = get_measures()
        snapshots_df, outcomes_df = tune_architectures_for_scenario(
            scenario=scenario,
            source_conn=source_conn,
            source_table=source_table,
            architecture_order=[arch_name],
            measure_functions=measure_functions,
            run_architecture_once_fn=run_architecture_once,
            logger=None if quiet else print,
        )
        return {
            "architecture": arch_name,
            "snapshots_df": snapshots_df,
            "outcomes_df": outcomes_df,
        }
    finally:
        source_conn.close()


def _run_architecture_with_source_db(
    scenario: Scenario,
    source_db_path: str,
    source_table: str,
    arch_name: str,
    params: Dict[str, float],
) -> Dict[str, object]:
    source_conn = build_source_conn_from_db(
        source_db_path=source_db_path,
        read_only=True,
    )
    try:
        snapshots_df, outcome = run_architecture_once(
            scenario=scenario,
            source_conn=source_conn,
            source_table=source_table,
            arch_name=arch_name,
            params=params,
            measure_functions=get_measures(),
        )
        return {
            "architecture": arch_name,
            "snapshots_df": snapshots_df,
            "outcome": outcome,
        }
    finally:
        source_conn.close()


def run_one_scenario(
    scenario: Scenario,
    source_table: str,
    source_db_path: str,
    parallel_workers: int,
    on_architecture_selected: Callable[[str, str, bool, Dict[str, float]], None] | None = None,
    quiet: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not quiet:
        print(f"\nRunning {scenario.scenario_id}: {scenario.description}")
    if not source_db_path:
        raise ValueError("source_db_path is required for scenario tuning")

    max_workers = min(max(1, parallel_workers), len(ARCHITECTURE_ORDER))
    by_arch: Dict[str, Dict[str, object]] = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_by_arch = {
            executor.submit(
                _tune_single_architecture_worker,
                scenario,
                arch_name,
                source_db_path,
                source_table,
                quiet,
            ): arch_name
            for arch_name in ARCHITECTURE_ORDER
        }
        for future in as_completed(future_by_arch):
            item = future.result()
            by_arch[str(item["architecture"])] = item
            if on_architecture_selected is not None:
                outcome_df = item["outcomes_df"]
                if not outcome_df.empty:
                    row = outcome_df.iloc[0]
                    params_json = row.get("tuning_candidate_params")
                    selected_params: Dict[str, float] = {}
                    if isinstance(params_json, str) and params_json.strip():
                        selected_params = json.loads(params_json)
                    on_architecture_selected(
                        str(scenario.scenario_id),
                        str(row["architecture"]),
                        bool(row.get("tuning_met_target")),
                        selected_params,
                    )

    snapshots_parts = [by_arch[arch_name]["snapshots_df"] for arch_name in ARCHITECTURE_ORDER]
    outcomes_parts = [by_arch[arch_name]["outcomes_df"] for arch_name in ARCHITECTURE_ORDER]
    return (
        pd.concat(snapshots_parts, ignore_index=True),
        pd.concat(outcomes_parts, ignore_index=True),
    )


def _require_selected_params_by_arch(
    selected_params_by_arch: Dict[str, Dict[str, float]],
) -> None:
    missing = [
        arch_name
        for arch_name in ARCHITECTURE_ORDER
        if arch_name not in selected_params_by_arch
    ]
    if missing:
        missing_str = ", ".join(missing)
        raise KeyError(
            "selected_params_by_arch must contain a full parameter map for every "
            f"architecture; missing: {missing_str}"
        )


def run_scenario_with_params(
    scenario: Scenario,
    source_table: str,
    selected_params_by_arch: Dict[str, Dict[str, float]],
    parallel_workers: int = 1,
    source_db_path: str = "",
    on_architecture_completed: Callable[[str, str], None] | None = None,
    quiet: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not quiet:
        print(f"\nRunning {scenario.scenario_id}: {scenario.description}")
    _require_selected_params_by_arch(selected_params_by_arch)
    if not source_db_path:
        raise ValueError("source_db_path is required for scenario execution")
    max_workers = min(max(1, parallel_workers), len(ARCHITECTURE_ORDER))
    snapshots_parts: List[pd.DataFrame] = []
    outcome_rows: List[Dict[str, object]] = []
    by_arch: Dict[str, Dict[str, object]] = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_by_arch = {
            executor.submit(
                _run_architecture_with_source_db,
                scenario,
                source_db_path,
                source_table,
                arch_name,
                selected_params_by_arch[arch_name],
            ): arch_name
            for arch_name in ARCHITECTURE_ORDER
        }
        for future in as_completed(future_by_arch):
            item = future.result()
            by_arch[str(item["architecture"])] = item

    for arch_name in ARCHITECTURE_ORDER:
        item = by_arch[arch_name]
        snapshots_df = item["snapshots_df"]
        outcome = item["outcome"]
        snapshots_parts.append(snapshots_df)
        outcome_rows.append(outcome)
        if on_architecture_completed is not None:
            on_architecture_completed(str(scenario.scenario_id), arch_name)
        if not quiet:
            print(f"  - {arch_name} params={outcome['architecture_params']}")

    return pd.concat(snapshots_parts, ignore_index=True), pd.DataFrame(outcome_rows)
