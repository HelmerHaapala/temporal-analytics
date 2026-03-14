"""
Tuning utilities for simulation scenario evaluation.
"""

import json
import math
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd

from scenarios.architecture_factory import (
    architecture_params_for_reporting,
    default_architecture_params,
)

MIN_HOURLY_CADENCE = 0.001
MAX_HOURLY_CADENCE = 24.0
MAX_TARGET_CADENCE_CANDIDATES = 5
BATCH_MIN_HOURS = 1.0 / 60.0
BATCH_MAX_HOURS = 7.0 * 24.0


def _scenario_has_any_accuracy_target(scenario: Any) -> bool:
    return any(
        getattr(scenario, field, None) is not None
        for field in (
            "live_accuracy_target_ratio",
            "daily_window_accuracy_target_ratio",
            "weekly_window_accuracy_target_ratio",
        )
    )


def architecture_initial(arch_name: str) -> str:
    if arch_name == "ground_truth":
        return "GT"
    if arch_name == "BATCH_reference":
        return "BATCH"
    if arch_name.startswith("A_"):
        return "A"
    if arch_name.startswith("B_"):
        return "B"
    if arch_name.startswith("C_"):
        return "C"
    if arch_name.startswith("D_"):
        return "D"
    if arch_name.startswith("E_"):
        return "E"
    return arch_name


def scenario_short_id(scenario: Any) -> str:
    scenario_id = str(getattr(scenario, "scenario_id", ""))
    if "_" in scenario_id:
        return scenario_id.split("_", 1)[0]
    return scenario_id


def cadence_hours_for_architecture(
    arch_name: str,
    params: Dict[str, float],
) -> float | None:
    if arch_name == "BATCH_reference":
        return float(params.get("closed_snapshot_hours", 0.0))
    if arch_name == "A_closed_snapshot_warehouse":
        return float(params.get("backfill_hot_refresh_hours", 0.0))
    if arch_name == "B_open_evolving_stream":
        return float(params.get("open_reconcile_every_hours", 0.0))
    if arch_name == "C_window_bounded_stream":
        return float(params.get("window_hours", 0.0))
    if arch_name == "D_log_consistent_htap":
        return float(params.get("htap_commit_every_hours", 0.0))
    if arch_name == "E_virtual_semantic_snapshot":
        return float(params.get("semantic_refresh_hours", 0.0))
    return None


def cadence_priority_hours(
    arch_name: str,
    params: Dict[str, float],
) -> float:
    cadence = cadence_hours_for_architecture(arch_name, params)
    return float(cadence) if cadence is not None else float("-inf")


def _dedupe_param_sets(param_sets: List[Dict[str, float]]) -> List[Dict[str, float]]:
    unique: List[Dict[str, float]] = []
    seen = set()
    for item in param_sets:
        key = json.dumps(item, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def _scenario_target_hours(scenario: Any) -> float | None:
    if scenario.freshness_target_minutes is None:
        return None
    return max(0.001, float(scenario.freshness_target_minutes) / 60.0)


def _fixed_target_hours(
    target_hours: float | None,
    *,
    min_hours: float = MIN_HOURLY_CADENCE,
    max_hours: float = MAX_HOURLY_CADENCE,
) -> float | None:
    if target_hours is None:
        return None
    return round(_clamp(float(target_hours), min_hours, max_hours), 6)


def _target_cadence_candidates(
    target_hours: float,
    max_hours: float = MAX_HOURLY_CADENCE,
) -> List[float]:
    anchors = [0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 24.0]
    scaled = [
        target_hours * 0.5,
        target_hours * 0.75,
        target_hours,
        target_hours * 1.25,
        target_hours * 1.5,
    ]
    pool = sorted(
        {
            round(_clamp(float(v), MIN_HOURLY_CADENCE, max_hours), 6)
            for v in anchors + scaled
        }
    )
    ranked = sorted(
        pool,
        key=lambda h: abs(math.log(max(h, MIN_HOURLY_CADENCE) / max(target_hours, MIN_HOURLY_CADENCE))),
    )
    selected = sorted(set(ranked[:MAX_TARGET_CADENCE_CANDIDATES]))
    return selected


def _non_target_cadence_candidates() -> List[float]:
    return [0.1, 0.5, 1.0, 6.0, 24.0]


def _rescue_candidates_for_architecture(
    scenario: Any,
    arch_name: str,
) -> List[Dict[str, float]]:
    base = default_architecture_params()
    target_hours = _scenario_target_hours(scenario)
    candidates: List[Dict[str, float]] = []

    def add_candidate(changes: Dict[str, float]) -> None:
        new_params = base.copy()
        new_params.update(changes)
        candidates.append(new_params)

    if arch_name == "C_window_bounded_stream":
        window_pool = (
            _target_cadence_candidates(target_hours)
            if target_hours is not None
            else [0.1, 0.25, 0.5, 1.0]
        )
        # Bias rescue to tighter windows where late finalization is less lossy.
        window_hours = sorted(set(window_pool + [0.1, 0.25, 0.5, 1.0]))[:4]
        for h in window_hours:
            add_candidate(
                {
                    "window_hours": h,
                    "allowed_lateness_hours": -1.0,
                }
            )
    elif arch_name == "D_log_consistent_htap" and _scenario_has_any_accuracy_target(scenario):
        add_candidate({"htap_commit_every_hours": 0.0})

    return _dedupe_param_sets(candidates)


def tuning_candidates_for_architecture(
    scenario: Any,
    arch_name: str,
) -> List[Dict[str, float]]:
    base = default_architecture_params()
    if arch_name == "ground_truth":
        return [base]

    candidates: List[Dict[str, float]] = []
    target_hours = _scenario_target_hours(scenario)

    def add_candidate(changes: Dict[str, float]) -> None:
        new_params = base.copy()
        new_params.update(changes)
        candidates.append(new_params)

    if arch_name == "BATCH_reference":
        fixed_hours = _fixed_target_hours(
            target_hours,
            min_hours=BATCH_MIN_HOURS,
            max_hours=BATCH_MAX_HOURS,
        )
        if fixed_hours is not None:
            add_candidate({"closed_snapshot_hours": fixed_hours})
        else:
            for h in _non_target_cadence_candidates():
                hours = _clamp(h, BATCH_MIN_HOURS, BATCH_MAX_HOURS)
                add_candidate({"closed_snapshot_hours": round(hours, 6)})
        return _dedupe_param_sets(candidates)

    cadence_hours = (
        _target_cadence_candidates(target_hours)
        if target_hours is not None
        else _non_target_cadence_candidates()
    )
    fixed_hours = _fixed_target_hours(
        target_hours,
        max_hours=max(MAX_HOURLY_CADENCE, target_hours or MAX_HOURLY_CADENCE),
    )

    if arch_name == "A_closed_snapshot_warehouse":
        refresh_hours = fixed_hours
        if refresh_hours is None:
            for h in cadence_hours:
                add_candidate({"backfill_hot_refresh_hours": h})
            refresh_hours = min(cadence_hours)
        else:
            add_candidate({"backfill_hot_refresh_hours": refresh_hours})
        add_candidate({"backfill_hot_refresh_hours": refresh_hours, "backfill_hot_hours": 48.0})
        add_candidate({"backfill_hot_refresh_hours": refresh_hours, "backfill_full_recompute_every_hours": 24.0})
        if scenario.require_weekly_window_accuracy:
            add_candidate({"backfill_hot_refresh_hours": refresh_hours, "backfill_hot_hours": 168.0})
            add_candidate(
                {
                    "backfill_hot_refresh_hours": refresh_hours,
                    "backfill_hot_hours": 168.0,
                    "backfill_full_recompute_every_hours": 12.0,
                }
            )

    elif arch_name == "B_open_evolving_stream":
        reconcile_hours = fixed_hours
        if reconcile_hours is None:
            for h in cadence_hours:
                add_candidate(
                    {
                        "open_reconcile_every_hours": h,
                        "open_propagation_lag_hours": 0.0,
                    }
                )
            reconcile_hours = min(cadence_hours)
        else:
            add_candidate(
                {
                    "open_reconcile_every_hours": reconcile_hours,
                    "open_propagation_lag_hours": 0.0,
                }
            )
        if reconcile_hours is not None:
            add_candidate(
                {
                    "open_reconcile_every_hours": reconcile_hours,
                    "open_propagation_lag_hours": max(0.0, min(0.5, reconcile_hours * 0.25)),
                }
            )

    elif arch_name == "C_window_bounded_stream":
        for h in cadence_hours:
            add_candidate({"window_hours": h, "allowed_lateness_hours": -1.0})

    elif arch_name == "D_log_consistent_htap":
        if fixed_hours is not None:
            add_candidate({"htap_commit_every_hours": fixed_hours})
        else:
            for h in cadence_hours:
                add_candidate({"htap_commit_every_hours": h})
        if scenario.require_live_accuracy:
            add_candidate({"htap_commit_every_hours": 0.0})

    elif arch_name == "E_virtual_semantic_snapshot":
        if fixed_hours is not None:
            add_candidate({"semantic_refresh_hours": fixed_hours})
        else:
            for h in cadence_hours:
                add_candidate({"semantic_refresh_hours": h})

    return _dedupe_param_sets(candidates)


def outcome_passes_target(scenario: Any, row: Dict[str, object]) -> bool:
    checks = []
    if scenario.freshness_target_minutes is not None:
        checks.append(bool(row.get("freshness_pass")))
    if scenario.live_accuracy_target_ratio is not None:
        checks.append(bool(row.get("live_accuracy_pass")))
    if scenario.daily_window_accuracy_target_ratio is not None:
        checks.append(bool(row.get("daily_window_accuracy_pass")))
    if scenario.weekly_window_accuracy_target_ratio is not None:
        checks.append(bool(row.get("weekly_window_accuracy_pass")))
    if scenario.stability_max_revision_ratio is not None:
        checks.append(bool(row.get("stability_pass")))
    return all(checks) if checks else True


def outcome_unmet_gap(scenario: Any, row: Dict[str, object]) -> float:
    gap = 0.0
    if scenario.freshness_target_minutes is not None:
        freshness = row.get("freshness_max_minutes")
        if freshness is None or pd.isna(freshness):
            gap += float("inf")
        else:
            gap += max(0.0, float(freshness) - float(scenario.freshness_target_minutes))
    if scenario.live_accuracy_target_ratio is not None:
        accuracy = row.get("live_accuracy_ratio")
        if accuracy is None or pd.isna(accuracy):
            gap += float("inf")
        else:
            gap += max(0.0, float(scenario.live_accuracy_target_ratio) - float(accuracy))
    if scenario.daily_window_accuracy_target_ratio is not None:
        accuracy = row.get("daily_window_accuracy_ratio")
        if accuracy is None or pd.isna(accuracy):
            gap += float("inf")
        else:
            gap += max(0.0, float(scenario.daily_window_accuracy_target_ratio) - float(accuracy))
    if scenario.weekly_window_accuracy_target_ratio is not None:
        accuracy = row.get("weekly_window_accuracy_ratio")
        if accuracy is None or pd.isna(accuracy):
            gap += float("inf")
        else:
            gap += max(0.0, float(scenario.weekly_window_accuracy_target_ratio) - float(accuracy))
    if scenario.stability_max_revision_ratio is not None:
        stability = row.get("stability_revision_ratio")
        if stability is None or pd.isna(stability):
            gap += float("inf")
        else:
            gap += max(0.0, float(stability) - float(scenario.stability_max_revision_ratio))
    return gap


def outcome_rank_key(scenario: Any, row: Dict[str, object]) -> Tuple[float, float, float]:
    passed = outcome_passes_target(scenario, row)
    unmet_gap = outcome_unmet_gap(scenario, row)
    stability = row.get("stability_revision_ratio")
    stability_val = float(stability) if stability is not None and not pd.isna(stability) else 0.0
    return (0.0 if passed else 1.0, unmet_gap, stability_val)


def tune_architectures_for_scenario(
    scenario: Any,
    source_conn: Any,
    source_table: str,
    architecture_order: List[str],
    measure_functions: Dict[str, object],
    run_architecture_once_fn: Callable[..., Tuple[pd.DataFrame, Dict[str, object]]],
    evaluation_context: Dict[str, object] | None = None,
    on_architecture_selected: Callable[[str, str, bool, Dict[str, float]], None] | None = None,
    logger: Callable[[str], None] | None = print,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    selected_snapshots: List[pd.DataFrame] = []
    selected_outcomes: List[Dict[str, object]] = []

    for arch_name in architecture_order:
        arch_initial = architecture_initial(arch_name)
        scenario_id = scenario_short_id(scenario)
        candidate_params = tuning_candidates_for_architecture(scenario, arch_name)
        rescue_candidates = _rescue_candidates_for_architecture(scenario, arch_name)
        existing_keys = {json.dumps(params, sort_keys=True) for params in candidate_params}
        rescue_candidates = [
            params
            for params in rescue_candidates
            if json.dumps(params, sort_keys=True) not in existing_keys
        ]
        total_candidate_count = len(candidate_params) + len(rescue_candidates)

        def arch_only_params(params: Dict[str, float]) -> Dict[str, float]:
            return architecture_params_for_reporting(params).get(arch_name, {})

        ordered_base = sorted(
            list(enumerate(candidate_params)),
            key=lambda item: (
                -cadence_priority_hours(arch_name, item[1]),
                item[0],
            ),
        )

        best_item: Dict[str, object] | None = None

        def evaluate_candidate(
            candidate_index: int,
            params: Dict[str, float],
        ) -> Dict[str, object]:
            snapshots_df, outcome = run_architecture_once_fn(
                scenario=scenario,
                source_conn=source_conn,
                source_table=source_table,
                arch_name=arch_name,
                params=params,
                measure_functions=measure_functions,
                evaluation_context=evaluation_context,
            )
            outcome["tuning_candidate_params"] = json.dumps(
                arch_only_params(params),
                sort_keys=True,
            )
            outcome["tuning_candidate_index"] = candidate_index
            outcome["tuning_candidate_count"] = total_candidate_count
            return {
                "rank": outcome_rank_key(scenario, outcome),
                "params": params,
                "snapshots": snapshots_df,
                "outcome": outcome,
            }

        for candidate_index, params in ordered_base:
            item = evaluate_candidate(candidate_index, params)
            if best_item is None or item["rank"] < best_item["rank"]:
                best_item = item
            if outcome_passes_target(scenario, item["outcome"]):
                best_item = item
                break

        if best_item is None:
            raise RuntimeError(f"No tuning candidates generated for architecture: {arch_name}")

        if not outcome_passes_target(scenario, best_item["outcome"]) and rescue_candidates:
            if logger is not None:
                logger(
                    f"    {arch_initial} - rescue search: evaluating "
                    f"{len(rescue_candidates)} architecture-specific fallback candidate(s)"
                )
            ordered_rescue = sorted(
                list(enumerate(rescue_candidates)),
                key=lambda item: (
                    -cadence_priority_hours(arch_name, item[1]),
                    item[0],
                ),
            )
            rescue_start_index = len(candidate_params)
            for rescue_offset, rescue_params in ordered_rescue:
                candidate_index = rescue_start_index + rescue_offset
                item = evaluate_candidate(candidate_index, rescue_params)
                if item["rank"] < best_item["rank"]:
                    best_item = item
                if outcome_passes_target(scenario, item["outcome"]):
                    best_item = item
                    break

        best_outcome = best_item["outcome"]
        best_outcome["tuning_selected"] = True
        best_outcome["tuning_met_target"] = outcome_passes_target(scenario, best_outcome)
        best_outcome["tuning_unmet_gap"] = outcome_unmet_gap(scenario, best_outcome)
        selected_arch_params = arch_only_params(best_item["params"])
        selected_snapshots.append(best_item["snapshots"])
        selected_outcomes.append(best_outcome)
        if on_architecture_selected is not None:
            on_architecture_selected(
                str(scenario.scenario_id),
                arch_name,
                bool(best_outcome["tuning_met_target"]),
                selected_arch_params,
            )
        elif logger is not None:
            if not best_outcome["tuning_met_target"]:
                logger(
                    f"    {scenario_id} - {arch_initial} - warning: no candidate met all active targets; "
                    "selected best-available candidate by unmet-gap ranking"
                )
            logger(
                f"    {scenario_id} - {arch_initial} - selected "
                f"(met_target={best_outcome['tuning_met_target']}) "
                f"params={json.dumps(selected_arch_params, sort_keys=True)}"
            )

    snapshots_df = pd.concat(selected_snapshots, ignore_index=True)
    outcomes_df = pd.DataFrame(selected_outcomes)
    return snapshots_df, outcomes_df
