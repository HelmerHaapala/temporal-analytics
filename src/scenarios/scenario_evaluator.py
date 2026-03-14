"""Scenario outcome evaluation utilities."""

import json
from typing import Dict, Optional

import pandas as pd

from scenarios.scenario_definitions import Scenario


ACCURACY_PASS_EPSILON = 1e-6
FRESHNESS_PASS_EPSILON_MINUTES = 1e-3
STABILITY_PASS_EPSILON = 1e-6
VALUE_CHANGE_EPSILON = 1e-6
ZERO_TRUTH_EPSILON = 1e-6


def source_period_truth_as_of(
    source_conn,
    source_table: str,
    observation_time: pd.Timestamp,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
) -> tuple[float, int]:
    result = source_conn.execute(
        f"""
        SELECT
            COALESCE(SUM(amount * quantity), 0.0) AS total_sales,
            COUNT(*) AS active_row_count
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY sale_id
                    ORDER BY arrival_time DESC, event_id DESC
                ) AS rn
            FROM {source_table}
            WHERE arrival_time <= ?
        ) latest
        WHERE rn = 1
          AND is_deleted = FALSE
          AND event_time >= ?
          AND event_time < ?
        """,
        [observation_time, period_start, period_end],
    ).fetchone()
    return float(result[0]), int(result[1])


def source_total_truth_as_of(
    source_conn,
    source_table: str,
    observation_time: pd.Timestamp,
) -> float:
    value = source_conn.execute(
        f"""
        SELECT
            COALESCE(SUM(amount * quantity), 0.0)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY sale_id
                    ORDER BY arrival_time DESC, event_id DESC
                ) AS rn
            FROM {source_table}
            WHERE arrival_time <= ?
        ) latest
        WHERE rn = 1
          AND is_deleted = FALSE
        """,
        [observation_time],
    ).fetchone()[0]
    return float(value)


def source_observation_grid(
    source_conn,
    source_table: str,
    observation_time: pd.Timestamp,
) -> pd.Series:
    arrival_df = source_conn.execute(
        f"""
        SELECT DISTINCT arrival_time
        FROM {source_table}
        WHERE arrival_time <= ?
        ORDER BY arrival_time
        """,
        [observation_time],
    ).df()
    if arrival_df.empty:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(arrival_df["arrival_time"], errors="coerce").dropna().reset_index(drop=True)


def normalize_arch_rows(arch_rows: pd.DataFrame) -> pd.DataFrame:
    if arch_rows.empty:
        return arch_rows.copy()
    normalized = arch_rows.copy()
    normalized["arrival_time"] = pd.to_datetime(normalized["arrival_time"], errors="coerce")
    normalized["max_visible_arrival_time"] = pd.to_datetime(
        normalized["max_visible_arrival_time"],
        errors="coerce",
    )
    normalized["value"] = pd.to_numeric(normalized["value"], errors="coerce")
    normalized = normalized.dropna(subset=["arrival_time"])
    sort_columns = ["arrival_time"]
    if "event_count" in normalized.columns:
        sort_columns.append("event_count")
    normalized = normalized.sort_values(sort_columns)
    normalized = normalized.drop_duplicates(subset=["arrival_time"], keep="last")
    return normalized.reset_index(drop=True)


def metric_rows_for_architecture(
    by_arch,
    arch_name: str,
    metric_name: str,
) -> pd.DataFrame:
    if arch_name not in by_arch.groups:
        return pd.DataFrame(columns=["arrival_time", "max_visible_arrival_time", "value"])
    rows = by_arch.get_group(arch_name).copy()
    if "metric" in rows.columns:
        rows = rows[rows["metric"] == metric_name].copy()
    return rows


def align_visibility_to_observation_grid(
    observation_grid: pd.Series,
    arch_rows: pd.DataFrame,
) -> pd.DataFrame:
    aligned = pd.DataFrame({"arrival_time": observation_grid})
    if aligned.empty:
        aligned["max_visible_arrival_time"] = pd.NaT
        return aligned
    if arch_rows.empty:
        aligned["max_visible_arrival_time"] = pd.NaT
        return aligned
    visibility_rows = arch_rows[["arrival_time", "max_visible_arrival_time"]].copy()
    visibility_rows = visibility_rows.sort_values("arrival_time")
    visibility_rows = visibility_rows.drop_duplicates(subset=["arrival_time"], keep="last")
    return pd.merge_asof(
        aligned,
        visibility_rows,
        on="arrival_time",
        direction="backward",
    )


def freshness_lag_metrics(aligned_visibility: pd.DataFrame) -> tuple[float | None, float | None, int]:
    if aligned_visibility.empty:
        return None, None, 0

    min_observation_time = pd.Timestamp(aligned_visibility["arrival_time"].iloc[0])
    lag_minutes = (
        aligned_visibility["arrival_time"] - aligned_visibility["max_visible_arrival_time"]
    ).dt.total_seconds() / 60.0
    unresolved_mask = aligned_visibility["max_visible_arrival_time"].isna()
    if unresolved_mask.any():
        lag_minutes.loc[unresolved_mask] = (
            aligned_visibility.loc[unresolved_mask, "arrival_time"] - min_observation_time
        ).dt.total_seconds() / 60.0

    lag_minutes = lag_minutes.clip(lower=0.0)
    lag_minutes = lag_minutes[lag_minutes.notna()]
    if lag_minutes.empty:
        return None, None, int(unresolved_mask.sum())

    return (
        float(lag_minutes.max()),
        float(lag_minutes.quantile(0.95)),
        int(unresolved_mask.sum()),
    )


def stability_restatement_metrics(arch_rows: pd.DataFrame) -> tuple[int, float, float, float, int]:
    if len(arch_rows) < 2:
        return 0, 0.0, 0.0, 0.0, 0

    series = arch_rows.copy().reset_index(drop=True)
    series["value_diff"] = series["value"].diff().abs()
    series["visible_changed"] = series["max_visible_arrival_time"].ne(
        series["max_visible_arrival_time"].shift(1)
    )
    transition_mask = series.index > 0
    same_horizon_mask = transition_mask & (~series["visible_changed"])
    restatement_mask = same_horizon_mask & (series["value_diff"] > VALUE_CHANGE_EPSILON)

    same_horizon_transition_count = int(same_horizon_mask.sum())
    restatement_count = int(restatement_mask.sum())
    restatement_ratio = (
        float(restatement_count) / float(same_horizon_transition_count)
        if same_horizon_transition_count > 0
        else 0.0
    )
    restatement_diffs = series.loc[restatement_mask, "value_diff"]
    total_abs_change = float(restatement_diffs.sum()) if not restatement_diffs.empty else 0.0
    max_abs_change = float(restatement_diffs.max()) if not restatement_diffs.empty else 0.0

    return (
        restatement_count,
        restatement_ratio,
        total_abs_change,
        max_abs_change,
        same_horizon_transition_count,
    )


def architecture_period_value(
    arch,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
) -> float:
    value = arch.conn.execute(
        f"""
        SELECT
            COALESCE(SUM(amount * quantity), 0.0)
        FROM {arch.table_name}
        WHERE is_deleted = FALSE
          AND event_time >= ?
          AND event_time < ?
        """,
        [period_start, period_end],
    ).fetchone()[0]
    return float(value)


def architecture_total_value(arch) -> float:
    value = arch.conn.execute(
        f"""
        SELECT
            COALESCE(SUM(amount * quantity), 0.0)
        FROM {arch.table_name}
        WHERE is_deleted = FALSE
        """
    ).fetchone()[0]
    return float(value)


def accuracy_ratio(truth_value: float, arch_value: float) -> tuple[float, float]:
    delta = arch_value - truth_value
    if abs(truth_value) <= ZERO_TRUTH_EPSILON:
        ratio = 1.0 if abs(arch_value) <= ZERO_TRUTH_EPSILON else 0.0
    else:
        ratio = max(0.0, 1.0 - (abs(delta) / abs(truth_value)))
    return delta, ratio


def closed_window_bounds(
    observation_time: pd.Timestamp,
    window_hours: float,
    delay_hours: float,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    period_end = observation_time - pd.Timedelta(hours=float(delay_hours))
    period_start = period_end - pd.Timedelta(hours=float(window_hours))
    return period_start, period_end


def build_scenario_evaluation_context(
    scenario: Scenario,
    source_conn,
    source_table: str,
) -> Dict[str, object]:
    observation_time = pd.Timestamp(
        source_conn.execute(f"SELECT MAX(arrival_time) FROM {source_table}").fetchone()[0]
    )
    observation_grid = source_observation_grid(
        source_conn=source_conn,
        source_table=source_table,
        observation_time=observation_time,
    )
    live_truth_value = None
    if scenario.require_live_accuracy:
        live_truth_value = source_total_truth_as_of(
            source_conn=source_conn,
            source_table=source_table,
            observation_time=observation_time,
        )

    daily_window_start = None
    daily_window_end = None
    daily_window_truth_value = None
    if scenario.require_daily_window_accuracy:
        daily_window_start, daily_window_end = closed_window_bounds(
            observation_time=observation_time,
            window_hours=24.0,
            delay_hours=float(scenario.daily_window_accuracy_delay_hours or 0.0),
        )
        daily_window_truth_value, _ = source_period_truth_as_of(
            source_conn=source_conn,
            source_table=source_table,
            observation_time=observation_time,
            period_start=daily_window_start,
            period_end=daily_window_end,
        )

    weekly_window_start = None
    weekly_window_end = None
    weekly_window_truth_value = None
    if scenario.require_weekly_window_accuracy:
        weekly_window_start, weekly_window_end = closed_window_bounds(
            observation_time=observation_time,
            window_hours=7.0 * 24.0,
            delay_hours=float(scenario.weekly_window_accuracy_delay_hours or 0.0),
        )
        weekly_window_truth_value, _ = source_period_truth_as_of(
            source_conn=source_conn,
            source_table=source_table,
            observation_time=observation_time,
            period_start=weekly_window_start,
            period_end=weekly_window_end,
        )

    return {
        "observation_time": observation_time,
        "observation_grid": observation_grid,
        "live_truth_value": live_truth_value,
        "daily_window_start": daily_window_start,
        "daily_window_end": daily_window_end,
        "daily_window_truth_value": daily_window_truth_value,
        "weekly_window_start": weekly_window_start,
        "weekly_window_end": weekly_window_end,
        "weekly_window_truth_value": weekly_window_truth_value,
    }


def evaluate_scenario(
    scenario: Scenario,
    source_conn,
    source_table: str,
    snapshots_df: pd.DataFrame,
    architectures: Dict[str, object],
    architecture_params: Dict[str, Dict[str, float]],
    evaluation_context: Optional[Dict[str, object]] = None,
) -> pd.DataFrame:
    outcomes = []
    by_arch = snapshots_df.groupby("architecture")
    context = evaluation_context or build_scenario_evaluation_context(
        scenario=scenario,
        source_conn=source_conn,
        source_table=source_table,
    )
    observation_time = pd.Timestamp(context["observation_time"])
    observation_grid = context["observation_grid"]
    live_truth_value = context["live_truth_value"]
    daily_window_start = context["daily_window_start"]
    daily_window_end = context["daily_window_end"]
    daily_window_truth_value = context["daily_window_truth_value"]
    weekly_window_start = context["weekly_window_start"]
    weekly_window_end = context["weekly_window_end"]
    weekly_window_truth_value = context["weekly_window_truth_value"]

    for arch_name, arch in architectures.items():
        outcome = {
            "scenario": scenario.scenario_id,
            "architecture": arch_name,
            "architecture_params": json.dumps(architecture_params.get(arch_name, {}), sort_keys=True),
            "processing_time_seconds": float(getattr(arch, "processing_time_seconds", 0.0)),
            "rows_loaded_count": int(getattr(arch, "rows_loaded_count", 0)),
            "freshness_target_minutes": scenario.freshness_target_minutes,
            "freshness_max_minutes": None,
            "freshness_p95_minutes": None,
            "freshness_unserved_points": None,
            "freshness_pass": None,
            "live_accuracy_target_ratio": scenario.live_accuracy_target_ratio,
            "live_accuracy_truth_value": live_truth_value,
            "live_accuracy_arch_value": None,
            "live_accuracy_delta": None,
            "live_accuracy_ratio": None,
            "live_accuracy_pass": None,
            "daily_window_accuracy_target_ratio": scenario.daily_window_accuracy_target_ratio,
            "daily_window_accuracy_delay_hours": scenario.daily_window_accuracy_delay_hours,
            "daily_window_start": daily_window_start,
            "daily_window_end": daily_window_end,
            "daily_window_truth_value": daily_window_truth_value,
            "daily_window_arch_value": None,
            "daily_window_delta": None,
            "daily_window_accuracy_ratio": None,
            "daily_window_accuracy_pass": None,
            "weekly_window_accuracy_target_ratio": scenario.weekly_window_accuracy_target_ratio,
            "weekly_window_accuracy_delay_hours": scenario.weekly_window_accuracy_delay_hours,
            "weekly_window_start": weekly_window_start,
            "weekly_window_end": weekly_window_end,
            "weekly_window_truth_value": weekly_window_truth_value,
            "weekly_window_arch_value": None,
            "weekly_window_delta": None,
            "weekly_window_accuracy_ratio": None,
            "weekly_window_accuracy_pass": None,
            "stability_target_revision_ratio": scenario.stability_max_revision_ratio,
            "stability_same_horizon_transitions": None,
            "stability_revision_count": None,
            "stability_revision_ratio": None,
            "stability_pass": None,
            "stability_evaluable": None,
            "stability_total_abs_change": None,
            "stability_max_abs_change": None,
            "observation_time": observation_time,
        }

        total_sales_rows = metric_rows_for_architecture(by_arch, arch_name, metric_name="total_sales")
        arch_rows = normalize_arch_rows(total_sales_rows)

        if scenario.stability_max_revision_ratio is not None:
            (
                revision_count,
                revision_ratio,
                total_abs_change,
                max_abs_change,
                transition_count,
            ) = stability_restatement_metrics(arch_rows)
            outcome["stability_same_horizon_transitions"] = transition_count
            outcome["stability_revision_count"] = revision_count
            outcome["stability_revision_ratio"] = revision_ratio
            outcome["stability_total_abs_change"] = total_abs_change
            outcome["stability_max_abs_change"] = max_abs_change
            outcome["stability_evaluable"] = True
            outcome["stability_pass"] = (
                revision_ratio <= (float(scenario.stability_max_revision_ratio) + STABILITY_PASS_EPSILON)
            )

        if not arch_rows.empty:
            aligned_visibility = align_visibility_to_observation_grid(
                observation_grid=observation_grid,
                arch_rows=arch_rows,
            )
            max_lag, p95_lag, unserved_points = freshness_lag_metrics(aligned_visibility)
            outcome["freshness_max_minutes"] = max_lag
            outcome["freshness_p95_minutes"] = p95_lag
            outcome["freshness_unserved_points"] = unserved_points
            if scenario.freshness_target_minutes is not None and max_lag is not None:
                outcome["freshness_pass"] = (
                    max_lag <= (float(scenario.freshness_target_minutes) + FRESHNESS_PASS_EPSILON_MINUTES)
                )

        if scenario.require_live_accuracy and live_truth_value is not None:
            arch_value = architecture_total_value(arch)
            delta, ratio = accuracy_ratio(live_truth_value, arch_value)
            outcome["live_accuracy_arch_value"] = arch_value
            outcome["live_accuracy_delta"] = delta
            outcome["live_accuracy_ratio"] = ratio
            outcome["live_accuracy_pass"] = (
                ratio + ACCURACY_PASS_EPSILON
            ) >= float(scenario.live_accuracy_target_ratio)

        if scenario.require_daily_window_accuracy and daily_window_truth_value is not None:
            arch_value = architecture_period_value(
                arch=arch,
                period_start=daily_window_start,
                period_end=daily_window_end,
            )
            delta, ratio = accuracy_ratio(daily_window_truth_value, arch_value)
            outcome["daily_window_arch_value"] = arch_value
            outcome["daily_window_delta"] = delta
            outcome["daily_window_accuracy_ratio"] = ratio
            outcome["daily_window_accuracy_pass"] = (
                ratio + ACCURACY_PASS_EPSILON
            ) >= float(scenario.daily_window_accuracy_target_ratio)

        if scenario.require_weekly_window_accuracy and weekly_window_truth_value is not None:
            arch_value = architecture_period_value(
                arch=arch,
                period_start=weekly_window_start,
                period_end=weekly_window_end,
            )
            delta, ratio = accuracy_ratio(weekly_window_truth_value, arch_value)
            outcome["weekly_window_arch_value"] = arch_value
            outcome["weekly_window_delta"] = delta
            outcome["weekly_window_accuracy_ratio"] = ratio
            outcome["weekly_window_accuracy_pass"] = (
                ratio + ACCURACY_PASS_EPSILON
            ) >= float(scenario.weekly_window_accuracy_target_ratio)

        outcomes.append(outcome)

    return pd.DataFrame(outcomes)
