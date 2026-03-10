"""
Scenario outcome evaluation utilities.
"""

import json
from typing import Dict

import pandas as pd

from scenarios.scenario_definitions import Scenario


ACCURACY_PASS_EPSILON = 1e-6
FRESHNESS_PASS_EPSILON_MINUTES = 1e-3
STABILITY_PASS_EPSILON = 1e-6
VALUE_CHANGE_EPSILON = 1e-6
ZERO_TRUTH_EPSILON = 1e-6


def month_bounds_for_last_full_month(observation_time: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    month_end = pd.Timestamp(observation_time).replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        nanosecond=0,
    )
    month_start = (month_end - pd.DateOffset(months=1)).replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        nanosecond=0,
    )
    return month_start, month_end


def source_last_full_month_truth(
    source_conn,
    source_table: str,
    observation_time: pd.Timestamp,
) -> tuple[pd.Timestamp, pd.Timestamp, float, int]:
    month_start, month_end = month_bounds_for_last_full_month(observation_time)
    total_sales, active_row_count = source_period_truth_as_of(
        source_conn=source_conn,
        source_table=source_table,
        observation_time=observation_time,
        period_start=month_start,
        period_end=month_end,
    )
    return month_start, month_end, total_sales, active_row_count


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


def architecture_last_full_month_value(
    arch,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
) -> float:
    return architecture_period_value(
        arch=arch,
        period_start=month_start,
        period_end=month_end,
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


def evaluate_scenario(
    scenario: Scenario,
    source_conn,
    source_table: str,
    snapshots_df: pd.DataFrame,
    architectures: Dict[str, object],
    architecture_params: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    outcomes = []
    by_arch = snapshots_df.groupby("architecture")

    observation_time = pd.Timestamp(
        source_conn.execute(f"SELECT MAX(arrival_time) FROM {source_table}").fetchone()[0]
    )
    observation_grid = source_observation_grid(
        source_conn=source_conn,
        source_table=source_table,
        observation_time=observation_time,
    )
    truth_total = source_total_truth_as_of(
        source_conn=source_conn,
        source_table=source_table,
        observation_time=observation_time,
    )
    if scenario.scenario_id == "S2":
        accuracy_period_end = observation_time - pd.Timedelta(hours=8)
        accuracy_period_start = accuracy_period_end - pd.Timedelta(hours=24)
        truth_total, _ = source_period_truth_as_of(
            source_conn=source_conn,
            source_table=source_table,
            observation_time=observation_time,
            period_start=accuracy_period_start,
            period_end=accuracy_period_end,
        )
    elif scenario.scenario_id == "S3":
        accuracy_period_end = observation_time - pd.Timedelta(days=1)
        accuracy_period_start = accuracy_period_end - pd.Timedelta(days=7)
        truth_total, _ = source_period_truth_as_of(
            source_conn=source_conn,
            source_table=source_table,
            observation_time=observation_time,
            period_start=accuracy_period_start,
            period_end=accuracy_period_end,
        )
    else:
        accuracy_period_start = None
        accuracy_period_end = None

    month_start = None
    month_end = None
    truth_monthly = None
    truth_monthly_row_count = None
    if scenario.require_monthly_accuracy:
        month_start, month_end, truth_monthly, truth_monthly_row_count = source_last_full_month_truth(
            source_conn=source_conn,
            source_table=source_table,
            observation_time=observation_time,
        )

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
            "stability_target_revision_ratio": scenario.stability_max_revision_ratio,
            "stability_same_horizon_transitions": None,
            "stability_revision_count": None,
            "stability_revision_ratio": None,
            "stability_pass": None,
            "stability_evaluable": None,
            "stability_total_abs_change": None,
            "stability_max_abs_change": None,
            "accuracy_target_ratio": scenario.accuracy_target_ratio,
            "accuracy_truth_value": truth_total,
            "accuracy_arch_value": None,
            "accuracy_delta": None,
            "accuracy_ratio": None,
            "accuracy_pass": None,
            "monthly_accuracy_target_ratio": scenario.monthly_accuracy_target_ratio,
            "monthly_truth_value": truth_monthly,
            "monthly_truth_row_count": truth_monthly_row_count,
            "monthly_evaluable": None,
            "monthly_arch_value": None,
            "monthly_delta": None,
            "monthly_accuracy": None,
            "monthly_pass": None,
            "observation_time": observation_time,
            "last_full_month_start": month_start,
            "last_full_month_end": month_end,
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
                outcome["stability_revision_ratio"]
                <= (float(scenario.stability_max_revision_ratio) + STABILITY_PASS_EPSILON)
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

        if scenario.scenario_id in {"S2", "S3"}:
            arch_total = architecture_period_value(
                arch=arch,
                period_start=accuracy_period_start,
                period_end=accuracy_period_end,
            )
        else:
            arch_total = architecture_total_value(arch)
        total_delta = arch_total - truth_total
        if abs(truth_total) <= ZERO_TRUTH_EPSILON:
            total_accuracy = 1.0 if abs(arch_total) <= ZERO_TRUTH_EPSILON else 0.0
        else:
            total_accuracy = max(0.0, 1.0 - (abs(total_delta) / abs(truth_total)))
        outcome["accuracy_arch_value"] = arch_total
        outcome["accuracy_delta"] = total_delta
        outcome["accuracy_ratio"] = total_accuracy
        if scenario.accuracy_target_ratio is not None:
            outcome["accuracy_pass"] = (
                total_accuracy + ACCURACY_PASS_EPSILON
            ) >= float(scenario.accuracy_target_ratio)

        if scenario.require_monthly_accuracy and truth_monthly is not None:
            if truth_monthly_row_count is not None and int(truth_monthly_row_count) <= 0:
                outcome["monthly_evaluable"] = False
                outcome["monthly_pass"] = False
            else:
                arch_value = architecture_last_full_month_value(
                    arch=arch,
                    month_start=month_start,
                    month_end=month_end,
                )
                delta = arch_value - truth_monthly
                if abs(truth_monthly) <= ZERO_TRUTH_EPSILON:
                    accuracy = 1.0 if abs(arch_value) <= ZERO_TRUTH_EPSILON else 0.0
                else:
                    accuracy = max(0.0, 1.0 - (abs(delta) / abs(truth_monthly)))
                outcome["monthly_evaluable"] = True
                outcome["monthly_arch_value"] = arch_value
                outcome["monthly_delta"] = delta
                outcome["monthly_accuracy"] = accuracy
                if scenario.monthly_accuracy_target_ratio is not None:
                    outcome["monthly_pass"] = (
                        accuracy + ACCURACY_PASS_EPSILON
                    ) >= float(scenario.monthly_accuracy_target_ratio)

        outcomes.append(outcome)

    return pd.DataFrame(outcomes)
