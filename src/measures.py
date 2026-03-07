"""Measure helpers used by the architectures during snapshot capture."""

from typing import Any, Callable, Dict, Optional, Tuple

import duckdb
import pandas as pd


def compute_total_sales(
    conn: duckdb.DuckDBPyConnection,
    table_name: str = "events",
) -> pd.DataFrame:
    return conn.execute(
        f"""
        SELECT
            COALESCE(SUM(amount * quantity), 0.0) AS total_sales,
            COUNT(*) AS event_count
        FROM {table_name}
        WHERE is_deleted = FALSE
        """
    ).fetchdf()


def get_measures() -> Dict[str, Callable[..., pd.DataFrame]]:
    return {
        "total_sales": compute_total_sales,
    }


def summarize_measure_snapshot(
    measure_name: str,
    snapshot: pd.DataFrame,
) -> Tuple[Optional[float], Optional[str]]:
    if snapshot.empty:
        return None, None

    if "value" in snapshot.columns:
        value = snapshot["value"].iloc[0]
        if value is None or pd.isna(value):
            return None, None
        return float(value), "Value"

    if measure_name == "total_sales" and "total_sales" in snapshot.columns:
        return float(snapshot["total_sales"].sum()), "Total Sales"

    numeric_cols = snapshot.select_dtypes(include=["number"]).columns.tolist()
    numeric_cols = [
        col
        for col in numeric_cols
        if not col.endswith("_id") and col not in {"event_count", "revenue_rank"}
    ]
    if not numeric_cols:
        return None, None
    return float(snapshot[numeric_cols[0]].mean()), numeric_cols[0]


def max_visible_arrival_time(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
) -> Optional[pd.Timestamp]:
    value = conn.execute(f"SELECT MAX(arrival_time) FROM {table_name}").fetchone()[0]
    if value is None:
        return None
    return pd.Timestamp(value)


def visible_arrival_time_for_arch(arch: Any) -> Optional[pd.Timestamp]:
    table_visible = max_visible_arrival_time(arch.conn, arch.table_name)
    cutoff = getattr(arch, "freshness_cutoff_time", None)
    if cutoff is None:
        return table_visible

    cutoff_ts = pd.Timestamp(cutoff)
    if table_visible is None:
        return cutoff_ts
    return cutoff_ts if cutoff_ts >= table_visible else table_visible


def capture_measure_snapshots(
    architectures: Dict[str, Any],
    measure_functions: Dict[str, Callable[..., pd.DataFrame]],
    event_count: int,
    arrival_time: Optional[pd.Timestamp] = None,
) -> list[dict]:
    snapshots: list[dict] = []

    for arch_name, arch in architectures.items():
        for measure_name, compute_fn in measure_functions.items():
            try:
                snapshot = compute_fn(arch.conn, arch.table_name)
                value, value_label = summarize_measure_snapshot(measure_name, snapshot)
                if value is None:
                    continue
                record = {
                    "event_count": event_count,
                    "metric": measure_name,
                    "architecture": arch_name,
                    "value": value,
                    "value_label": value_label,
                    "max_visible_arrival_time": visible_arrival_time_for_arch(arch),
                }
                if arrival_time is not None:
                    record["arrival_time"] = arrival_time
                if not snapshot.empty and "period_start" in snapshot.columns:
                    record["period_start"] = snapshot["period_start"].iloc[0]
                if not snapshot.empty and "period_end" in snapshot.columns:
                    record["period_end"] = snapshot["period_end"].iloc[0]
                snapshots.append(record)
            except Exception as exc:
                print(f"    Warning: {arch_name}.{measure_name} snapshot failed at {event_count}: {exc}")

    return snapshots
