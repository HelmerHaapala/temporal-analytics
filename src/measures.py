"""
Measures for temporal analytics case study.

"""

from typing import Any, Callable, Dict, List, Optional, Tuple

import duckdb
import pandas as pd


class Measures:
    
    @staticmethod
    def compute_total_sales(
        conn: duckdb.DuckDBPyConnection,
        table_name: str = "events"
    ) -> pd.DataFrame:
        """
        Total sales across entire dataset.
        """
        result = conn.execute(f"""
            SELECT
                SUM(amount * quantity) as total_sales,
                COUNT(*) as event_count
            FROM {table_name}
            WHERE is_deleted = FALSE
        """).fetchdf()
        
        return result


def get_measures() -> Dict[str, callable]:
    """
    Return a mapping of measure_name --> computation function.
    
    Returns:
        Dict[str, callable]: Measure name --> computation function
    """
    return {
        'total_sales': lambda conn, table='events': Measures.compute_total_sales(conn, table),
    }


def summarize_measure_snapshot(
    measure_name: str,
    snapshot: pd.DataFrame,
) -> Tuple[Optional[float], Optional[str]]:
    if snapshot.empty:
        return None, None

    if measure_name == "total_sales" and "total_sales" in snapshot.columns:
        return float(snapshot["total_sales"].sum()), "Total Sales"

    numeric_cols = snapshot.select_dtypes(include=["number"]).columns.tolist()
    numeric_cols = [
        col
        for col in numeric_cols
        if not col.endswith("_id") and col not in {"event_count", "revenue_rank"}
    ]
    if numeric_cols:
        return float(snapshot[numeric_cols[0]].mean()), numeric_cols[0]

    return None, None


def capture_measure_snapshots(
    architectures: Dict[str, Any],
    measure_functions: Dict[str, Callable[..., pd.DataFrame]],
    event_count: int,
    arrival_time: Optional[pd.Timestamp] = None,
) -> List[Dict[str, Any]]:
    measure_snapshots: List[Dict[str, Any]] = []

    for arch_name, arch in architectures.items():
        for measure_name, compute_fn in measure_functions.items():
            try:
                snapshot = compute_fn(arch.conn, arch.table_name)
                value, value_label = summarize_measure_snapshot(measure_name, snapshot)
                if value is None:
                    continue

                snapshot_record = {
                    "event_count": event_count,
                    "metric": measure_name,
                    "architecture": arch_name,
                    "value": value,
                    "value_label": value_label,
                }
                if arrival_time is not None:
                    snapshot_record["arrival_time"] = arrival_time
                measure_snapshots.append(snapshot_record)
            except Exception as exc:
                print(
                    f"    Warning: {arch_name}.{measure_name} snapshot failed at {event_count}: {exc}"
                )

    return measure_snapshots