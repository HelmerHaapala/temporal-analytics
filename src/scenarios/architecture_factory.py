"""
Architecture and DuckDB helper functions for simulation scenarios.
"""

from typing import Dict

import duckdb

from architectures import (  # noqa: E402
    BatchReference,
    ClosedSnapshotWithBackfill,
    GroundTruthArchitecture,
    LogConsistentHTAP,
    OpenEvolvingStream,
    VirtualSemanticSnapshot,
    WindowBoundedStream,
)


DEFAULT_ARCHITECTURE_PARAMS: Dict[str, float] = {
    "closed_snapshot_hours": 24.0,
    "backfill_hot_hours": 96.0,
    "backfill_hot_refresh_hours": 1.0,
    "backfill_full_recompute_every_hours": 240.0,
    "open_reconcile_every_hours": 1.0,
    "open_propagation_lag_hours": 0.0,
    "window_hours": 1.0,
    "allowed_lateness_hours": 240.0,
    "htap_commit_every_hours": 1.0,
    "semantic_refresh_hours": 1.0,
}

BASELINE_ARCHITECTURE_PARAM_OVERRIDES: Dict[str, Dict[str, float]] = {
    "ground_truth": {},
    "BATCH_reference": {
        "closed_snapshot_hours": 24.0,
    },
    "A_closed_snapshot_warehouse": {
        "backfill_hot_hours": 168.0,
        "backfill_hot_refresh_hours": 4.0,
        "backfill_full_recompute_every_hours": 336.0,
    },
    "B_open_evolving_stream": {
        "open_reconcile_every_hours": 6.0,
        "open_propagation_lag_hours": 1.0,
    },
    "C_window_bounded_stream": {
        "window_hours": 1.0,
        "allowed_lateness_hours": 168.0,
    },
    "D_log_consistent_htap": {
        "htap_commit_every_hours": 2.0,
    },
    "E_virtual_semantic_snapshot": {
        "semantic_refresh_hours": 6.0,
    },
}


def default_architecture_params() -> Dict[str, float]:
    return DEFAULT_ARCHITECTURE_PARAMS.copy()


def baseline_architecture_params(arch_name: str) -> Dict[str, float]:
    params = default_architecture_params()
    params.update(BASELINE_ARCHITECTURE_PARAM_OVERRIDES.get(arch_name, {}))
    return params


def build_source_conn_from_db(
    source_db_path: str,
    read_only: bool = True,
) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=source_db_path, read_only=read_only)


def build_single_architecture(
    arch_name: str,
    params: Dict[str, float],
) -> object:
    if arch_name == "ground_truth":
        return GroundTruthArchitecture()
    if arch_name == "BATCH_reference":
        return BatchReference(
            batch_window_hours=params["closed_snapshot_hours"],
        )
    if arch_name == "A_closed_snapshot_warehouse":
        return ClosedSnapshotWithBackfill(
            hot_partition_hours=params["backfill_hot_hours"],
            hot_partition_refresh_hours=params["backfill_hot_refresh_hours"],
            full_recompute_every_hours=params["backfill_full_recompute_every_hours"],
        )
    if arch_name == "B_open_evolving_stream":
        return OpenEvolvingStream(
            reconcile_every_hours=params["open_reconcile_every_hours"],
            propagation_lag_hours=params["open_propagation_lag_hours"],
        )
    if arch_name == "C_window_bounded_stream":
        return WindowBoundedStream(
            window_size_hours=params["window_hours"],
            allowed_lateness_hours=params["allowed_lateness_hours"],
        )
    if arch_name == "D_log_consistent_htap":
        return LogConsistentHTAP(
            commit_every_hours=params["htap_commit_every_hours"],
        )
    if arch_name == "E_virtual_semantic_snapshot":
        return VirtualSemanticSnapshot(
            semantic_refresh_hours=params["semantic_refresh_hours"],
        )
    raise ValueError(f"Unknown architecture: {arch_name}")


def architecture_params_for_reporting(params: Dict[str, float]) -> Dict[str, Dict[str, float]]:
    return {
        "ground_truth": {},
        "BATCH_reference": {
            "closed_snapshot_hours": params["closed_snapshot_hours"],
        },
        "A_closed_snapshot_warehouse": {
            "backfill_hot_hours": params["backfill_hot_hours"],
            "backfill_hot_refresh_hours": params["backfill_hot_refresh_hours"],
            "backfill_full_recompute_every_hours": params["backfill_full_recompute_every_hours"],
        },
        "B_open_evolving_stream": {
            "open_reconcile_every_hours": params["open_reconcile_every_hours"],
            "open_propagation_lag_hours": params["open_propagation_lag_hours"],
        },
        "C_window_bounded_stream": {
            "window_hours": params["window_hours"],
            "allowed_lateness_hours": params["allowed_lateness_hours"],
        },
        "D_log_consistent_htap": {
            "htap_commit_every_hours": params["htap_commit_every_hours"],
        },
        "E_virtual_semantic_snapshot": {
            "semantic_refresh_hours": params["semantic_refresh_hours"],
        },
    }
