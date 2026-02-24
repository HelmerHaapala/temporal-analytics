"""
Main orchestration script for the temporal analytics case study.
"""

import os

import pandas as pd

from data_generator import TemporalEventGenerator
from measures import get_measures
from architectures import (
    BatchReference,
    ClosedSnapshotWithBackfill,
    GroundTruthArchitecture,
    OpenEvolvingStream,
    VirtualSemanticSnapshot,
    WindowBoundedStream,
)


def run_case_study(
    n_events: int,
    time_span: int,
    anomaly_ratio: float,
    closed_snapshot_days: float,
    hot_partition_days: float,
    hot_partition_refresh_hours: float,
    full_recompute_every_days: float,
    open_reconcile_every_hours: float,
    open_propagation_lag_hours: float,
    window_size_hours: float,
    allowed_lateness_days: float,
    semantic_refresh_hours: float,
):
    print("\nSTART")
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True) # Ensure output directory exists

    architectures = { #Initialize all architectures
        "ground_truth": GroundTruthArchitecture(),
        "BATCH_reference": BatchReference(batch_window_days=closed_snapshot_days,),
        "A_closed_snapshot_warehouse": ClosedSnapshotWithBackfill(hot_partition_days=hot_partition_days,hot_partition_refresh_hours=hot_partition_refresh_hours,full_recompute_every_days=full_recompute_every_days,),
        "B_open_evolving_stream": OpenEvolvingStream(reconcile_every_hours=open_reconcile_every_hours,propagation_lag_hours=open_propagation_lag_hours,),
        "C_window_bounded_stream": WindowBoundedStream(window_size_hours=window_size_hours,allowed_lateness_days=allowed_lateness_days,),
        "E_virtual_semantic_snapshot": VirtualSemanticSnapshot(semantic_refresh_hours=semantic_refresh_hours,),
    }

    print("\n[1/3] Generating synthetic operational change history...")
    print(f"  Generating {n_events} events over a {time_span}-day span with {anomaly_ratio*100:.1f}% anomalies...")
    
    #Run the generator to create the source table in DuckDB
    gen = TemporalEventGenerator(
        n_events=n_events,
        anomaly_ratio=anomaly_ratio,
        time_span_days=time_span,
    )
    source_table = "events_source"
    source_conn, _ = gen.create_source_table(table_name=source_table)

    #Save source events to CSV for reference
    events_path = f"{output_dir}/events_source.csv"
    source_conn.execute(f"SELECT * FROM {source_table}").df().to_csv(events_path, index=False)
    print(f"  Saved source events to {events_path}")

    print(f"\n[2/3] Running all {len(architectures)} architectures...")

    #Initialize measures
    measure_functions = get_measures()
    
    #Process each architecture and capture measure snapshots
    measure_snapshots: list[dict] = []
    for arch_name, arch in architectures.items():
        print(f"  Processing architecture: {arch_name}...")
        measure_snapshots.extend(
            arch.process_source(
                source_conn=source_conn,
                source_table=source_table,
                measure_functions=measure_functions,
                arch_name=arch_name,
            )
        )
        print(f"\t  {arch_name} processed successfully in {arch.processing_time_seconds:.3f} seconds")

    print(f"  All {len(architectures)} architectures processed")

    print("\n[3/3] Saving measure snapshot results...")

    snapshots_df = pd.DataFrame(measure_snapshots)
    if not snapshots_df.empty:
        snapshots_path = f"{output_dir}/measure_snapshots.csv"
        snapshots_df.to_csv(snapshots_path, index=False)
        print(f"  Saved measure snapshots to {snapshots_path}")

    print("\nCOMPLETED")

    return architectures, measure_snapshots


if __name__ == "__main__":
    import argparse

    #Set parameters for the run
    parser = argparse.ArgumentParser()
    # Source data
    parser.add_argument("--n-events", type=int, default=15000) #Total number of events to generate, impacts especially ground truth performance
    parser.add_argument("--time-span", type=int, default=20) #Time span (in days) over which event_time values are distributed, impacts especially window performance
    parser.add_argument("--anomaly-ratio", type=float, default=0.65) #Total fraction of anomalous events split randomly into delete/late/update
    
    # BATCH_reference
    parser.add_argument("--closed-snapshot-days", type=float, default=1) #Refresh frequency in days
    
    # A_closed_snapshot_warehouse
    parser.add_argument("--backfill-hot-days", type=float, default=4) #Hot partition window size in days
    parser.add_argument("--backfill-hot-refresh-hours", type=float, default=1) #Hot partition refresh frequency in hours
    parser.add_argument("--backfill-full-recompute-every-days", type=float, default=10) #Full-history recompute cadence in days
    
    # B_open_evolving_stream
    parser.add_argument("--open-reconcile-every-hours", type=float, default=12) #Reconciliation cadence by arrival time
    parser.add_argument("--open-propagation-lag-hours", type=float, default=1) #Visibility lag by arrival time
    
    # C_window_bounded_stream
    parser.add_argument("--window-hours", type=float, default=3) #Window size in hours, ie. micro-batch size
    parser.add_argument("--allowed-lateness-days", type=float, default=14) #Allowed lateness, watermark delay from event_time (not arrival)
    
    # E_virtual_semantic_snapshot
    parser.add_argument("--semantic-refresh-hours", type=float, default=6) #Logical snapshot refresh interval
    args = parser.parse_args()

    run_case_study(
        n_events=args.n_events,
        anomaly_ratio=args.anomaly_ratio,
        time_span=args.time_span,
        closed_snapshot_days=args.closed_snapshot_days,
        hot_partition_days=args.backfill_hot_days,
        hot_partition_refresh_hours=args.backfill_hot_refresh_hours,
        full_recompute_every_days=args.backfill_full_recompute_every_days,
        open_reconcile_every_hours=args.open_reconcile_every_hours,
        open_propagation_lag_hours=args.open_propagation_lag_hours,
        window_size_hours=args.window_hours,
        allowed_lateness_days=args.allowed_lateness_days,
        semantic_refresh_hours=args.semantic_refresh_hours,
    )
