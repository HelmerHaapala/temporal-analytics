"""
Architecture B: Closed Snapshot + Backfill.
"""
from time import perf_counter

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._shared_sql import EVENT_COLUMNS, EVENT_COLUMNS_SQL, EVENT_SCHEMA_SQL

class ClosedSnapshotWithBackfill:

    def __init__(
        self,
        hot_partition_days: float = 30,
        hot_partition_refresh_hours: float = 1,
        full_recompute_every_days: float = 14.0,
    ):
        self.conn = duckdb.connect(":memory:")
        self.table_name = "fact_sales"
        self.hot_partition_days = hot_partition_days
        self.hot_partition_refresh_hours = hot_partition_refresh_hours
        self.full_recompute_every_days = full_recompute_every_days
        self.processing_time_seconds = 0.0
        self._init_tables() #call for table initialization


    def _init_tables(self) -> None:
        #Create target table
        self.conn.execute(
            f"""
            CREATE TABLE {self.table_name} (
            {EVENT_SCHEMA_SQL}
            )
            """
        )

    def _recompute_partition(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        boundary_start: pd.Timestamp,
        interval_end: pd.Timestamp,
    ) -> None:
        #Delete the currently reconciled scope from the target.
        self.conn.execute(
            f"""
            DELETE FROM {self.table_name}
            WHERE arrival_time >= ?
            """,
            [boundary_start],
        )

        #Rebuild latest rows in the selected scope.
        hot_latest_df = source_conn.execute(
            f"""
            SELECT
                {EVENT_COLUMNS_SQL}
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY arrival_time DESC, event_id DESC) AS rn
                FROM {source_table}
                WHERE arrival_time >= ? AND arrival_time <= ?
            ) hot
            WHERE rn = 1
            """,
            [boundary_start, interval_end],
        ).df()

        #Only reconcile rows that are currently in the selected scope.
        self.conn.register("hot_latest_events", hot_latest_df)

        #Write non-deleted latest rows from the selected scope.
        #Rows before boundary_start remain immutable.
        HOT_EVENT_COLUMNS_SQL = ",\n".join(f"hot.{column}" for column in EVENT_COLUMNS)
        self.conn.execute(
            f"""
            INSERT INTO {self.table_name}
            SELECT
                {HOT_EVENT_COLUMNS_SQL}
            FROM hot_latest_events hot
            LEFT JOIN {self.table_name} cold ON cold.sale_id = hot.sale_id
            WHERE cold.sale_id IS NULL AND hot.is_deleted = FALSE
            """
        )

    def process_source(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        measure_functions: dict,
        arch_name: str,
    ) -> list[dict]:
        
        start_time = perf_counter()
        try:
            #Get the event boundaries from source
            mindatetime = source_conn.execute(f"SELECT MIN(arrival_time) FROM {source_table}").fetchone()[0]
            maxdatetime = source_conn.execute(f"SELECT MAX(arrival_time) FROM {source_table}").fetchone()[0]
            
            #When to stop iterating and when iterated last
            source_start = pd.Timestamp(mindatetime)
            current_start = source_start
            last_end = pd.Timestamp(maxdatetime)

            #How often to refresh and how large batches
            refresh_interval = pd.Timedelta(hours=self.hot_partition_refresh_hours)
            hot_window = pd.Timedelta(days=self.hot_partition_days)

            # Full recomputation cadence is always enabled.
            full_recompute_interval = pd.Timedelta(days=self.full_recompute_every_days)
            next_full_recompute_at = source_start + full_recompute_interval

            snapshots: list[dict] = []
            
            #Main loop for batch processing
            while current_start <= last_end:
                #Current hot partition upper boundary
                interval_end = current_start + refresh_interval - pd.Timedelta(microseconds=1)

                #Hot partition lower boundary is either hot_window or source_start, whichever is more recent
                hot_boundary_start = interval_end - hot_window
                boundary_start = hot_boundary_start if hot_boundary_start > source_start else source_start

                #At sparse checkpoints, reconcile from full deduped history
                #In a production environment this would be scheduled separately (and pause micro-batching until batch is done)
                if interval_end >= next_full_recompute_at:
                    boundary_start = source_start #Set start to source start instead of micro-batch start
                    while interval_end >= next_full_recompute_at:
                        next_full_recompute_at += full_recompute_interval

                #Micro batch or full recompute
                self._recompute_partition(
                    source_conn=source_conn,
                    source_table=source_table,
                    boundary_start=boundary_start,
                    interval_end=interval_end,
                )

                #Record measure snapshot after processing
                event_count = source_conn.execute(
                    f"SELECT COUNT(*) FROM {source_table} WHERE arrival_time <= ?",
                    [interval_end],
                ).fetchone()[0]
                snapshots.extend(
                    capture_measure_snapshots(
                        architectures={arch_name: self},
                        measure_functions=measure_functions,
                        event_count=event_count,
                        arrival_time=interval_end,
                    )
                )

                current_start += refresh_interval

            return snapshots
        finally:
            self.processing_time_seconds += perf_counter() - start_time
