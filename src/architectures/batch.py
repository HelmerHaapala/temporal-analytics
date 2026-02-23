"""
Architecture A: Closed Snapshot Warehouse.
"""
from time import perf_counter

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._shared_sql import EVENT_SCHEMA_SQL, EVENT_COLUMNS_SQL


class BatchReference:
    def __init__(self, batch_window_days):
        self.batch_window = pd.Timedelta(days=batch_window_days)
        self.conn = duckdb.connect(":memory:")
        self.table_name = "fact_sales"
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

    def recompute_snapshot(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        batch_time: pd.Timestamp,
    ) -> None:
        #Process the entire snapshot for all events with arrival_time <= batch_time
        snapshot_df = source_conn.execute(
            f"""
            SELECT
                {EVENT_COLUMNS_SQL}
            FROM ( --deduplicate updates/deletes
                SELECT {EVENT_COLUMNS_SQL}, ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY arrival_time DESC, event_id DESC) AS rn
                FROM {source_table}
                WHERE arrival_time <= ?
            )
            WHERE rn = 1 AND is_deleted = FALSE
            """,
            [batch_time],
        ).df()

        #Delete existing snapshot and replace with new one
        self.conn.execute(f"DELETE FROM {self.table_name}")
        if not snapshot_df.empty:
            self.conn.register("snapshot_df", snapshot_df)
            self.conn.execute(
                f"""
                INSERT INTO {self.table_name}
                SELECT
                    {EVENT_COLUMNS_SQL}
                FROM snapshot_df
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
            #Determine when to calculate measure value snapshots
            snapshots: list[dict] = []
            #Get the event boundaries from source
            mindatetime = source_conn.execute(f"SELECT MIN(arrival_time) FROM {source_table}").fetchone()[0]
            maxdatetime = source_conn.execute(f"SELECT MAX(arrival_time) FROM {source_table}").fetchone()[0]
            
            #Use precise boundaries for daily batches
            current_time = pd.Timestamp(mindatetime)
            last_time = pd.Timestamp(maxdatetime)

            # Main loop for batch processing.
            while current_time <= last_time:
                batch_time = min(current_time + self.batch_window, last_time)
                #Call for a single ETL cycle
                self.recompute_snapshot(
                    source_conn=source_conn,
                    source_table=source_table,
                    batch_time=batch_time,
                )

                #Record measure snapshot after processing
                event_count = source_conn.execute(
                    f"SELECT COUNT(*) FROM {source_table} WHERE arrival_time <= ?",
                    [batch_time],
                ).fetchone()[0]
                snapshots.extend(
                    capture_measure_snapshots(
                        architectures={arch_name: self},
                        measure_functions=measure_functions,
                        event_count=event_count,
                        arrival_time=batch_time,
                    )
                )
                # Stop after emitting the first snapshot at last_time to avoid duplicates.
                if batch_time >= last_time:
                    break
                current_time += self.batch_window

            return snapshots
        finally:
            self.processing_time_seconds += perf_counter() - start_time


# Backward-compatible alias while callers migrate.
ClosedSnapshotWarehouse = BatchReference

