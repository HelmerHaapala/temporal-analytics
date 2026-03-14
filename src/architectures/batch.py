"""
Benchmark architecture: traditional full-batch reference.
"""
from time import perf_counter

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._row_load_tracking import record_row_loads
from ._shared_sql import EVENT_SCHEMA_SQL, EVENT_COLUMNS_SQL


class BatchReference:
    def __init__(
        self,
        batch_window_hours: float,
    ):
        self.batch_window = pd.Timedelta(hours=batch_window_hours)
        self.conn = duckdb.connect(":memory:")
        self.table_name = "fact_sales"
        self.processing_time_seconds = 0.0
        self.rows_loaded_count = 0
        self.row_load_counts: dict[int, int] = {}
        self.freshness_cutoff_time: pd.Timestamp | None = None
        self._init_tables()

    def _init_tables(self) -> None:
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
        snapshot_df = source_conn.execute(
            f"""
            SELECT
                {EVENT_COLUMNS_SQL}
            FROM (
                SELECT {EVENT_COLUMNS_SQL}, ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY arrival_time DESC, event_id DESC) AS rn
                FROM {source_table}
                WHERE arrival_time <= ?
            )
            WHERE rn = 1 AND is_deleted = FALSE
            """,
            [batch_time],
        ).df()

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
            self.rows_loaded_count += int(len(snapshot_df))
            record_row_loads(self.row_load_counts, snapshot_df)

    def process_source(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        measure_functions: dict,
        arch_name: str,
    ) -> list[dict]:
        start_time = perf_counter()
        try:
            snapshots: list[dict] = []
            mindatetime = source_conn.execute(f"SELECT MIN(arrival_time) FROM {source_table}").fetchone()[0]
            maxdatetime = source_conn.execute(f"SELECT MAX(arrival_time) FROM {source_table}").fetchone()[0]

            current_time = pd.Timestamp(mindatetime)
            last_time = pd.Timestamp(maxdatetime)

            while current_time <= last_time:
                batch_time = min(current_time, last_time)
                self.recompute_snapshot(
                    source_conn=source_conn,
                    source_table=source_table,
                    batch_time=batch_time,
                )
                self.freshness_cutoff_time = batch_time

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
                if batch_time >= last_time:
                    break
                current_time += self.batch_window

            return snapshots
        finally:
            self.processing_time_seconds += perf_counter() - start_time


