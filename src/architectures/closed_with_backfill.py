"""
Architecture A: Closed Snapshot + Backfill.
"""

from time import perf_counter

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._row_load_tracking import record_row_loads
from ._shared_sql import EVENT_COLUMNS, EVENT_COLUMNS_SQL, EVENT_SCHEMA_SQL


class ClosedSnapshotWithBackfill:

    def __init__(
        self,
        hot_partition_hours: float = 720.0,
        hot_partition_refresh_hours: float = 1.0,
        full_recompute_every_hours: float = 336.0,
    ) -> None:
        self.conn = duckdb.connect(":memory:")
        self.table_name = "fact_sales"
        self.hot_partition_hours = hot_partition_hours
        self.hot_partition_refresh_hours = hot_partition_refresh_hours
        self.full_recompute_every_hours = full_recompute_every_hours
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

    def _recompute_partition(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        boundary_start: pd.Timestamp,
        interval_end: pd.Timestamp,
    ) -> None:
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

        self.conn.register("hot_latest_events", hot_latest_df)
        self.conn.execute(
            f"""
            DELETE FROM {self.table_name}
            WHERE sale_id IN (SELECT sale_id FROM hot_latest_events)
            """
        )
        inserted_events = hot_latest_df[hot_latest_df["is_deleted"] == False].copy()
        inserted_rows = len(inserted_events)
        hot_event_columns_sql = ",\n".join(f"hot.{column}" for column in EVENT_COLUMNS)
        self.conn.execute(
            f"""
            INSERT INTO {self.table_name}
            SELECT
                {hot_event_columns_sql}
            FROM hot_latest_events hot
            WHERE hot.is_deleted = FALSE
            """
        )
        self.rows_loaded_count += int(inserted_rows)
        record_row_loads(self.row_load_counts, inserted_events)

    def process_source(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        measure_functions: dict,
        arch_name: str,
    ) -> list[dict]:
        start_time = perf_counter()
        try:
            mindatetime = source_conn.execute(f"SELECT MIN(arrival_time) FROM {source_table}").fetchone()[0]
            maxdatetime = source_conn.execute(f"SELECT MAX(arrival_time) FROM {source_table}").fetchone()[0]

            source_start = pd.Timestamp(mindatetime)
            current_start = source_start
            last_end = pd.Timestamp(maxdatetime)

            refresh_interval = pd.Timedelta(hours=self.hot_partition_refresh_hours)
            hot_window = pd.Timedelta(hours=self.hot_partition_hours)
            full_recompute_interval = pd.Timedelta(hours=self.full_recompute_every_hours)
            next_full_recompute_at = source_start + full_recompute_interval

            snapshots: list[dict] = []

            while current_start <= last_end:
                interval_end = current_start

                hot_boundary_start = interval_end - hot_window
                boundary_start = hot_boundary_start if hot_boundary_start > source_start else source_start

                if interval_end >= next_full_recompute_at:
                    boundary_start = source_start
                    while interval_end >= next_full_recompute_at:
                        next_full_recompute_at += full_recompute_interval

                self._recompute_partition(
                    source_conn=source_conn,
                    source_table=source_table,
                    boundary_start=boundary_start,
                    interval_end=interval_end,
                )
                self.freshness_cutoff_time = interval_end

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
