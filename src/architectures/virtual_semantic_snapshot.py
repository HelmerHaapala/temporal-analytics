"""
Architecture E: Virtual Semantic Snapshot.
"""

from time import perf_counter

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._shared_sql import EVENT_COLUMNS_SQL, EVENT_SCHEMA_SQL, observed_events_sql


class VirtualSemanticSnapshot:

    def __init__(
        self,
        semantic_refresh_hours: float = 6,
    ):
        self.semantic_refresh_hours = max(0.001, float(semantic_refresh_hours))
        self.conn = duckdb.connect(":memory:")
        self.table_name = "semantic_snapshot"
        self.processing_time_seconds = 0.0
        self.rows_loaded_count = 0
        self.freshness_cutoff_time: pd.Timestamp | None = None
        self._init_tables()

    def _init_tables(self) -> None:
        self.conn.execute(
            f"""
            CREATE TABLE raw_events (
            {EVENT_SCHEMA_SQL}
            )
            """
        )

        self.conn.execute(
            """
            CREATE TABLE semantic_control (
                snapshot_cutoff TIMESTAMP
            )
            """
        )

        self.conn.execute(
            """
            INSERT INTO semantic_control VALUES (TIMESTAMP '1900-01-01')
            """
        )

        cutoff_expr = "(SELECT snapshot_cutoff FROM semantic_control LIMIT 1)"

        self.conn.execute(
            f"""
            CREATE VIEW {self.table_name} AS
            SELECT
                {EVENT_COLUMNS_SQL}
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY sale_id
                        ORDER BY arrival_time DESC, event_id DESC
                    ) AS rn
                FROM raw_events
                WHERE arrival_time <= {cutoff_expr}
            ) ranked
            WHERE rn = 1 AND is_deleted = FALSE
            """
        )

    def _set_snapshot_cutoff(self, cutoff_time: pd.Timestamp) -> None:
        self.conn.execute("DELETE FROM semantic_control")
        self.conn.execute("INSERT INTO semantic_control VALUES (?)", [cutoff_time])
        self.freshness_cutoff_time = pd.Timestamp(cutoff_time)

    def process_source(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        measure_functions: dict,
        arch_name: str,
    ) -> list[dict]:
        start_time = perf_counter()

        try:
            observed_events = source_conn.execute(observed_events_sql(source_table)).df()
            if observed_events.empty:
                return []

            self.conn.register("observed_events_df", observed_events)
            try:
                self.conn.execute(
                    f"""
                    INSERT INTO raw_events
                    SELECT
                        {EVENT_COLUMNS_SQL}
                    FROM observed_events_df
                    """
                )
                self.rows_loaded_count += int(len(observed_events))
            finally:
                self.conn.unregister("observed_events_df")

            min_arrival = pd.Timestamp(observed_events["arrival_time"].min())
            max_arrival = pd.Timestamp(observed_events["arrival_time"].max())
            refresh_interval = pd.Timedelta(hours=self.semantic_refresh_hours)

            snapshots: list[dict] = []
            current_start = min_arrival
            while current_start <= max_arrival:
                interval_end = current_start + refresh_interval - pd.Timedelta(microseconds=1)
                if interval_end > max_arrival:
                    break
                self._set_snapshot_cutoff(interval_end)
                
                event_count = self.conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM raw_events
                    WHERE arrival_time <= ?
                    """,
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

            if not snapshots:
                snapshots.extend(
                    capture_measure_snapshots(
                        architectures={arch_name: self},
                        measure_functions=measure_functions,
                        event_count=len(observed_events),
                        arrival_time=max_arrival,
                    )
                )

            return snapshots
        finally:
            self.processing_time_seconds += perf_counter() - start_time
