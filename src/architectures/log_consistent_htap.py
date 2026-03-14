"""
Architecture D: Log-consistent HTAP.
"""

from time import perf_counter

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._row_load_tracking import record_row_loads
from ._shared_sql import EVENT_COLUMNS_SQL, EVENT_SCHEMA_SQL, observed_events_sql


class LogConsistentHTAP:
    """Architecture D: commit-snapshot reads on top of an observed change log."""

    def __init__(
        self,
        commit_every_hours: float = 2,
    ):
        self.commit_every = pd.Timedelta(hours=max(0.0, float(commit_every_hours)))
        self.conn = duckdb.connect(":memory:")
        self.table_name = "committed_snapshot"
        self.processing_time_seconds = 0.0
        self.rows_loaded_count = 0
        self.row_load_counts: dict[int, int] = {}
        self.freshness_cutoff_time: pd.Timestamp | None = None
        self._init_tables()

    def _init_tables(self) -> None:
        self.conn.execute(
            f"""
            CREATE TABLE observed_change_log (
            {EVENT_SCHEMA_SQL}
            )
            """
        )

        self.conn.execute(
            """
            CREATE TABLE commit_control (
                commit_cutoff TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO commit_control VALUES (TIMESTAMP '1900-01-01')
            """
        )

        cutoff_expr = "(SELECT commit_cutoff FROM commit_control LIMIT 1)"

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
                FROM observed_change_log
                WHERE arrival_time <= {cutoff_expr}
            ) committed
            WHERE rn = 1 AND is_deleted = FALSE
            """
        )

    def _set_commit_cutoff(self, cutoff_time: pd.Timestamp) -> None:
        self.conn.execute("DELETE FROM commit_control")
        self.conn.execute("INSERT INTO commit_control VALUES (?)", [cutoff_time])
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
                    INSERT INTO observed_change_log
                    SELECT
                        {EVENT_COLUMNS_SQL}
                    FROM observed_events_df
                    """
                )
                self.rows_loaded_count += int(len(observed_events))
                record_row_loads(self.row_load_counts, observed_events)
            finally:
                self.conn.unregister("observed_events_df")

            snapshots: list[dict] = []
            cumulative_count = 0

            first_arrival = pd.Timestamp(observed_events["arrival_time"].min())
            next_commit_time = first_arrival

            for arrival_time, batch in observed_events.groupby("arrival_time", sort=False):
                arrival_ts = pd.Timestamp(arrival_time)
                cumulative_count += len(batch)

                # Commit at the current arrival boundary when the cadence is due.
                if self.commit_every <= pd.Timedelta(0):
                    self._set_commit_cutoff(arrival_ts)
                elif arrival_ts >= next_commit_time:
                    self._set_commit_cutoff(arrival_ts)
                    next_commit_time = arrival_ts + self.commit_every

                snapshots.extend(
                    capture_measure_snapshots(
                        architectures={arch_name: self},
                        measure_functions=measure_functions,
                        event_count=cumulative_count,
                        arrival_time=arrival_ts,
                    )
                )

            return snapshots
        finally:
            self.processing_time_seconds += perf_counter() - start_time
