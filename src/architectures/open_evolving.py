"""
Architecture B: Open Evolving Stream.
"""
from time import perf_counter
from typing import Optional

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._shared_sql import EVENT_COLUMNS_SQL, EVENT_SCHEMA_SQL, observed_events_sql


class OpenEvolvingStream:
    """Architecture B: lagged periodic reconciliation over observed source changes."""

    def __init__(
        self,
        reconcile_every_hours: float = 6,
        propagation_lag_hours: float = 1,
    ):
        self.reconcile_every = pd.Timedelta(hours=max(0.001, float(reconcile_every_hours)))
        self.propagation_lag = pd.Timedelta(hours=max(0.0, float(propagation_lag_hours)))
        self.last_reconciled_visible_cutoff: Optional[pd.Timestamp] = None
        self.next_reconcile_time: Optional[pd.Timestamp] = None
        self.conn = duckdb.connect(":memory:")
        self.log_table_name = "event_log"
        self.table_name = "served_state"
        self.processing_time_seconds = 0.0
        self.rows_loaded_count = 0
        self.freshness_cutoff_time: Optional[pd.Timestamp] = None
        self._init_tables()

    def _init_tables(self) -> None:
        self.conn.execute(
            f"""
            CREATE TABLE {self.log_table_name} (
            {EVENT_SCHEMA_SQL}
            )
            """
        )
        self.conn.execute(
            f"""
            CREATE TABLE {self.table_name} (
            {EVENT_SCHEMA_SQL}
            )
            """
        )

    def _reconcile_served_state(self, visible_cutoff: pd.Timestamp) -> None:
        cutoff = pd.Timestamp(visible_cutoff)
        if self.last_reconciled_visible_cutoff is not None and cutoff <= self.last_reconciled_visible_cutoff:
            return

        snapshot_df = self.conn.execute(
            f"""
            SELECT
                {EVENT_COLUMNS_SQL}
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY sale_id
                        ORDER BY arrival_time DESC, event_id DESC
                    ) AS rn
                FROM {self.log_table_name}
                WHERE arrival_time <= ?
            ) ranked
            WHERE rn = 1 AND is_deleted = FALSE
            """,
            [cutoff],
        ).df()

        self.conn.execute(f"DELETE FROM {self.table_name}")
        if not snapshot_df.empty:
            self.conn.register("served_snapshot_df", snapshot_df)
            try:
                self.conn.execute(
                    f"""
                    INSERT INTO {self.table_name}
                    SELECT
                        {EVENT_COLUMNS_SQL}
                    FROM served_snapshot_df
                    """
                )
                self.rows_loaded_count += int(len(snapshot_df))
            finally:
                self.conn.unregister("served_snapshot_df")

        self.last_reconciled_visible_cutoff = cutoff
        self.freshness_cutoff_time = cutoff

    def _maybe_reconcile(self, batch_arrival_time: pd.Timestamp) -> None:
        current_time = pd.Timestamp(batch_arrival_time)
        visible_cutoff = current_time - self.propagation_lag

        if self.last_reconciled_visible_cutoff is not None and visible_cutoff <= self.last_reconciled_visible_cutoff:
            return
        if self.next_reconcile_time is None:
            self.next_reconcile_time = current_time + self.reconcile_every
            return
        if current_time < self.next_reconcile_time:
            return

        self._reconcile_served_state(visible_cutoff)

        while self.next_reconcile_time <= current_time:
            self.next_reconcile_time += self.reconcile_every

    def ingest_events(self, events_df: pd.DataFrame) -> int:
        if events_df is None or events_df.empty:
            return 0

        n_rows = len(events_df)
        staged_events = events_df.copy().reset_index(drop=True)

        temp_view = "ingestion_batch"
        self.conn.register(temp_view, staged_events)
        try:
            self.conn.execute(
                f"""
                INSERT INTO {self.log_table_name}
                SELECT
                    *
                FROM {temp_view}
                """
            )
            self.rows_loaded_count += int(n_rows)
        finally:
            self.conn.unregister(temp_view)
        batch_arrival_time = pd.Timestamp(staged_events["arrival_time"].max())
        self._maybe_reconcile(batch_arrival_time)
        return n_rows

    def process_source(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        measure_functions: dict,
        arch_name: str,
    ) -> list[dict]:
        start_time = perf_counter()
        try:
            ordered_events = source_conn.execute(observed_events_sql(source_table)).df()
            if ordered_events.empty:
                return []

            snapshots: list[dict] = []
            cumulative_count = 0

            for arrival_time, batch in ordered_events.groupby("arrival_time", sort=False):
                cumulative_count += self.ingest_events(batch.reset_index(drop=True))
                snapshots.extend(
                    capture_measure_snapshots(
                        architectures={arch_name: self},
                        measure_functions=measure_functions,
                        event_count=cumulative_count,
                        arrival_time=arrival_time,
                    )
                )

            return snapshots

        finally:
            self.processing_time_seconds += perf_counter() - start_time
