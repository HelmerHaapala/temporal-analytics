"""
Architecture C: Event-time windowed operational source with watermark finalization.
"""

from datetime import timedelta
from time import perf_counter
from typing import Optional, Tuple

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._shared_sql import EVENT_COLUMNS_SQL, EVENT_SCHEMA_SQL, observed_events_sql


class WindowBoundedStream:

    def __init__(
        self,
        window_size_hours: float,
        allowed_lateness_hours: float,
    ) -> None:
        self.window_size_hours = float(window_size_hours)
        if self.window_size_hours <= 0:
            raise ValueError("window_size_hours must be > 0")
        self.allowed_lateness_hours = float(allowed_lateness_hours)
        self.conn = duckdb.connect(":memory:")
        self.watermark: Optional[pd.Timestamp] = None
        self.max_event_time_seen: Optional[pd.Timestamp] = None
        self.event_log_table_name = "event_log"
        self.table_name = "served_view"
        self.processing_time_seconds = 0.0
        self.rows_loaded_count = 0
        self.freshness_cutoff_time: Optional[pd.Timestamp] = None
        self._init_tables()

    def _init_tables(self) -> None:
        self.conn.execute(
            f"""
            CREATE TABLE {self.event_log_table_name} (
                {EVENT_SCHEMA_SQL}
                ,window_start TIMESTAMP
                ,window_end TIMESTAMP
                ,is_final BOOLEAN DEFAULT FALSE
            )
            """
        )

        self.conn.execute(
            f"""
            CREATE VIEW {self.table_name} AS
            SELECT
                {EVENT_COLUMNS_SQL},
                window_start,
                window_end,
                is_final
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY sale_id
                        ORDER BY arrival_time DESC, event_id DESC
                    ) AS rn
                FROM {self.event_log_table_name}
            ) t
            WHERE rn = 1 AND is_deleted = FALSE
            """
        )

    def _assign_window(self, event_time: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
        ts = event_time.timestamp()
        window_seconds = self.window_size_hours * 3600.0
        window_start_ts = (ts // window_seconds) * window_seconds
        window_start = pd.Timestamp(window_start_ts, unit="s")
        window_end = window_start + timedelta(hours=self.window_size_hours)
        return window_start, window_end

    def _advance_watermark(self, batch_max_event_time: pd.Timestamp) -> None:
        if self.max_event_time_seen is None or batch_max_event_time > self.max_event_time_seen:
            self.max_event_time_seen = batch_max_event_time

        new_watermark = self.max_event_time_seen - timedelta(hours=self.allowed_lateness_hours)
        if self.watermark is None or new_watermark > self.watermark:
            self.watermark = new_watermark

    def _finalize_closed_windows(self) -> None:
        self.conn.execute(
            f"""
            UPDATE {self.event_log_table_name}
            SET is_final = TRUE
            WHERE COALESCE(is_final, FALSE) = FALSE
              AND window_end <= '{self.watermark.isoformat()}'
            """
        )

    def ingest_events(self, events_df: pd.DataFrame) -> None:
        windowed = events_df.copy()

        windowed["event_time"] = pd.to_datetime(windowed["event_time"])
        windowed["arrival_time"] = pd.to_datetime(windowed["arrival_time"])
        self.freshness_cutoff_time = pd.Timestamp(windowed["arrival_time"].max())

        windowed[["window_start", "window_end"]] = windowed["event_time"].apply(
            lambda et: pd.Series(self._assign_window(pd.Timestamp(et)))
        )

        batch_max_event_time = pd.Timestamp(windowed["event_time"].max())
        self._advance_watermark(batch_max_event_time)

        self._finalize_closed_windows()

        accepted = windowed
        if self.watermark is not None:
            accepted = windowed[windowed["window_end"] > self.watermark].copy()
        if accepted.empty:
            return

        accepted["is_final"] = False
        temp_view = "ingestion_batch"
        self.conn.register(temp_view, accepted)
        try:
            self.conn.execute(
                f"""
                INSERT INTO {self.event_log_table_name} (
                    {EVENT_COLUMNS_SQL},
                    window_start,
                    window_end,
                    is_final
                )
                SELECT
                    {EVENT_COLUMNS_SQL},
                    window_start,
                    window_end,
                    is_final
                FROM {temp_view}
                """
            )
            self.rows_loaded_count += int(len(accepted))
        finally:
            self.conn.unregister(temp_view)

        self._finalize_closed_windows()

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
                self.ingest_events(batch.reset_index(drop=True))
                cumulative_count += len(batch)
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
