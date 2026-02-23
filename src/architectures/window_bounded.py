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
        window_size_hours,
        allowed_lateness_days,
    ):
        self.window_size_hours = float(window_size_hours)
        if self.window_size_hours <= 0:
            raise ValueError("window_size_hours must be > 0")
        self.allowed_lateness_days = float(allowed_lateness_days)
        self.conn = duckdb.connect(":memory:")
        self.ingestion_count = 0
        # Watermark state
        self.watermark: Optional[pd.Timestamp] = None
        self.max_event_time_seen: Optional[pd.Timestamp] = None
        # Tables / views
        self.event_log_table_name = "event_log" #append-only log of all ingested events, with window and finality metadata
        self.table_name = "served_view" #latest ingested record per sale_id, including open windows, with is_final flag
        self.processing_time_seconds = 0.0
        self._init_tables() #call for table initialization

    def _init_tables(self) -> None:
        #Main event log: all ingested events (open windows + finalized rows)
        #with an is_final flag that becomes TRUE when watermark closes their window
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

        # Published view: latest ingested record per sale_id, including open windows
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
        #Assign a fixed-size tumbling window based on event_time
        ts = event_time.timestamp()
        window_seconds = self.window_size_hours * 3600.0
        window_start_ts = (ts // window_seconds) * window_seconds
        window_start = pd.Timestamp(window_start_ts, unit="s")
        window_end = window_start + timedelta(hours=self.window_size_hours)
        return window_start, window_end

    def _advance_watermark(self, batch_max_event_time: pd.Timestamp) -> None:
        #Update running max event_time and watermark (non-decreasing)
        if self.max_event_time_seen is None or batch_max_event_time > self.max_event_time_seen:
            self.max_event_time_seen = batch_max_event_time

        new_watermark = self.max_event_time_seen - timedelta(days=self.allowed_lateness_days)
        if self.watermark is None or new_watermark > self.watermark:
            self.watermark = new_watermark

    def _finalize_closed_windows(self) -> None:
        #Mark rows in windows closed by the current watermark as final
        self.conn.execute(
            f"""
            UPDATE {self.event_log_table_name}
            SET is_final = TRUE
            WHERE COALESCE(is_final, FALSE) = FALSE
              AND window_end <= '{self.watermark.isoformat()}'
            """
        )

    def ingest_events(self, events_df: pd.DataFrame) -> None:
        #Ingest a micro-batch (=events with the same arrival_time)
        self.ingestion_count += 1
        windowed = events_df.copy()

        #Ensure pandas Timestamps
        windowed["event_time"] = pd.to_datetime(windowed["event_time"])
        windowed["arrival_time"] = pd.to_datetime(windowed["arrival_time"])

        #Assign event-time windows
        windowed[["window_start", "window_end"]] = windowed["event_time"].apply(
            lambda et: pd.Series(self._assign_window(pd.Timestamp(et)))
        )

        #Advance watermark using running max event time (not batch max only)
        batch_max_event_time = pd.Timestamp(windowed["event_time"].max())
        self._advance_watermark(batch_max_event_time)

        #Finalize any previously ingested windows that are now closed
        self._finalize_closed_windows()

        #Drop events that belong to already closed windows
        accepted = windowed
        if self.watermark is not None:
            accepted = windowed[windowed["window_end"] > self.watermark].copy()
        if accepted.empty:
            return

        #Insert accepted events into the main log.
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
        finally:
            self.conn.unregister(temp_view)

        #After inserting accepted events, we may be able to finalize them immediately
        self._finalize_closed_windows()

    def process_source(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        measure_functions: dict,
        arch_name: str,
    ) -> list[dict]:
        start_time = perf_counter()
       
        #Main loop
        try:
            #Read source as virtualized operational observations
            #one observed row per (sale_id, arrival_time), then process in pull order
            ordered_events = source_conn.execute(observed_events_sql(source_table)).df()
            if ordered_events.empty: return []

            snapshots: list[dict] = []
            cumulative_count = 0
            #Process all observations visible at each arrival boundary
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
