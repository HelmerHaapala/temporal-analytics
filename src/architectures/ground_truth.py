"""
Ground truth architecture for time-aligned snapshots.
"""
from time import perf_counter

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._row_load_tracking import record_row_loads
from ._shared_sql import EVENT_COLUMNS, EVENT_COLUMNS_SQL, EVENT_SCHEMA_SQL


class GroundTruthArchitecture:

    def __init__(self) -> None:
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

    def _apply_event(self, event: dict) -> None:
        placeholders = ", ".join(["?"] * len(EVENT_COLUMNS))

        self.conn.execute(
            f"""
            DELETE FROM {self.table_name}
            WHERE sale_id = ?
            """,
            [event["sale_id"]],
        )

        self.conn.execute(
            f"""
                INSERT INTO {self.table_name}
                (
                    {EVENT_COLUMNS_SQL}
                )
            VALUES ({placeholders})
            """,
                [event[column] for column in EVENT_COLUMNS],
        )
        self.rows_loaded_count += 1
        record_row_loads(self.row_load_counts, event)

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
            ordered_events = source_conn.execute(
                f"""
                SELECT
                    {EVENT_COLUMNS_SQL}
                FROM {source_table}
                ORDER BY arrival_time
                """
            ).df()
            if ordered_events.empty:
                return []

            cumulative_count = 0
            for _, row in ordered_events.iterrows():
                event_dict = row[list(EVENT_COLUMNS)].to_dict()
                cumulative_count += 1
                self._apply_event(event_dict)
                self.freshness_cutoff_time = pd.Timestamp(row["arrival_time"])

                snapshots.extend(
                    capture_measure_snapshots(
                        architectures={arch_name: self},
                        measure_functions=measure_functions,
                        event_count=cumulative_count,
                        arrival_time=row["arrival_time"],
                    )
                )

            return snapshots
        finally:
            self.processing_time_seconds += perf_counter() - start_time
