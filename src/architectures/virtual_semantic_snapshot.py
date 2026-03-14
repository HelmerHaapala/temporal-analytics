"""
Architecture E: Virtual Semantic Snapshot.
"""

from time import perf_counter

import duckdb
import pandas as pd

from measures import capture_measure_snapshots
from ._row_load_tracking import record_row_loads
from ._shared_sql import EVENT_COLUMNS_SQL, EVENT_SCHEMA_SQL


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

    def _refresh_semantic_snapshot(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        snapshot_cutoff: pd.Timestamp,
    ) -> int:
        snapshot_df = source_conn.execute(
            f"""
            SELECT
                {EVENT_COLUMNS_SQL}
            FROM (
                SELECT
                    {EVENT_COLUMNS_SQL},
                    ROW_NUMBER() OVER (
                        PARTITION BY sale_id
                        ORDER BY arrival_time DESC, event_id DESC
                    ) AS rn
                FROM {source_table}
                WHERE arrival_time <= ?
            ) ranked
            WHERE rn = 1 AND is_deleted = FALSE
            """,
            [snapshot_cutoff],
        ).df()

        self.conn.execute(f"DELETE FROM {self.table_name}")
        if snapshot_df.empty:
            self.freshness_cutoff_time = pd.Timestamp(snapshot_cutoff)
            return 0

        self.conn.register("semantic_snapshot_df", snapshot_df)
        try:
            self.conn.execute(
                f"""
                INSERT INTO {self.table_name}
                SELECT
                    {EVENT_COLUMNS_SQL}
                FROM semantic_snapshot_df
                """
            )
        finally:
            self.conn.unregister("semantic_snapshot_df")

        visible_row_count = int(len(snapshot_df))
        self.rows_loaded_count += visible_row_count
        record_row_loads(self.row_load_counts, snapshot_df)
        self.freshness_cutoff_time = pd.Timestamp(snapshot_cutoff)
        return visible_row_count

    def process_source(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        source_table: str,
        measure_functions: dict,
        arch_name: str,
    ) -> list[dict]:
        start_time = perf_counter()

        try:
            min_arrival = source_conn.execute(
                f"SELECT MIN(arrival_time) FROM {source_table}"
            ).fetchone()[0]
            max_arrival = source_conn.execute(
                f"SELECT MAX(arrival_time) FROM {source_table}"
            ).fetchone()[0]
            if min_arrival is None or max_arrival is None:
                return []

            min_arrival = pd.Timestamp(min_arrival)
            max_arrival = pd.Timestamp(max_arrival)
            refresh_interval = pd.Timedelta(hours=self.semantic_refresh_hours)

            snapshots: list[dict] = []
            snapshot_cutoff = min_arrival
            while snapshot_cutoff <= max_arrival:
                self._refresh_semantic_snapshot(
                    source_conn=source_conn,
                    source_table=source_table,
                    snapshot_cutoff=snapshot_cutoff,
                )
                event_count = source_conn.execute(
                    f"SELECT COUNT(*) FROM {source_table} WHERE arrival_time <= ?",
                    [snapshot_cutoff],
                ).fetchone()[0]

                snapshots.extend(
                    capture_measure_snapshots(
                        architectures={arch_name: self},
                        measure_functions=measure_functions,
                        event_count=event_count,
                        arrival_time=snapshot_cutoff,
                    )
                )
                snapshot_cutoff += refresh_interval

            return snapshots
        finally:
            self.processing_time_seconds += perf_counter() - start_time
