"""Shared SQL snippets used by architecture implementations."""

EVENT_SCHEMA_SQL = """
                event_id BIGINT,
                sale_id BIGINT,
                event_time TIMESTAMP,
                arrival_time TIMESTAMP,
                product_name VARCHAR,
                category VARCHAR,
                region_name VARCHAR,
                country VARCHAR,
                quantity INTEGER,
                amount DOUBLE,
                is_update BOOLEAN,
                is_deleted BOOLEAN
"""

EVENT_COLUMNS = (
    "event_id",
    "sale_id",
    "event_time",
    "arrival_time",
    "product_name",
    "category",
    "region_name",
    "country",
    "quantity",
    "amount",
    "is_update",
    "is_deleted",
)

EVENT_COLUMNS_SQL = ",\n".join(EVENT_COLUMNS)

def observed_events_sql(source_table: str) -> str:
    return f"""
                SELECT
                    {EVENT_COLUMNS_SQL}
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY sale_id, arrival_time
                            ORDER BY event_id DESC
                        ) AS rn
                    FROM {source_table}
                ) observed
                WHERE rn = 1
                ORDER BY arrival_time, sale_id, event_id
                """
