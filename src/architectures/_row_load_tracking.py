"""Helpers for tracking how often individual source rows are loaded."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd


def record_row_loads(
    row_load_counts: dict[int, int],
    rows: pd.DataFrame | Mapping[str, object] | None,
) -> None:
    """Increment per-event load counters from loaded source rows."""
    if rows is None:
        return

    if isinstance(rows, pd.DataFrame):
        if rows.empty or "event_id" not in rows.columns:
            return
        event_ids = pd.to_numeric(rows["event_id"], errors="coerce").dropna().astype(int)
        if event_ids.empty:
            return
        value_counts = event_ids.value_counts()
        for event_id, count in value_counts.items():
            event_id_int = int(event_id)
            row_load_counts[event_id_int] = row_load_counts.get(event_id_int, 0) + int(count)
        return

    event_id = rows.get("event_id")
    if event_id is None:
        return
    try:
        event_id_int = int(event_id)
    except (TypeError, ValueError):
        return
    row_load_counts[event_id_int] = row_load_counts.get(event_id_int, 0) + 1
