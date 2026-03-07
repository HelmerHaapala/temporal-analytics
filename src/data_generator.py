"""Synthetic source-data generator for the simulation."""

from dataclasses import dataclass
from datetime import datetime, timedelta

import duckdb
import numpy as np
import pandas as pd


@dataclass
class TemporalEventGenerator:
    """Generate synthetic events with updates, deletes, and late arrivals."""
    n_events: int
    anomaly_ratio: float
    time_span_days: int
    seed: int = 42

    def generate_events(self) -> pd.DataFrame:
        """Create synthetic source events."""
        if not 0 <= self.anomaly_ratio <= 1:
            raise ValueError("anomaly_ratio must be between 0 and 1")
        rng = np.random.default_rng(self.seed)

        ratios = rng.dirichlet([1.0, 1.0, 1.0]) * self.anomaly_ratio
        delete_ratio, late_arrival_ratio, update_ratio = ratios

        base_date = datetime(2025, 1, 1)
        event_id_counter = 1
        sale_id_counter = 1

        def _sample_anomaly_arrival(prior_arrival: datetime) -> datetime:
            for _ in range(10):
                candidate = base_date + timedelta(
                    days=float(rng.uniform(0, self.time_span_days))
                )
                if candidate >= prior_arrival:
                    return candidate
            return prior_arrival + timedelta(microseconds=1)

        def _sample_amount(mean: float, sigma: float, min_value: float, max_value: float) -> float:
            value = float(rng.lognormal(mean=mean, sigma=sigma))
            return float(np.clip(value, min_value, max_value))

        product_names = [f"Product_{i}" for i in range(1, 11)]
        product_categories = rng.choice(
            ["Electronics", "Clothing", "Books", "Food"], 10
        )
        region_names = [f"Region_{i}" for i in range(1, 11)]
        region_countries = rng.choice(["USA", "EU", "APAC"], 10)

        base_events: list[dict] = []
        n_updates = int(self.n_events * update_ratio)
        n_deletes = int(self.n_events * delete_ratio)
        n_base_events = self.n_events - n_updates - n_deletes

        for _ in range(n_base_events):
            days_offset = float(rng.uniform(0, self.time_span_days))
            event_time = base_date + timedelta(days=days_offset)

            if float(rng.random()) < float(late_arrival_ratio):
                arrival_days_offset = days_offset + float(rng.uniform(1, 10))
            else:
                arrival_days_offset = days_offset + float(rng.uniform(0, 1))

            arrival_time = base_date + timedelta(days=arrival_days_offset)
            product_index = int(rng.integers(0, 10))
            region_index = int(rng.integers(0, 10))
            quantity = int(rng.integers(1, 25))
            amount = _sample_amount(mean=2.0, sigma=0.7, min_value=0.5, max_value=500.0)

            base_events.append({
                "event_id": event_id_counter,
                "sale_id": sale_id_counter,
                "event_time": event_time,
                "arrival_time": arrival_time,
                "product_name": product_names[product_index],
                "category": product_categories[product_index],
                "region_name": region_names[region_index],
                "country": region_countries[region_index],
                "quantity": quantity,
                "amount": amount,
                "is_update": False,
                "is_deleted": False,
            })
            event_id_counter += 1
            sale_id_counter += 1

        for _ in range(n_updates):
            if not base_events:
                break
            prior_event = base_events[int(rng.integers(0, len(base_events)))]
            update_arrival_time = _sample_anomaly_arrival(prior_event["arrival_time"])
            product_index = int(rng.integers(0, 10))
            region_index = int(rng.integers(0, 10))
            base_events.append({
                "event_id": event_id_counter,
                "sale_id": prior_event["sale_id"],
                "event_time": prior_event["event_time"],
                "arrival_time": update_arrival_time,
                "product_name": product_names[product_index],
                "category": product_categories[product_index],
                "region_name": region_names[region_index],
                "country": region_countries[region_index],
                "quantity": int(rng.integers(1, 50)),
                "amount": _sample_amount(mean=4.5, sigma=1.0, min_value=1.0, max_value=5000.0),
                "is_update": True,
                "is_deleted": False,
            })
            event_id_counter += 1

        for _ in range(n_deletes):
            if not base_events:
                break
            prior_event = base_events[int(rng.integers(0, len(base_events)))]
            delete_arrival_time = _sample_anomaly_arrival(prior_event["arrival_time"])

            base_events.append({
                "event_id": event_id_counter,
                "sale_id": prior_event["sale_id"],
                "event_time": prior_event["event_time"],
                "arrival_time": delete_arrival_time,
                "product_name": prior_event["product_name"],
                "category": prior_event["category"],
                "region_name": prior_event["region_name"],
                "country": prior_event["country"],
                "quantity": prior_event["quantity"],
                "amount": prior_event["amount"],
                "is_update": False,
                "is_deleted": True,
            })
            event_id_counter += 1

        rng.shuffle(base_events)

        events_df = pd.DataFrame(base_events)
        events_df = events_df.sort_values(["arrival_time", "event_id"]).reset_index(drop=True)
        arrival_times = events_df["arrival_time"].to_numpy().copy()
        one_microsecond = np.timedelta64(1, "us")
        for idx in range(1, len(arrival_times)):
            if arrival_times[idx] <= arrival_times[idx - 1]:
                arrival_times[idx] = arrival_times[idx - 1] + one_microsecond
        events_df["arrival_time"] = arrival_times

        return events_df

    def create_source_table(
        self,
        conn: duckdb.DuckDBPyConnection | None = None,
        table_name: str = "events_source",
    ) -> tuple[duckdb.DuckDBPyConnection, dict]:

        events_df = self.generate_events()
        if conn is None:
            conn = duckdb.connect(":memory:")
        conn.register("events_df", events_df)
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM events_df")
        print(f"[OK] Database table '{table_name}' created with {len(events_df)} events inserted")

        stats = conn.execute(
            f"SELECT COUNT(*) AS total_events, MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time FROM {table_name}"
        ).df().iloc[0].to_dict()
        return conn, stats
