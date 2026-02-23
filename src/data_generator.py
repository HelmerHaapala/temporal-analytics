"""
Synthetic operational change-history generator for temporal analytics case study.

Generates a controlled history of row-level changes used to emulate
an evolving operational database observed over time.
- The source table is a virtualization artifact for reproducible experiments.
- It is not intended to represent a production CDC transport stream.

Disturbances included in the generated history:
- Late arrivals (event-time < arrival-time)
- Out-of-order arrivals
- Corrective updates to prior events
- Soft-deletes (tombstones) emitted as `is_deleted=True`
- DuckDB source table creation
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List

import duckdb
import numpy as np
import pandas as pd


@dataclass
class TemporalEventGenerator:
    """Generate synthetic events with temporal anomalies"""
    n_events: int
    anomaly_ratio: float
    time_span_days: int

    def generate_events(self) -> pd.DataFrame:
        """Sales Events"""
        if not 0 <= self.anomaly_ratio <= 1:
            raise ValueError("anomaly_ratio must be between 0 and 1")

        # Split the anomaly ratio randomly across delete, late-arrival, and update.
        ratios = np.random.default_rng().dirichlet([1.0, 1.0, 1.0]) * self.anomaly_ratio
        delete_ratio, late_arrival_ratio, update_ratio = ratios

        base_date = datetime(2025, 1, 1)
        max_event_date = base_date + timedelta(days=self.time_span_days)
        event_id_counter = 1
        sale_id_counter = 1

        #Arrival times for anomalies to prevent clustering at the end of daterange
        def _sample_anomaly_arrival(prior_arrival: datetime) -> datetime:
            for _ in range(10):
                candidate = base_date + timedelta(
                    days=np.random.uniform(0, self.time_span_days)
                )
                if candidate >= prior_arrival:
                    return candidate
            return prior_arrival + timedelta(microseconds=1)

        #Introduce variance to event values
        def _sample_amount(mean: float, sigma: float, min_value: float, max_value: float) -> float:
            value = float(np.random.lognormal(mean=mean, sigma=sigma))
            return float(np.clip(value, min_value, max_value))

        #Define dimensions
        product_names = [f"Product_{i}" for i in range(1, 11)]
        product_categories = np.random.choice(
            ["Electronics", "Clothing", "Books", "Food"], 10
        )
        region_names = [f"Region_{i}" for i in range(1, 11)]
        region_countries = np.random.choice(["USA", "EU", "APAC"], 10)

        #Determine the number of base events, updates, and deletes
        base_events: List[Dict] = []
        n_updates = int(self.n_events * update_ratio)
        n_deletes = int(self.n_events * delete_ratio)
        n_base_events = self.n_events - n_updates - n_deletes

        #Base events
        for _ in range(n_base_events):
            days_offset = np.random.uniform(0, self.time_span_days)
            event_time = base_date + timedelta(days=days_offset) #generating in a random sequence

            #late or on-time arrival
            if np.random.random() < late_arrival_ratio:
                arrival_days_offset = days_offset + np.random.uniform(1, 10)
            else:
                arrival_days_offset = days_offset + np.random.uniform(0, 1)

            #transactional values, ie. what happened
            arrival_time = base_date + timedelta(days=arrival_days_offset)
            product_index = np.random.randint(0, 10)
            region_index = np.random.randint(0, 10)
            quantity = np.random.randint(1, 25)
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
        
        #Updates to existing events
        for _ in range(n_updates):
            if not base_events:
                break
            prior_event = base_events[np.random.randint(0, len(base_events))] #pick one event to be updated
            update_arrival_time = _sample_anomaly_arrival(prior_event["arrival_time"])
            product_index = np.random.randint(0, 10)
            region_index = np.random.randint(0, 10)
            base_events.append({
                "event_id": event_id_counter,
                "sale_id": prior_event["sale_id"],
                "event_time": prior_event["event_time"],
                "arrival_time": update_arrival_time,
                "product_name": product_names[product_index],
                "category": product_categories[product_index],
                "region_name": region_names[region_index],
                "country": region_countries[region_index],
                "quantity": np.random.randint(1, 50),
                "amount": _sample_amount(mean=4.5, sigma=1.0, min_value=1.0, max_value=5000.0),
                "is_update": True,
                "is_deleted": False,
            })
            event_id_counter += 1
        #Deletes to existing events
        for _ in range(n_deletes):
            if not base_events:
                break
            prior_event = base_events[np.random.randint(0, len(base_events))] #pick one event to be deleted
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

        #shuffle the updates/deletes between the base events
        np.random.shuffle(base_events)

        #Ensure no event arrives at exactly the same time to avoid measuring conflicts
        events_df = pd.DataFrame(base_events)
        events_df = events_df.sort_values(["arrival_time", "event_id"]).reset_index(drop=True)
        arrival_times = events_df["arrival_time"].to_numpy().copy()
        one_microsecond = np.timedelta64(1, "us")
        for idx in range(1, len(arrival_times)):
            if arrival_times[idx] <= arrival_times[idx - 1]:
                arrival_times[idx] = arrival_times[idx - 1] + one_microsecond
        events_df["arrival_time"] = arrival_times

        return events_df

    def create_source_table( #Returns the connection and stats
        self,
        conn: duckdb.DuckDBPyConnection | None = None,
        table_name: str = "events_source",
    ) -> tuple[duckdb.DuckDBPyConnection, dict]:
        
        #call the generator
        events_df = self.generate_events()
        if conn is None:
            conn = duckdb.connect(":memory:")
        conn.register("events_df", events_df)
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM events_df")
        print(f"[OK] Database table '{table_name}' created with {len(events_df)} events inserted")

        #get stats
        stats = conn.execute(
            f"SELECT COUNT(*) AS total_events, MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time FROM {table_name}"
        ).df()
        stats_dict = stats.iloc[0].to_dict()
        return conn, stats_dict
