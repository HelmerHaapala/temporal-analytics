"""Scenario model and simulation scenario definitions."""

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    description: str
    freshness_target_minutes: Optional[float]
    live_accuracy_target_ratio: Optional[float] = None
    daily_window_accuracy_target_ratio: Optional[float] = None
    daily_window_accuracy_delay_hours: Optional[float] = None
    weekly_window_accuracy_target_ratio: Optional[float] = None
    weekly_window_accuracy_delay_hours: Optional[float] = None
    stability_max_revision_ratio: Optional[float] = None

    @property
    def require_live_accuracy(self) -> bool:
        return self.live_accuracy_target_ratio is not None

    @property
    def require_daily_window_accuracy(self) -> bool:
        return self.daily_window_accuracy_target_ratio is not None

    @property
    def require_weekly_window_accuracy(self) -> bool:
        return self.weekly_window_accuracy_target_ratio is not None

BASELINE_SCENARIO = Scenario(
    scenario_id="B0",
    description="Baseline reference: fixed architecture parameters; no business targets",
    freshness_target_minutes=None,
    stability_max_revision_ratio=None,
)


BUSINESS_SCENARIOS: List[Scenario] = [
    Scenario(
        scenario_id="S1",
        description=(
            "Live Sales Dashboard: freshness <= 10 minutes; "
            "live point-in-time accuracy >= 60%"
        ),
        freshness_target_minutes=10.0,
        live_accuracy_target_ratio=0.60,
    ),
    Scenario(
        scenario_id="S2",
        description=(
            "Hourly Sales Dashboard: freshness <= 1 hour; "
            "live point-in-time accuracy >= 80%"
        ),
        freshness_target_minutes=60.0,
        live_accuracy_target_ratio=0.80,
    ),
    Scenario(
        scenario_id="S3",
        description=(
            "Daily Sales Dashboard: freshness <= 24 hours; "
            "latest closed 24-hour window accuracy >= 99% after 8 hours"
        ),
        freshness_target_minutes=24.0 * 60.0,
        daily_window_accuracy_target_ratio=0.99,
        daily_window_accuracy_delay_hours=8.0,
    ),
    Scenario(
        scenario_id="S4",
        description=(
            "Weekly Management Review: freshness <= 7 days; "
            "latest closed 7-day window accuracy >= 99.9%; "
            "same-horizon restatement ratio <= 0%"
        ),
        freshness_target_minutes=7.0 * 24.0 * 60.0,
        weekly_window_accuracy_target_ratio=0.999,
        weekly_window_accuracy_delay_hours=0.0,
        stability_max_revision_ratio=0.0,
    ),
    Scenario(
        scenario_id="S5",
        description=(
            "Combined Requirements: freshness <= 10 minutes; "
            "live point-in-time accuracy >= 80%; "
            "latest closed 24-hour window accuracy >= 99% after 8 hours; "
            "latest closed 7-day window accuracy >= 99.9%; "
            "same-horizon restatement ratio <= 0%"
        ),
        freshness_target_minutes=10.0,
        live_accuracy_target_ratio=0.80,
        daily_window_accuracy_target_ratio=0.99,
        daily_window_accuracy_delay_hours=8.0,
        weekly_window_accuracy_target_ratio=0.999,
        weekly_window_accuracy_delay_hours=0.0,
        stability_max_revision_ratio=0.0,
    ),
]


SCENARIOS: List[Scenario] = [BASELINE_SCENARIO, *BUSINESS_SCENARIOS]


ARCHITECTURE_ORDER = [
    "ground_truth",
    "BATCH_reference",
    "A_closed_snapshot_warehouse",
    "B_open_evolving_stream",
    "C_window_bounded_stream",
    "D_log_consistent_htap",
    "E_virtual_semantic_snapshot",
]
