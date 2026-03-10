"""Scenario model and simulation scenario definitions."""

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    description: str
    freshness_target_minutes: Optional[float]
    accuracy_target_ratio: Optional[float]
    stability_max_revision_ratio: Optional[float]
    monthly_accuracy_target_ratio: Optional[float]

    @property
    def require_monthly_accuracy(self) -> bool:
        return self.monthly_accuracy_target_ratio is not None


LEGACY_SCENARIO_ID_MAP = {
    "S1_live_sales_dashboard": "S1",
    "S2_daily_sales_dashboard": "S2",
    "S3_monthly_management_flash": "S4",
    "S4_weekly_partner_payout_preview": "S3",
}


def normalize_scenario_id(value: object) -> str:
    text = str(value)
    return LEGACY_SCENARIO_ID_MAP.get(text, text)


BASELINE_SCENARIO = Scenario(
    scenario_id="B0",
    description="Baseline reference: fixed architecture parameters; no business targets",
    freshness_target_minutes=None,
    accuracy_target_ratio=None,
    stability_max_revision_ratio=None,
    monthly_accuracy_target_ratio=None,
)


BUSINESS_SCENARIOS: List[Scenario] = [
    Scenario(
        scenario_id="S1",
        description=(
            "Live Sales Dashboard: freshness <= 10 minutes; "
            "point-in-time accuracy >= 80%"
        ),
        freshness_target_minutes=10.0,
        accuracy_target_ratio=0.80,
        stability_max_revision_ratio=None,
        monthly_accuracy_target_ratio=None,
    ),
    Scenario(
        scenario_id="S2",
        description=(
            "Daily Sales Dashboard: freshness <= 24 hours; "
            "latest closed 24-hour window accuracy >= 99% after 8 hours"
        ),
        freshness_target_minutes=24.0 * 60.0,
        accuracy_target_ratio=0.99,
        stability_max_revision_ratio=None,
        monthly_accuracy_target_ratio=None,
    ),
    Scenario(
        scenario_id="S3",
        description=(
            "Weekly Partner Payout Preview: freshness <= 7 days; "
            "latest closed 7-day window accuracy >= 99% after 1 day; "
            "same-horizon restatement ratio <= 10%"
        ),
        freshness_target_minutes=7.0 * 24.0 * 60.0,
        accuracy_target_ratio=0.99,
        stability_max_revision_ratio=0.10,
        monthly_accuracy_target_ratio=None,
    ),
    Scenario(
        scenario_id="S4",
        description=(
            "Formal Monthly Reporting: freshness <= 7 days; "
            "monthly accuracy >= 99.9%; same-horizon restatement ratio <= 0%"
        ),
        freshness_target_minutes=7.0 * 24.0 * 60.0,
        accuracy_target_ratio=None,
        stability_max_revision_ratio=0.0,
        monthly_accuracy_target_ratio=0.999,
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
