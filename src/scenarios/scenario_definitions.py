"""
Scenario model and baseline scenario definitions.
"""

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
    "S3_monthly_management_flash": "S3",
    "S4_weekly_partner_payout_preview": "S4",
}


def normalize_scenario_id(value: object) -> str:
    text = str(value)
    return LEGACY_SCENARIO_ID_MAP.get(text, text)


SCENARIOS: List[Scenario] = [
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
            "Daily Sales Dashboard: freshness <= 8 hours; "
            "point-in-time accuracy >= 90%"
        ),
        freshness_target_minutes=8.0 * 60.0,
        accuracy_target_ratio=0.90,
        stability_max_revision_ratio=None,
        monthly_accuracy_target_ratio=None,
    ),
    Scenario(
        scenario_id="S3",
        description=(
            "Monthly Management Flash: freshness <= 24 hours; "
            "monthly accuracy >= 99%"
        ),
        freshness_target_minutes=24.0 * 60.0,
        accuracy_target_ratio=None,
        stability_max_revision_ratio=None,
        monthly_accuracy_target_ratio=0.99,
    ),
    Scenario(
        scenario_id="S4",
        description=(
            "Weekly Partner Payout Preview: freshness <= 6 hours; "
            "point-in-time accuracy >= 97%; same-horizon restatement ratio <= 10%"
        ),
        freshness_target_minutes=6.0 * 60.0,
        accuracy_target_ratio=0.97,
        stability_max_revision_ratio=0.10,
        monthly_accuracy_target_ratio=None,
    ),
]


ARCHITECTURE_ORDER = [
    "ground_truth",
    "BATCH_reference",
    "A_closed_snapshot_warehouse",
    "B_open_evolving_stream",
    "C_window_bounded_stream",
    "D_log_consistent_htap",
    "E_virtual_semantic_snapshot",
]
