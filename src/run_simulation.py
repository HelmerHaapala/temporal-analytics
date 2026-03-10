"""Simulation entry point."""

import argparse

from scenarios.simulation_runner import run_scenarios


def run_simulation(
    n_events: int = 1500,
    time_span: int = 45,
    anomaly_ratio: float = 0.65,
    seed: int = 42,
) -> None:
    print("\nSTART")
    run_scenarios(
        n_events=n_events,
        time_span=time_span,
        anomaly_ratio=anomaly_ratio,
        seed=seed,
    )
    print("\nCOMPLETED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-events", type=int, default=100000)
    parser.add_argument("--time-span", type=int, default=95)
    parser.add_argument("--anomaly-ratio", type=float, default=0.65)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    run_simulation(
        n_events=args.n_events,
        time_span=args.time_span,
        anomaly_ratio=args.anomaly_ratio,
        seed=args.seed,
    )
