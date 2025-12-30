from __future__ import annotations

import argparse
from dotenv import load_dotenv

from .brain_service import build_services


def main() -> None:
    parser = argparse.ArgumentParser(description="Run end-of-day liquidation now.")
    parser.add_argument(
        "--after-hours",
        action="store_true",
        help="Force after-hours liquidation using limit orders (extended hours).",
    )
    args = parser.parse_args()
    load_dotenv()
    scheduler = build_services()
    if hasattr(scheduler, "strategy"):
        scheduler.strategy.run_eod_liquidation(force_after_hours=args.after_hours)
    elif scheduler._eod_callback:
        scheduler._eod_callback()
    else:
        print("No EOD callback configured.")


if __name__ == "__main__":
    main()
