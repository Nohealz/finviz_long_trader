from __future__ import annotations

"""
Utility script: fetch intraday high-of-day for symbols.

Usage examples:
  python -m src.tools.intraday_high --symbols AAPL,MSFT
  python -m src.tools.intraday_high              # defaults to symbols in state.json positions
"""

import argparse
import datetime as dt
import sys
from typing import List, Dict, Optional

import requests
from dotenv import load_dotenv

from src.brain.config import Settings
from src.brain.state_store import JsonStateStore
from src.shared.time_utils import now
from src.shared.logging_setup import configure_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch intraday high-of-day for symbols.")
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols. If omitted, uses symbols from current positions in state.",
    )
    parser.add_argument(
        "--resolution",
        type=str,
        default="5",
        help="Finnhub candle resolution (1,5,15,30,60). Default: 5",
    )
    return parser.parse_args()


def load_symbols(args: argparse.Namespace, state_path) -> List[str]:
    if args.symbols:
        return [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    store = JsonStateStore(state_path)
    return sorted(store.positions.keys())


def get_intraday_high(
    symbol: str,
    settings: Settings,
    logger,
    session: requests.Session,
    resolution: str = "5",
    use_candle: bool = False,
) -> Optional[float]:
    """
    Prefer Finnhub /quote high-of-day (works on free tiers). If explicitly requested,
    fall back to candle endpoint.
    """
    base = "https://finnhub.io/api/v1"

    # First try /quote for HOD
    try:
        resp = session.get(f"{base}/quote", params={"symbol": symbol, "token": settings.FINNHUB_API_KEY}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            hod = data.get("h")
            if hod is not None and hod > 0:
                return float(hod)
        else:
            logger.warning("Finnhub quote error %s for %s: %s", resp.status_code, symbol, resp.text[:200])
            resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Finnhub quote failed for %s: %s", symbol, exc)

    if not use_candle:
        return None

    # Optional candle path (may be restricted on free plans).
    end = int(now(settings.TIMEZONE).timestamp())
    start_of_day = now(settings.TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    start = int(start_of_day.timestamp())
    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": start,
        "to": end,
        "token": settings.FINNHUB_API_KEY,
    }
    resp = session.get(f"{base}/stock/candle", params=params, timeout=10)
    if resp.status_code != 200:
        logger.warning(
            "Finnhub candle error %s for %s (resolution=%s): %s",
            resp.status_code,
            symbol,
            resolution,
            resp.text[:200],
        )
        resp.raise_for_status()
    data = resp.json()
    if data.get("s") != "ok" or "h" not in data:
        logger.warning("Finnhub candle response not ok for %s: %s", symbol, data.get("s"))
        return None
    highs = data.get("h", [])
    return max(highs) if highs else None


def main() -> None:
    load_dotenv()
    settings = Settings()
    logger = configure_logging(str(settings.LOG_FILE))
    args = parse_args()
    session = requests.Session()
    symbols = load_symbols(args, settings.STATE_FILE)
    if not symbols:
        print("No symbols provided or found in state.")
        sys.exit(0)
    print(f"Fetching intraday highs (resolution {args.resolution}) for: {', '.join(symbols)}")
    for sym in symbols:
        try:
            high = get_intraday_high(sym, settings, logger, session, resolution=args.resolution, use_candle=False)
            if high is None:
                print(f"{sym}: no data")
            else:
                print(f"{sym}: HOD {high:.4f}")
        except Exception as exc:  # noqa: BLE001
            print(f"{sym}: error {exc}")
            logger.warning("Intraday high fetch failed for %s: %s", sym, exc)


if __name__ == "__main__":
    main()
