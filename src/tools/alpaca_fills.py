"""
Fetch Alpaca fills (activities) for today and print a summary.

Usage:
  python -m src.tools.alpaca_fills
Requires ALPACA_API_KEY / ALPACA_API_SECRET / ALPACA_API_BASE_URL in environment.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from typing import List, Dict, Any

import requests
from dotenv import load_dotenv


def get_env_or_exit(name: str) -> str:
    val = os.getenv(name)
    if not val:
        sys.exit(f"Missing required env var: {name}")
    return val


def fetch_fills() -> List[Dict[str, Any]]:
    load_dotenv()
    api_key = get_env_or_exit("ALPACA_API_KEY")
    api_secret = get_env_or_exit("ALPACA_API_SECRET")
    base_url = os.getenv("ALPACA_API_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")

    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }
    today = date.today().isoformat()
    params = {
        "after": f"{today}T00:00:00Z",
        "until": f"{today}T23:59:59Z",
        "direction": "asc",
        "page_size": 100,
        "activity_types": "FILL",
    }
    url = f"{base_url}/v2/account/activities"
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def summarize(fills: List[Dict[str, Any]]) -> None:
    total = 0.0
    by_symbol: Dict[str, float] = {}
    for f in fills:
        sym = f.get("symbol")
        price = float(f.get("price", 0))
        qty = float(f.get("qty", 0))
        side = f.get("side", "").lower()
        # For buys, pnl delta negative; for sells, positive
        delta = price * qty * (-1 if side == "buy" else 1)
        by_symbol[sym] = by_symbol.get(sym, 0.0) + delta
        total += delta
    print(f"Fills today: {len(fills)} | Gross notional (buys negative, sells positive): {total:.2f}")
    for sym, val in sorted(by_symbol.items()):
        print(f"{sym}: {val:.2f}")


def main() -> None:
    fills = fetch_fills()
    if not fills:
        print("No fills today.")
        return
    summarize(fills)
    print("--- Raw fills ---")
    for f in fills:
        print(f)


if __name__ == "__main__":
    main()
