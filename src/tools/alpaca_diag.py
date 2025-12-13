"""
Quick Alpaca paper diagnostic: clock/account and a place/cancel cycle for a non-marketable order.

Usage:
  python -m src.tools.alpaca_diag
Requires ALPACA_API_KEY / ALPACA_API_SECRET / ALPACA_API_BASE_URL in environment.
"""

from __future__ import annotations

import os
import sys
import uuid
import requests
from dotenv import load_dotenv


def get_env_or_exit(name: str) -> str:
    val = os.getenv(name)
    if not val:
        sys.exit(f"Missing required env var: {name}")
    return val


def main() -> None:
    load_dotenv()
    api_key = get_env_or_exit("ALPACA_API_KEY")
    api_secret = get_env_or_exit("ALPACA_API_SECRET")
    base_url = os.getenv("ALPACA_API_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")

    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
        "Content-Type": "application/json",
    }

    def get(path: str):
        resp = requests.get(f"{base_url}{path}", headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def post(path: str, json_body: dict):
        resp = requests.post(f"{base_url}{path}", headers=headers, json=json_body, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def delete(path: str):
        resp = requests.delete(f"{base_url}{path}", headers=headers, timeout=10)
        resp.raise_for_status()
        # Alpaca often returns 204 with empty body for cancel; handle gracefully.
        if resp.content:
            try:
                return resp.json()
            except Exception:
                return {"status_code": resp.status_code, "content": resp.text}
        return {"status_code": resp.status_code}

    print("Checking clock...")
    clock = get("/v2/clock")
    print("Clock:", clock)

    print("Checking account...")
    acct = get("/v2/account")
    print("Account status:", acct.get("status"), "equity:", acct.get("equity"))

    # Place a non-marketable limit buy for 1 share far from market; extended_hours True.
    symbol = "AAPL"
    client_order_id = f"diag-{uuid.uuid4().hex[:8]}"
    payload = {
        "symbol": symbol,
        "qty": 1,
        "side": "buy",
        "type": "limit",
        "limit_price": 0.01,  # far below market to avoid fill
        "time_in_force": "day",
        "extended_hours": True,
        "client_order_id": client_order_id,
    }
    print(f"Placing test order for {symbol} (non-marketable)...")
    order = post("/v2/orders", payload)
    order_id = order.get("id")
    print("Order accepted:", order_id, "status:", order.get("status"))

    if order_id:
        print("Canceling test order...")
        try:
            cancel_resp = delete(f"/v2/orders/{order_id}")
            print("Cancel response:", cancel_resp)
        except Exception as exc:  # noqa: BLE001
            print("Cancel failed:", exc)

    print("Done.")


if __name__ == "__main__":
    main()
