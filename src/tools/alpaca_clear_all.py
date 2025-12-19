"""
Alpaca clear-all helper:
- Cancel all open orders.
- Close all open positions.
   * If market is closed (premarket/after-hours), submits limit sells at $0.01 to force fills in extended hours.
   * If market is open, uses market sells.

Usage:
  python -m src.tools.alpaca_clear_all
Requires ALPACA_API_KEY / ALPACA_API_SECRET / ALPACA_API_BASE_URL / ALPACA_DATA_BASE_URL in env.
"""

from __future__ import annotations

import sys
import time
from typing import List

from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest, GetOrdersRequest


def get_env_or_exit(name: str) -> str:
    import os

    val = os.getenv(name)
    if not val:
        sys.exit(f"Missing required env var: {name}")
    return val


def cancel_all(trading: TradingClient) -> None:
    try:
        trading.cancel_orders()
        print("Canceled all open orders.")
    except Exception as exc:
        print(f"Cancel orders failed: {exc}")


def close_positions_market(trading: TradingClient) -> None:
    try:
        trading.close_all_positions(cancel_orders=True)
        print("Submitted market close for all positions.")
    except Exception as exc:
        print(f"Market close-all failed: {exc}")


def close_positions_limit(trading: TradingClient) -> None:
    try:
        positions = trading.get_all_positions()
    except Exception as exc:
        print(f"Failed to fetch positions: {exc}")
        return
    if not positions:
        print("No positions to close.")
        return
    # Ensure no open orders before closing positions.
    open_orders = trading.get_orders(filter=GetOrdersRequest(status="open"))
    if open_orders:
        print(f"{len(open_orders)} open orders remain; canceling before closing positions.")
        trading.cancel_orders()
        # Small wait to let cancels settle
        time.sleep(2)
    for p in positions:
        try:
            qty = int(p.qty)
            if qty <= 0:
                continue
            req = LimitOrderRequest(
                symbol=p.symbol,
                qty=qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                limit_price=0.01,
                extended_hours=True,
            )
            trading.submit_order(req)
            print(f"Submitted limit sell @0.01 for {p.symbol} qty={qty}")
        except Exception as exc:
            print(f"Limit close failed for {p.symbol}: {exc}")


def wait_until_flat(trading: TradingClient, timeout_sec: int = 60, poll_sec: int = 3) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        open_orders = trading.get_orders(filter=GetOrdersRequest(status="open"))
        positions = trading.get_all_positions()
        if not open_orders and not positions:
            print("Account is flat and no open orders.")
            return
        time.sleep(poll_sec)
    print("Timeout waiting to be flat.")


def main() -> None:
    load_dotenv()
    api_key = get_env_or_exit("ALPACA_API_KEY")
    api_secret = get_env_or_exit("ALPACA_API_SECRET")
    base_url = get_env_or_exit("ALPACA_API_BASE_URL").rstrip("/")

    trading = TradingClient(api_key, api_secret, paper=True, url_override=base_url)

    clock = trading.get_clock()
    market_open = clock.is_open
    print(f"Clock: is_open={market_open}, next_open={clock.next_open}, next_close={clock.next_close}")

    cancel_all(trading)

    if market_open:
        close_positions_market(trading)
    else:
        close_positions_limit(trading)

    wait_until_flat(trading, timeout_sec=120, poll_sec=5)


if __name__ == "__main__":
    main()
