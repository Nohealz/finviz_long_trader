"""
Rebuild a PnL log for a given date directly from Alpaca fill activities.

Usage:
  python -m src.tools.pnl_rebuild_from_alpaca --date 2025-12-15

This will write to data/pnl-<date>.log (overwriting if it exists) and print a
small summary to stdout.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Dict

from alpaca.trading.client import TradingClient
from dotenv import load_dotenv


def load_creds() -> dict:
    load_dotenv()
    key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_API_SECRET")
    base = os.getenv("ALPACA_API_BASE_URL", "https://paper-api.alpaca.markets")
    if not key or not secret:
        raise SystemExit("Missing ALPACA_API_KEY/ALPACA_API_SECRET in environment")
    return {"key": key, "secret": secret, "base": base}


def fetch_fills(trading: TradingClient, start: dt.datetime, end: dt.datetime) -> list[dict]:
    params = {
        "activity_types": "FILL",
        "after": start.astimezone(dt.timezone.utc).isoformat(),
        "until": end.astimezone(dt.timezone.utc).isoformat(),
        "direction": "asc",
        "page_size": 100,
    }
    results: list[dict] = []
    page_token = None
    while True:
        if page_token:
            params["page_token"] = page_token
        batch = trading.get("/account/activities", params)
        if not batch:
            break
        results.extend(batch)
        if len(batch) < params["page_size"]:
            break
        page_token = batch[-1].get("id")
        if not page_token:
            break
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="ISO date (YYYY-MM-DD), defaults to today in America/New_York")
    args = parser.parse_args()

    tz = dt.timezone(dt.timedelta(hours=-5))  # America/New_York (no DST handling here)
    if args.date:
        target_date = dt.date.fromisoformat(args.date)
    else:
        target_date = dt.datetime.now(tz).date()

    start = dt.datetime.combine(target_date, dt.time(0, 0), tzinfo=tz)
    end = start + dt.timedelta(days=1)

    creds = load_creds()
    trading = TradingClient(creds["key"], creds["secret"], paper=True, url_override=creds["base"])

    activities = fetch_fills(trading, start, end)
    # Sort defensively.
    activities = sorted(
        activities,
        key=lambda a: a.get("transaction_time") or a.get("date") or "",
    )

    out_path = Path("data") / f"pnl-{target_date.isoformat()}.log"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        positions: Dict[str, dict] = {}
        realized_total = 0.0
        for act in activities:
            try:
                sym = act["symbol"]
                qty = int(act.get("qty") or 0)
                price = float(act.get("price") or 0)
                side = (act.get("side") or "").lower()
                ts_raw = act.get("transaction_time") or act.get("date")
                ts = ts_raw.replace("Z", "+00:00") if ts_raw else dt.datetime.now(dt.timezone.utc).isoformat()
            except Exception:
                continue
            if qty <= 0 or price <= 0 or side not in {"buy", "sell"}:
                continue

            pos = positions.setdefault(sym, {"qty": 0, "avg": 0.0})

            if side == "buy":
                new_qty = pos["qty"] + qty
                new_avg = (pos["avg"] * pos["qty"] + price * qty) / new_qty if new_qty else 0.0
                pos["qty"], pos["avg"] = new_qty, new_avg
                f.write(
                    json.dumps(
                        {
                            "event": "entry",
                            "symbol": sym,
                            "timestamp": ts,
                            "price": price,
                            "quantity": qty,
                            "order_id": act.get("order_id") or act.get("id"),
                        }
                    )
                    + "\n"
                )
            else:  # sell
                if pos["qty"] <= 0:
                    # No buys recorded; treat as flat and skip PnL.
                    pnl_delta = 0.0
                    pos["qty"] = 0
                else:
                    sell_qty = min(qty, pos["qty"])
                    pnl_delta = (price - pos["avg"]) * sell_qty
                    pos["qty"] -= sell_qty
                    if pos["qty"] == 0:
                        pos["avg"] = 0.0
                    realized_total += pnl_delta
                f.write(
                    json.dumps(
                        {
                            "event": "exit_fill",
                            "symbol": sym,
                            "timestamp": ts,
                            "price": price,
                            "quantity": qty,
                            "pnl_delta": pnl_delta,
                            "order_id": act.get("order_id") or act.get("id"),
                        }
                    )
                    + "\n"
                )
                if pos["qty"] == 0:
                    f.write(
                        json.dumps(
                            {
                                "event": "close",
                                "symbol": sym,
                                "timestamp": ts,
                                "realized_pnl": pnl_delta,
                            }
                        )
                        + "\n"
                    )

    print(f"Wrote rebuilt PnL log to {out_path}")
    # Auto-run summary.
    try:
        from src.tools.pnl_summary import summarise_and_write

        output, out_file = summarise_and_write(out_path)
        print(output)
        print(f"Summary written to: {out_file}")
    except Exception as exc:  # pragma: no cover - best-effort
        print(f"Summary failed: {exc}")


if __name__ == "__main__":
    main()
