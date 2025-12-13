from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set

import requests

from src.brain.models import Fill, Order, OrderSide, OrderStatus, OrderType, Quote
from src.execution.broker_interface import Broker, MarketDataProvider


class AlpacaClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str, data_url: str, logger: Optional[logging.Logger] = None) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.data_url = data_url.rstrip("/")
        self.logger = logger or logging.getLogger(__name__)

    def _headers(self) -> dict:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Content-Type": "application/json",
        }

    def post_order(self, symbol: str, qty: int, side: OrderSide, type_: OrderType, limit_price: float | None = None, extended_hours: bool = True) -> dict:
        payload = {
            "symbol": symbol,
            "qty": qty,
            "side": side.name.lower(),
            "type": type_.name.lower(),
            "time_in_force": "day",
            "extended_hours": extended_hours,
        }
        if type_ == OrderType.LIMIT and limit_price:
            payload["limit_price"] = limit_price
        resp = requests.post(f"{self.base_url}/v2/orders", json=payload, headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_orders(self, status: str = "all") -> List[dict]:
        resp = requests.get(f"{self.base_url}/v2/orders", params={"status": status, "direction": "desc"}, headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_quote(self, symbol: str) -> dict:
        resp = requests.get(f"{self.data_url}/v2/stocks/{symbol}/quotes/latest", headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()


def _alpaca_status_to_order_status(status: str) -> OrderStatus:
    mapping = {
        "new": OrderStatus.NEW,
        "partially_filled": OrderStatus.WORKING,
        "filled": OrderStatus.FILLED,
        "done_for_day": OrderStatus.CANCELLED,
        "canceled": OrderStatus.CANCELLED,
        "expired": OrderStatus.CANCELLED,
        "replaced": OrderStatus.WORKING,
        "pending_cancel": OrderStatus.WORKING,
        "pending_replace": OrderStatus.WORKING,
        "pending_new": OrderStatus.NEW,
        "accepted": OrderStatus.WORKING,
    }
    return mapping.get(status, OrderStatus.WORKING)


class AlpacaBroker(Broker):
    """
    Alpaca-backed broker implementation. simulate_minute polls Alpaca orders for fills.
    """

    def __init__(self, client: AlpacaClient, logger: Optional[logging.Logger] = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)
        self.processed_fills: Set[str] = set()

    def place_order(self, order: Order) -> Order:
        resp = self.client.post_order(
            symbol=order.symbol,
            qty=order.quantity,
            side=order.side,
            type_=order.type,
            limit_price=order.price,
            extended_hours=True,
        )
        order_copy = order.model_copy(deep=True)
        order_copy.id = resp.get("id", order_copy.id)
        order_copy.status = _alpaca_status_to_order_status(resp.get("status", "new"))
        return order_copy

    def get_open_orders(self) -> List[Order]:
        orders = []
        for o in self.client.get_orders(status="open"):
            try:
                orders.append(
                    Order(
                        id=o.get("id"),
                        symbol=o["symbol"],
                        side=OrderSide[o["side"].upper()],
                        type=OrderType[o["type"].upper()],
                        price=float(o["limit_price"]) if o.get("limit_price") else None,
                        quantity=int(o["qty"]),
                        status=_alpaca_status_to_order_status(o.get("status", "new")),
                        tags=[],
                    )
                )
            except Exception:
                continue
        return orders

    def simulate_minute(self, quotes: Dict[str, Quote], use_high_for_limits: bool = False) -> List[Fill]:
        fills: List[Fill] = []
        try:
            # Check recently closed/filled orders
            for o in self.client.get_orders(status="all"):
                oid = o.get("id")
                if not oid or oid in self.processed_fills:
                    continue
                status = o.get("status", "")
                if status != "filled":
                    continue
                filled_qty = int(o.get("filled_qty", 0))
                filled_price = float(o.get("filled_avg_price") or o.get("limit_price") or 0)
                if filled_qty <= 0 or filled_price <= 0:
                    continue
                fills.append(
                    Fill(
                        order_id=oid,
                        symbol=o["symbol"],
                        quantity=filled_qty,
                        price=filled_price,
                    )
                )
                self.processed_fills.add(oid)
        except Exception as exc:
            self.logger.warning("Alpaca simulate_minute polling failed: %s", exc)
        return fills


class AlpacaMarketDataProvider(MarketDataProvider):
    def __init__(self, client: AlpacaClient, logger: Optional[logging.Logger] = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        quotes: Dict[str, Quote] = {}
        for sym in symbols:
            try:
                resp = self.client.get_quote(sym)
                q = resp.get("quote", {})
                bid = q.get("bp")
                ask = q.get("ap")
                last = q.get("ap") or q.get("bp") or q.get("mid") or q.get("ap")
                mid = None
                if bid and ask:
                    mid = (float(bid) + float(ask)) / 2.0
                quotes[sym] = Quote(
                    symbol=sym,
                    bid=float(bid) if bid else None,
                    ask=float(ask) if ask else None,
                    last=float(last) if last else None,
                    mid=mid,
                    timestamp=None,
                    high=None,
                )
            except Exception as exc:
                self.logger.debug("Failed to fetch quote for %s: %s", sym, exc)
        return quotes
