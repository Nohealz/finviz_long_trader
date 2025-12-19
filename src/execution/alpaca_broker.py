from __future__ import annotations

import datetime as dt
import logging
from typing import Dict, List, Optional, Set

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.models import Quote as AlpacaQuote
from alpaca.data.requests import StockLatestQuoteRequest, StockLatestTradeRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide as AlpacaOrderSide, OrderType as AlpacaOrderType, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest, MarketOrderRequest
from alpaca.trading.models import TradeActivity

from src.brain.models import Fill, Order, OrderSide, OrderStatus, OrderType, Quote
from src.execution.broker_interface import Broker, MarketDataProvider


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
        "stopped": OrderStatus.CANCELLED,
        "rejected": OrderStatus.REJECTED,
    }
    return mapping.get(status.lower(), OrderStatus.WORKING)


class AlpacaBroker(Broker):
    """
    Alpaca-backed broker using alpaca-py SDK. Polling-based fills with dedupe.
    """

    def __init__(self, api_key: str, api_secret: str, base_url: str, data_url: str, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.trading = TradingClient(api_key, api_secret, paper=True, url_override=base_url)
        self.data = StockHistoricalDataClient(api_key, api_secret, raw_data=False, url_override=data_url)
        self.processed_fills: Set[str] = set()
        self.last_fill_poll: dt.datetime = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)

    def place_order(self, order: Order) -> Order:
        if order.type == OrderType.MARKET:
            req = MarketOrderRequest(
                symbol=order.symbol,
                qty=order.quantity,
                side=AlpacaOrderSide.BUY if order.side == OrderSide.BUY else AlpacaOrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                extended_hours=True,
            )
        else:
            req = LimitOrderRequest(
                symbol=order.symbol,
                qty=order.quantity,
                side=AlpacaOrderSide.BUY if order.side == OrderSide.BUY else AlpacaOrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                limit_price=order.price,
                extended_hours=True,
            )
        alp_order = self.trading.submit_order(req)
        order_copy = order.model_copy(deep=True)
        order_copy.id = str(alp_order.id)
        order_copy.status = _alpaca_status_to_order_status(str(alp_order.status))
        return order_copy

    def get_open_orders(self) -> List[Order]:
        alp_orders = self.trading.get_orders(filter=GetOrdersRequest(status="open", limit=500))
        results: List[Order] = []
        for o in alp_orders:
            try:
                results.append(
                    Order(
                        id=str(o.id),
                        symbol=o.symbol,
                        side=OrderSide.BUY if o.side == AlpacaOrderSide.BUY else OrderSide.SELL,
                        type=OrderType.MARKET if o.type == AlpacaOrderType.MARKET else OrderType.LIMIT,
                        price=float(o.limit_price) if getattr(o, "limit_price", None) else None,
                        quantity=int(o.qty),
                        status=_alpaca_status_to_order_status(str(o.status)),
                        tags=[],
                    )
                )
            except Exception:
                continue
        return results

    def cancel_order(self, order_id: str) -> None:
        try:
            self.trading.cancel_order_by_id(order_id)
        except Exception as exc:
            self.logger.warning("Cancel failed for %s: %s", order_id, exc)

    def simulate_minute(self, quotes: Dict[str, Quote], use_high_for_limits: bool = False) -> List[Fill]:
        # Poll for fills via account activities; ignore quotes.
        fills: List[Fill] = []
        try:
            new_fills = self.get_fills_since(self.last_fill_poll)
            self.last_fill_poll = dt.datetime.now(dt.timezone.utc)
            fills.extend(new_fills)
        except Exception as exc:
            self.logger.warning("Alpaca simulate_minute polling failed: %s", exc)
        return fills

    def list_positions(self) -> List[dict]:
        try:
            return [p.__dict__ for p in self.trading.get_all_positions()]
        except Exception as exc:
            self.logger.warning("Alpaca list_positions failed: %s", exc)
            return []

    def close_all_positions(self, cancel_orders: bool = True) -> None:
        try:
            self.trading.close_all_positions(cancel_orders=cancel_orders)
        except Exception as exc:
            self.logger.error("Alpaca close_all_positions failed: %s", exc)
            raise

    def get_fills_since(self, after: dt.datetime, include_processed: bool = False) -> List[Fill]:
        fills: List[Fill] = []
        try:
            params = {
                "activity_types": "FILL",
                "after": after.isoformat(),
                "direction": "asc",
                "page_size": 100,
            }
            page_token = None
            while True:
                if page_token:
                    params["page_token"] = page_token
                # trading.get expects params as the second positional argument, not keyword.
                activities = self.trading.get("/account/activities", params)
                if not activities:
                    break
                for act in activities:
                    try:
                        fid = act.get("id") or act.get("order_id")
                        if not fid:
                            continue
                        if not include_processed and fid in self.processed_fills:
                            continue
                        qty = int(act.get("qty") or 0)
                        price = float(act.get("price") or 0)
                        if qty <= 0 or price <= 0:
                            continue
                        side_raw = (act.get("side") or "").lower()
                        side = None
                        if side_raw == "buy":
                            side = OrderSide.BUY
                        elif side_raw == "sell":
                            side = OrderSide.SELL
                        ts_raw = act.get("transaction_time")
                        ts = dt.datetime.fromisoformat(ts_raw.replace("Z", "+00:00")) if ts_raw else dt.datetime.now(dt.timezone.utc)
                        fills.append(
                            Fill(
                                id=fid,
                                order_id=act.get("order_id", fid),
                                symbol=act["symbol"],
                                quantity=qty,
                                price=price,
                                side=side,
                                timestamp=ts,
                            )
                        )
                        if not include_processed:
                            self.processed_fills.add(fid)
                    except Exception:
                        continue
                if len(activities) < params["page_size"]:
                    break
                page_token = activities[-1].get("id")
                if not page_token:
                    break
        except Exception as exc:
            self.logger.warning("Alpaca get_fills_since failed: %s", exc)
        return fills


class AlpacaMarketDataProvider(MarketDataProvider):
    def __init__(self, api_key: str, api_secret: str, data_url: str, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.client = StockHistoricalDataClient(api_key, api_secret, raw_data=False, url_override=data_url)

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        quotes: Dict[str, Quote] = {}
        if not symbols:
            return quotes
        try:
            quote_req = StockLatestQuoteRequest(symbol_or_symbols=symbols)
            trade_req = StockLatestTradeRequest(symbol_or_symbols=symbols)
            latest_quotes = self.client.get_stock_latest_quote(quote_req)
            latest_trades = self.client.get_stock_latest_trade(trade_req)

            quote_items = latest_quotes.items() if isinstance(latest_quotes, dict) else [(latest_quotes.symbol, latest_quotes)]
            trade_items = latest_trades.items() if isinstance(latest_trades, dict) else [(latest_trades.symbol, latest_trades)]
            trade_map = {sym: tr for sym, tr in trade_items if tr}

            for sym, q in quote_items:
                if not isinstance(q, AlpacaQuote):
                    continue
                bid = q.bid_price
                ask = q.ask_price
                trade = trade_map.get(sym)
                last = None
                if trade and getattr(trade, "price", None):
                    last = float(trade.price)
                elif ask is not None:
                    last = float(ask)
                elif bid is not None:
                    last = float(bid)
                mid = None
                if bid and ask:
                    mid = (float(bid) + float(ask)) / 2.0
                quotes[sym] = Quote(
                    symbol=sym,
                    bid=float(bid) if bid is not None else None,
                    ask=float(ask) if ask is not None else None,
                    last=last,
                    mid=mid,
                    timestamp=q.timestamp,
                    high=None,
                )
        except Exception as exc:
            self.logger.debug("Failed to fetch quotes from Alpaca: %s", exc)
        return quotes
