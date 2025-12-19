from __future__ import annotations

import logging
from typing import Dict, List

from src.brain.models import Fill, Order, OrderSide, OrderStatus, OrderType, Quote
from src.execution.broker_interface import Broker, MarketDataProvider


class PaperBroker(Broker):
    """
    Simple in-memory paper broker that simulates fills using provided quotes.
    """

    def __init__(self, market_data: MarketDataProvider, logger: logging.Logger | None = None) -> None:
        self.market_data = market_data
        self.logger = logger or logging.getLogger(__name__)
        self.open_orders: Dict[str, Order] = {}

    def place_order(self, order: Order) -> Order:
        order_copy = order.model_copy(deep=True)
        order_copy.mark_status(OrderStatus.WORKING)
        self.open_orders[order_copy.id] = order_copy
        self.logger.debug("Order accepted: %s %s (%s)", order_copy.side, order_copy.symbol, order_copy.id)
        return order_copy

    def get_open_orders(self) -> List[Order]:
        return list(self.open_orders.values())

    def simulate_minute(self, quotes: Dict[str, Quote], use_high_for_limits: bool = False) -> List[Fill]:
        fills: List[Fill] = []
        to_remove: List[str] = []
        for order_id, order in list(self.open_orders.items()):
            quote = quotes.get(order.symbol)
            if not quote:
                continue
            if order.type == OrderType.MARKET:
                # Market fill: buy at ask if available, sell at bid if available, otherwise last.
                if order.side == OrderSide.SELL:
                    price = quote.bid if quote.bid else quote.last
                else:
                    price = quote.ask if quote.ask else quote.last
                fills.append(
                    Fill(
                        order_id=order.id,
                        symbol=order.symbol,
                        quantity=order.quantity,
                        price=price,
                        side=order.side,
                    )
                )
                to_remove.append(order_id)
            elif order.type == OrderType.LIMIT and order.side == OrderSide.SELL:
                target_hit = False
                if use_high_for_limits and quote.high is not None and order.price is not None and quote.high >= order.price:
                    target_hit = True
                elif quote.mid and order.price and quote.mid >= order.price:
                    target_hit = True
                if target_hit:
                    fills.append(
                        Fill(
                            order_id=order.id,
                            symbol=order.symbol,
                            quantity=order.quantity,
                            price=order.price,
                            side=order.side,
                        )
                    )
                    to_remove.append(order_id)
            # Additional order types can be added here later.

        for order_id in to_remove:
            self.open_orders.pop(order_id, None)
        if fills:
            self.logger.debug("Simulated fills: %d", len(fills))
        return fills
