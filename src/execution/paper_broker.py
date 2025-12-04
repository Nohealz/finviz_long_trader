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

    def simulate_minute(self, quotes: Dict[str, Quote]) -> List[Fill]:
        fills: List[Fill] = []
        to_remove: List[str] = []
        for order_id, order in list(self.open_orders.items()):
            quote = quotes.get(order.symbol)
            if not quote:
                continue
            if order.type == OrderType.MARKET:
                price = quote.last * 1.001  # Approximate bar high for market buys
                fills.append(
                    Fill(
                        order_id=order.id,
                        symbol=order.symbol,
                        quantity=order.quantity,
                        price=price,
                    )
                )
                to_remove.append(order_id)
            elif order.type == OrderType.LIMIT and order.side == OrderSide.SELL:
                if quote.mid and order.price and quote.mid >= order.price:
                    fills.append(
                        Fill(
                            order_id=order.id,
                            symbol=order.symbol,
                            quantity=order.quantity,
                            price=order.price,
                        )
                    )
                    to_remove.append(order_id)
            # Additional order types can be added here later.

        for order_id in to_remove:
            self.open_orders.pop(order_id, None)
        if fills:
            self.logger.debug("Simulated fills: %d", len(fills))
        return fills
