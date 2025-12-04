from __future__ import annotations

import logging
import math
from typing import Dict, List, Set

from .config import Settings
from .finviz_client import FinvizScreenerClient
from .models import (
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Quote,
)
from .state_store import JsonStateStore
from ..execution.broker_interface import Broker, MarketDataProvider


class Strategy:
    """
    Encapsulates trading decisions: when to buy, when to place targets, and how to
    update positions from fills.
    """

    def __init__(
        self,
        settings: Settings,
        screener: FinvizScreenerClient,
        fill_data: MarketDataProvider,
        buy_data: MarketDataProvider,
        broker: Broker,
        state_store: JsonStateStore,
        logger: logging.Logger | None = None,
    ) -> None:
        self.settings = settings
        self.screener = screener
        self.fill_data = fill_data
        self.buy_data = buy_data
        self.broker = broker
        self.state_store = state_store
        self.logger = logger or logging.getLogger(__name__)

    def run_tick(self) -> None:
        screener_symbols = self.screener.get_symbols()
        positions = self.state_store.get_open_positions()
        open_order_symbols = {order.symbol for order in self.state_store.orders.values() if order.status in {OrderStatus.NEW, OrderStatus.WORKING}}
        pending_buys = {
            order.symbol
            for order in self.state_store.orders.values()
            if order.side == OrderSide.BUY and order.status in {OrderStatus.NEW, OrderStatus.WORKING}
        }

        # Quotes for filling open orders (limit sells) only.
        symbols_for_fills: Set[str] = set(open_order_symbols)
        fill_quotes: Dict[str, Quote] = {}
        if symbols_for_fills:
            fill_quotes = self.fill_data.get_quotes(sorted(symbols_for_fills))

        fills = self.broker.simulate_minute(fill_quotes) if fill_quotes else []
        self._process_fills(fills)

        # Evaluate fresh buys after processing prior fills.
        self._evaluate_new_buys(screener_symbols, pending_buys)

        # After placing new buys, use buy_data quotes to fill them immediately.
        # Note: reuse existing fill_quotes if available; otherwise fetch buy quotes for the new orders' symbols.
        new_buy_symbols = {
            order.symbol
            for order in self.state_store.orders.values()
            if order.side == OrderSide.BUY and order.status == OrderStatus.WORKING
        }
        if new_buy_symbols:
            buy_quotes = self.buy_data.get_quotes(sorted(new_buy_symbols))
            post_order_fills = self.broker.simulate_minute(buy_quotes)
            self._process_fills(post_order_fills)

    def _evaluate_new_buys(self, screener_symbols: List[str], pending_buys: Set[str]) -> None:
        positions = self.state_store.get_open_positions()
        buy_candidates = []
        for symbol in screener_symbols:
            if symbol in positions or symbol in pending_buys:
                continue
            buy_candidates.append(symbol)

        if not buy_candidates:
            return

        buy_quotes = self.buy_data.get_quotes(buy_candidates)
        for symbol in buy_candidates:
            quote = buy_quotes.get(symbol)
            if not quote:
                self.logger.debug("No buy quote for %s; skipping buy decision", symbol)
                continue
            shares = max(1, math.ceil(self.settings.BASE_POSITION_DOLLARS / quote.last))
            order = Order(
                symbol=symbol,
                side=OrderSide.BUY,
                type=OrderType.MARKET,
                quantity=shares,
                status=OrderStatus.NEW,
                tags=["entry"],
            )
            placed = self.broker.place_order(order)
            self.state_store.upsert_order(placed)
            self.logger.info("Placed market buy for %s: %s shares", symbol, shares)

    def _process_fills(self, fills: List[Fill]) -> None:
        for fill in fills:
            order = self.state_store.get_order(fill.order_id)
            if not order:
                self.logger.warning("Fill for unknown order %s", fill.order_id)
                continue
            order.mark_status(OrderStatus.FILLED)
            self.state_store.upsert_order(order)
            self.state_store.record_fill(fill)
            self.logger.info("Order %s filled for %s @ %.2f (%s shares)", order.id, order.symbol, fill.price, fill.quantity)
            if order.side == OrderSide.BUY:
                self._handle_buy_fill(order, fill)
            else:
                self._handle_sell_fill(order, fill)

    def _handle_buy_fill(self, order: Order, fill: Fill) -> None:
        position = self.state_store.positions.get(order.symbol)
        if position:
            position.apply_buy_fill(fill)
        else:
            position = Position(
                symbol=order.symbol,
                total_shares=fill.quantity,
                avg_price=fill.price,
                cash_invested=fill.price * fill.quantity,
                realized_pnl=0.0,
                open_target_orders=[],
            )
        self._place_targets(position, fill.price, fill.quantity)
        self.state_store.upsert_position(position)

    def _handle_sell_fill(self, order: Order, fill: Fill) -> None:
        position = self.state_store.positions.get(order.symbol)
        if not position:
            self.logger.warning("Sell fill for %s with no tracked position", order.symbol)
            return
        position.apply_sell_fill(fill)
        if order.id in position.open_target_orders:
            position.open_target_orders.remove(order.id)
        if position.closed:
            position.open_target_orders.clear()
            self.logger.info("Position %s fully closed; realized PnL %.2f", position.symbol, position.realized_pnl)
        self.state_store.upsert_position(position)

    def _place_targets(self, position: Position, entry_price: float, total_shares: int) -> None:
        """
        Create four staged target orders: +10%, +20%, +50%, +100%.
        """
        first = math.floor(total_shares * 0.25)
        second = math.floor(total_shares * 0.25)
        third = math.floor(total_shares * 0.25)
        fourth = total_shares - (first + second + third)
        targets = [
            ("target_10", entry_price * 1.10, first),
            ("target_20", entry_price * 1.20, second),
            ("target_50", entry_price * 1.50, third),
            ("target_100", entry_price * 2.00, fourth),
        ]
        for tag, price, qty in targets:
            if qty <= 0:
                continue
            order = Order(
                symbol=position.symbol,
                side=OrderSide.SELL,
                type=OrderType.LIMIT,
                price=price,
                quantity=qty,
                status=OrderStatus.NEW,
                tags=[tag],
            )
            placed = self.broker.place_order(order)
            position.open_target_orders.append(placed.id)
            self.state_store.upsert_order(placed)
            self.logger.info(
                "Placed %s limit sell for %s: %s shares @ %.2f",
                tag,
                position.symbol,
                qty,
                price,
            )
