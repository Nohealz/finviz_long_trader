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
from ..shared.time_utils import now
from ..shared.pnl_logger import PnLLogger


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
        self.pnl_logger = PnLLogger(settings.PNL_LOG_FILE, logger=self.logger)
        self._eod_done_date: str | None = None

    def run_tick(self) -> None:
        screener_symbols = self.screener.get_symbols()
        positions = self.state_store.get_open_positions()
        pending_buys = {
            order.symbol
            for order in self.state_store.orders.values()
            if order.side == OrderSide.BUY and order.status in {OrderStatus.NEW, OrderStatus.WORKING}
        }

        buy_candidates = [sym for sym in screener_symbols if sym not in positions and sym not in pending_buys]

        open_order_symbols = [
            order.symbol
            for order in self.state_store.orders.values()
            if order.status in {OrderStatus.NEW, OrderStatus.WORKING}
        ]

        buy_quotes: Dict[str, Quote] = {}
        placed_symbols: Set[str] = set()
        if buy_candidates:
            buy_quotes = self.buy_data.get_quotes(buy_candidates)
            placed_symbols = self._place_buys(buy_candidates, buy_quotes)

        fill_quotes: Dict[str, Quote] = {}
        if open_order_symbols:
            fill_quotes = self.fill_data.get_quotes(open_order_symbols)

        combined_quotes = dict(fill_quotes)
        combined_quotes.update({sym: buy_quotes[sym] for sym in placed_symbols if sym in buy_quotes})

        if not combined_quotes:
            return

        current = now(self.settings.TIMEZONE)
        # Use high-of-day for limit sells even premarket so we catch fills on HOD moves.
        use_high_for_limits = True
        fills = self.broker.simulate_minute(combined_quotes, use_high_for_limits=use_high_for_limits)
        if fills:
            self._process_fills(fills)

    def _place_buys(self, buy_candidates: List[str], quotes: Dict[str, Quote]) -> Set[str]:
        placed_symbols: Set[str] = set()
        for symbol in buy_candidates:
            quote = quotes.get(symbol)
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
            placed_symbols.add(symbol)
        return placed_symbols

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
            position.closed = False
        else:
            position = Position(
                symbol=order.symbol,
                total_shares=fill.quantity,
                avg_price=fill.price,
                cash_invested=fill.price * fill.quantity,
                realized_pnl=0.0,
                open_target_orders=[],
                closed=False,
            )
        self._place_targets(position, fill.price, fill.quantity)
        self.state_store.upsert_position(position)
        self.pnl_logger.log_entry(order.symbol, fill.timestamp, fill.price, fill.quantity, order.id)

    def _handle_sell_fill(self, order: Order, fill: Fill) -> None:
        position = self.state_store.positions.get(order.symbol)
        if not position:
            self.logger.warning("Sell fill for %s with no tracked position", order.symbol)
            return
        avg_before = position.avg_price
        position.apply_sell_fill(fill)
        if order.id in position.open_target_orders:
            position.open_target_orders.remove(order.id)
        pnl_delta = (fill.price - avg_before) * fill.quantity
        self.pnl_logger.log_exit_fill(order.symbol, fill.timestamp, fill.price, fill.quantity, pnl_delta, order.id)
        if position.closed:
            position.open_target_orders.clear()
            self.logger.info("Position %s fully closed; realized PnL %.2f", position.symbol, position.realized_pnl)
            self.pnl_logger.log_close_summary(position.symbol, fill.timestamp, position.realized_pnl)
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

    def run_eod_liquidation(self) -> None:
        """
        End-of-day cleanup: cancel open targets, market-sell remaining shares, and optionally clear state.
        """
        today = now(self.settings.TIMEZONE).date().isoformat()
        if self._eod_done_date == today:
            return

        self.logger.info("EOD liquidation started")

        # Cancel open target orders
        for order in list(self.state_store.orders.values()):
            if order.side == OrderSide.SELL and order.status in {OrderStatus.NEW, OrderStatus.WORKING}:
                order.mark_status(OrderStatus.CANCELLED)
                self.state_store.upsert_order(order)

        positions = self.state_store.get_open_positions()
        symbols_to_sell = [sym for sym, pos in positions.items() if pos.total_shares > 0]
        if symbols_to_sell:
            quotes = self.buy_data.get_quotes(symbols_to_sell)
            for sym, pos in positions.items():
                if pos.total_shares <= 0:
                    continue
                qty = pos.total_shares
                order = Order(
                    symbol=sym,
                    side=OrderSide.SELL,
                    type=OrderType.MARKET,
                    quantity=qty,
                    status=OrderStatus.NEW,
                    tags=["eod_liquidation"],
                )
                placed = self.broker.place_order(order)
                self.state_store.upsert_order(placed)
                fill_quotes = {sym: quotes.get(sym)} if quotes.get(sym) else {}
                fills = self.broker.simulate_minute(fill_quotes)
                self._process_fills(fills)

        # Mark positions closed
        for pos in positions.values():
            pos.closed = True
            pos.open_target_orders.clear()
            pos.total_shares = 0
            self.state_store.upsert_position(pos)

        if self.settings.EOD_CLEAR_STATE:
            self.state_store.clear()
            self.logger.info("State cleared after EOD liquidation")

        self._eod_done_date = today
