from __future__ import annotations

import datetime as dt
import json
import logging
import math
import time
from pathlib import Path
from typing import Dict, List, Set
from zoneinfo import ZoneInfo

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
from ..execution.alpaca_broker import AlpacaBroker
from ..shared.time_utils import now
from ..shared.pnl_logger import PnLLogger
from ..tools.pnl_summary import summarise_and_write
from ..tools.pnl_threshold_chart import generate_threshold_chart


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
        self._last_backfill_attempt: dict[str, dt.datetime] = {}
        self._last_gate_state: bool | None = None
        self._reconciling: bool = False
        # On startup, reconcile local state with broker (Alpaca) to avoid drift.
        self._reconcile_state_with_broker()
        # Seed broker dedupe with persisted fill IDs if supported.
        if isinstance(self.broker, AlpacaBroker):
            self.broker.processed_fills.update(self.state_store.processed_fill_ids)

    def run_tick(self) -> None:
        current = now(self.settings.TIMEZONE)
        # Refresh positions/orders from broker so local cache mirrors Alpaca.
        self._refresh_state_from_broker()
        sym_price_list = self.screener.get_symbols_with_prices()
        screener_symbols = [sym for sym, _ in sym_price_list]
        finviz_prices = {sym: px for sym, px in sym_price_list if px}
        positions = self.state_store.get_open_positions()
        all_positions = self.state_store.positions
        pending_buys = {
            order.symbol
            for order in self.state_store.orders.values()
            if order.side == OrderSide.BUY and order.status in {OrderStatus.NEW, OrderStatus.WORKING}
        }

        today = now(self.settings.TIMEZONE).date().isoformat()
        traded_today = {sym for sym, pos in all_positions.items() if pos.last_entry_date == today}
        # Also respect traded_dates map to block re-entries even after full close.
        for sym in set(self.state_store.traded_dates.keys()):
            if self.state_store.traded_dates.get(sym) == today:
                traded_today.add(sym)

        # Gate: require screener refresh before trading (avoid stale previous-day list).
        screener_ok = True
        if self.settings.FINVIZ_REQUIRE_REFRESH_BEFORE_TRADING:
            today = current.date().isoformat()
            last_date = self.state_store.get_metric("screener_date")
            if last_date != today:
                # New day: reset screener gate
                self.state_store.set_metric("screener_date", today)
                self.state_store.set_metric("screener_initial_hash", None)
                self.state_store.set_metric("screener_refreshed", False)
            cur_hash = hash(tuple(sorted(screener_symbols)))
            initial_hash = self.state_store.get_metric("screener_initial_hash")
            refreshed = self.state_store.get_metric("screener_refreshed", False)
            if initial_hash is None:
                self.state_store.set_metric("screener_initial_hash", cur_hash)
                self.logger.info("Captured initial screener snapshot (%d symbols)", len(screener_symbols))
            elif not refreshed and cur_hash != initial_hash and len(screener_symbols) >= self.settings.FINVIZ_MIN_SYMBOLS:
                self.state_store.set_metric("screener_refreshed", True)
                self.logger.info("Screener list changed; trading unlocked (%d symbols)", len(screener_symbols))
            screener_ok = bool(self.state_store.get_metric("screener_refreshed", False))
            # Log gate state changes (and locked ticks).
            if self._last_gate_state is None or screener_ok != self._last_gate_state:
                state_str = "UNLOCKED" if screener_ok else "LOCKED"
                self.logger.info("Finviz gate state: %s (symbols=%d)", state_str, len(screener_symbols))
            elif not screener_ok:
                # When locked, remind each tick.
                self.logger.info("Finviz gate still LOCKED (symbols=%d)", len(screener_symbols))
            self._last_gate_state = screener_ok

        buy_candidates = []
        if screener_ok:
            buy_candidates = [
                sym
                for sym in screener_symbols
                if sym not in positions and sym not in pending_buys and sym not in traded_today
            ]

        open_order_symbols = [
            order.symbol
            for order in self.state_store.orders.values()
            if order.status in {OrderStatus.NEW, OrderStatus.WORKING}
        ]

        buy_quotes: Dict[str, Quote] = {}
        placed_symbols: Set[str] = set()
        if buy_candidates:
            buy_quotes = self.buy_data.get_quotes(buy_candidates)
            placed_symbols = self._place_buys(buy_candidates, buy_quotes, current, finviz_prices)
            # Optional post-buy fill poll to place targets without waiting for the next tick.
            if placed_symbols and self.settings.POST_BUY_FILL_POLL_SECONDS > 0:
                time.sleep(self.settings.POST_BUY_FILL_POLL_SECONDS)
                try:
                    quick_fills = self.broker.simulate_minute(
                        {sym: q for sym, q in buy_quotes.items() if sym in placed_symbols},
                        use_high_for_limits=False,
                    )
                    if quick_fills:
                        self._process_fills(quick_fills)
                except Exception as exc:
                    self.logger.debug("Quick post-buy fill poll failed: %s", exc)

        fill_quotes: Dict[str, Quote] = {}
        if open_order_symbols:
            fill_quotes = self.fill_data.get_quotes(open_order_symbols)

        combined_quotes = dict(fill_quotes)
        combined_quotes.update({sym: buy_quotes[sym] for sym in placed_symbols if sym in buy_quotes})

        # Add quotes for current positions (for valuation and potential fills) if missing.
        position_symbols = list(positions.keys())
        missing_for_positions = [sym for sym in position_symbols if sym not in combined_quotes]
        if missing_for_positions:
            position_quotes = self.buy_data.get_quotes(missing_for_positions)
            combined_quotes.update(position_quotes)

        # Track invested capital (cost basis) daily max.
        invested_value = 0.0
        for pos in positions.values():
            invested_value += pos.total_shares * pos.avg_price

        max_invested, updated_max = self.state_store.record_invested_value(invested_value, today)
        if updated_max and invested_value > 0:
            self.logger.info("Updated max invested value for %s: %.2f", today, invested_value)

        if not combined_quotes:
            return

        current = now(self.settings.TIMEZONE)
        # Only use high-of-day for limit sells during regular hours.
        use_high_for_limits = current.time() >= self.settings.REGULAR_OPEN
        fills = self.broker.simulate_minute(combined_quotes, use_high_for_limits=use_high_for_limits)
        if fills:
            self._process_fills(fills)

    def _place_buys(self, buy_candidates: List[str], quotes: Dict[str, Quote], current: dt.datetime, finviz_prices: Dict[str, float]) -> Set[str]:
        placed_symbols: Set[str] = set()
        premarket = current.time() < self.settings.REGULAR_OPEN
        slippage = self.settings.PREMARKET_BUY_SLIPPAGE_BPS / 10000.0 if premarket else 0.0
        for symbol in buy_candidates:
            quote = quotes.get(symbol)
            finviz_px = finviz_prices.get(symbol)
            price_for_size = None
            limit_price: float | None = None

            if finviz_px:
                price_for_size = finviz_px
                limit_price = round(finviz_px * 1.5, 2)  # force a fill with high enough limit
                order_type = OrderType.LIMIT
            else:
                if not quote:
                    self.logger.debug("No buy quote for %s; skipping buy decision", symbol)
                    continue
                price_for_size = quote.last or quote.ask or quote.bid
                if not price_for_size:
                    self.logger.debug("No price for %s; skipping buy decision", symbol)
                    continue
                order_type = OrderType.MARKET
                if premarket and isinstance(self.broker, AlpacaBroker):
                    ask = quote.ask or price_for_size
                    limit_price = round(max(0.01, ask * (1 + slippage)), 2)
                    order_type = OrderType.LIMIT

            shares = int(max(1, math.ceil(self.settings.BASE_POSITION_DOLLARS / price_for_size)))
            shares = max(1, shares)  # enforce integer and at least 1
            order = Order(
                symbol=symbol,
                side=OrderSide.BUY,
                type=order_type,
                price=limit_price,
                quantity=shares,
                status=OrderStatus.NEW,
                tags=["entry"],
            )
            placed = self.broker.place_order(order)
            self.state_store.upsert_order(placed)
            if order_type == OrderType.LIMIT:
                self.logger.info(
                    "Placed limit buy for %s: %s shares @ %.2f (premarket=%s)",
                    symbol,
                    shares,
                    limit_price or 0.0,
                    premarket,
                )
            else:
                self.logger.info("Placed market buy for %s: %s shares", symbol, shares)
            placed_symbols.add(symbol)
        return placed_symbols

    def _process_fills(self, fills: List[Fill]) -> None:
        for fill in fills:
            order = self.state_store.get_order(fill.order_id)
            skip_targets = False or self._reconciling
            if not order:
                self.logger.warning("Fill for unknown order %s", fill.order_id)
                # create a placeholder order to keep state consistent, but avoid placing targets
                side = fill.side or OrderSide.BUY
                order = Order(
                    id=fill.order_id,
                    symbol=fill.symbol,
                    side=side,
                    type=OrderType.MARKET,
                    quantity=fill.quantity,
                    status=OrderStatus.NEW,
                    tags=["reconciled_fill"],
                )
                skip_targets = True
                self.state_store.upsert_order(order)
            elif "reconciled_fill" in order.tags:
                skip_targets = True

            order.mark_status(OrderStatus.FILLED)
            self.state_store.upsert_order(order)
            self.state_store.record_fill(fill)
            self.state_store.record_processed_fill_id(fill.id)
            self.logger.info("Order %s filled for %s @ %.2f (%s shares)", order.id, order.symbol, fill.price, fill.quantity)
            if order.side == OrderSide.BUY:
                self._handle_buy_fill(order, fill, skip_targets=skip_targets)
            else:
                self._handle_sell_fill(order, fill)

    def _handle_buy_fill(self, order: Order, fill: Fill, skip_targets: bool = False) -> None:
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
        position.last_entry_date = now(self.settings.TIMEZONE).date().isoformat()
        # If this fill was reconciled (unknown order) or explicitly requested to skip, do not place targets.
        if skip_targets or "reconciled_fill" in order.tags:
            self.logger.info("Skipping targets for reconciled fill on %s", order.symbol)
            self.state_store.upsert_position(position)
            self.state_store.mark_traded(position.symbol, position.last_entry_date)
            self.pnl_logger.log_entry(order.symbol, fill.timestamp, fill.price, fill.quantity, order.id)
            return

        # If this order is only partially filled, defer targets until fully filled.
        filled_qty_for_order = sum(
            f.quantity for f in self.state_store.fills.values() if f.order_id == order.id
        )
        if filled_qty_for_order < order.quantity:
            self.logger.info(
                "Deferring targets for %s; partial fill %s/%s",
                order.symbol,
                filled_qty_for_order,
                order.quantity,
            )
            self.state_store.upsert_position(position)
            self.state_store.mark_traded(position.symbol, position.last_entry_date)
            self.pnl_logger.log_entry(order.symbol, fill.timestamp, fill.price, fill.quantity, order.id)
            return

        # If other buy orders are still open for this symbol, defer targets to avoid wash-trade rejections.
        open_buys = [
            o
            for o in self.state_store.orders.values()
            if o.symbol == order.symbol and o.side == OrderSide.BUY and o.status in {OrderStatus.NEW, OrderStatus.WORKING}
        ]
        if open_buys:
            self.logger.info(
                "Deferring targets for %s; %d open buy orders remain",
                order.symbol,
                len(open_buys),
            )
        else:
            self._place_targets(position, fill.price, position.total_shares)
        self.state_store.upsert_position(position)
        self.state_store.mark_traded(position.symbol, position.last_entry_date)
        self.pnl_logger.log_entry(order.symbol, fill.timestamp, fill.price, fill.quantity, order.id)
        # If the buy was partial, place targets only for filled qty; remaining qty will trigger targets on subsequent fills.

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
        if total_shares <= 0 or position.closed:
            self.logger.debug("No targets placed for %s (shares=%s, closed=%s)", position.symbol, total_shares, position.closed)
            return
        # Skip if we already have open target orders for this symbol.
        existing_targets = [
            o for o in self.state_store.orders.values()
            if o.symbol == position.symbol
            and o.side == OrderSide.SELL
            and o.status in {OrderStatus.NEW, OrderStatus.WORKING}
        ]
        if existing_targets:
            self.logger.debug(
                "Skip placing targets for %s; %d existing sell orders already open",
                position.symbol,
                len(existing_targets),
            )
            return

        first = math.floor(total_shares * 0.25)
        second = math.floor(total_shares * 0.25)
        third = math.floor(total_shares * 0.25)
        fourth = total_shares - (first + second + third)
        targets = [
            ("target_10", round(entry_price * 1.10, 2), first),
            ("target_20", round(entry_price * 1.20, 2), second),
            ("target_50", round(entry_price * 1.50, 2), third),
            ("target_100", round(entry_price * 2.00, 2), fourth),
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
        End-of-day cleanup: cancel/close, capture fills, and clear state.
        """
        today = now(self.settings.TIMEZONE).date().isoformat()
        if self._eod_done_date == today:
            return

        if isinstance(self.broker, AlpacaBroker):
            self._run_alpaca_eod(today)
        else:
            self._run_paper_eod(today)

    def _run_paper_eod(self, today: str) -> None:
        self.logger.info("EOD liquidation started (paper broker)")
        max_invested = self._get_max_invested_snapshot()

        # Cancel open target orders
        for order in list(self.state_store.orders.values()):
            if order.side == OrderSide.SELL and order.status in {OrderStatus.NEW, OrderStatus.WORKING}:
                order.mark_status(OrderStatus.CANCELLED)
                self.state_store.upsert_order(order)

        positions = self.state_store.get_open_positions()
        # If nothing to do, skip clearing/logging.
        if not positions and not self.state_store.orders:
            self._eod_done_date = today
            return

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
        self._write_pnl_summary(today, max_invested=max_invested)

    def _run_alpaca_eod(self, today: str) -> None:
        self.logger.info("EOD liquidation started (Alpaca)")
        max_invested = self._get_max_invested_snapshot()
        close_started = now(self.settings.TIMEZONE).astimezone(dt.timezone.utc)
        deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=self.settings.EOD_POLL_TIMEOUT_SECONDS)

        try:
            self.broker.close_all_positions(cancel_orders=True)
        except Exception as exc:
            self.logger.error("Failed to initiate Alpaca close_all_positions: %s", exc)
            return

        while True:
            fills = self.broker.get_fills_since(close_started)
            new_fills = [f for f in fills if not self.state_store.is_fill_processed(f.id)]
            if new_fills:
                self._process_fills(new_fills)
                for f in new_fills:
                    self.state_store.record_processed_fill_id(f.id)

            open_positions = self.broker.list_positions()
            open_orders = self.broker.get_open_orders()

            if not open_positions and not open_orders:
                break

            if dt.datetime.now(dt.timezone.utc) >= deadline:
                self.logger.warning(
                    "EOD polling timed out with %d positions and %d open orders remaining",
                    len(open_positions),
                    len(open_orders),
                )
                # Do not clear state if not flat; leave for next sync.
                return

            time.sleep(self.settings.EOD_POLL_INTERVAL_SECONDS)

        # Mark any tracked positions as closed locally
        for pos in self.state_store.positions.values():
            pos.closed = True
            pos.open_target_orders.clear()
            pos.total_shares = 0
            self.state_store.upsert_position(pos)

        if self.settings.EOD_CLEAR_STATE:
            self.state_store.clear()
            self.logger.info("State cleared after EOD liquidation")

        self._eod_done_date = today
        # Rebuild PnL log from Alpaca fills and write summary to ensure accuracy.
        self._rebuild_pnl_log(dt.date.fromisoformat(today))
        self._write_pnl_summary(today, max_invested=max_invested)

    def _get_max_invested_snapshot(self) -> tuple[float, str | None] | None:
        val = self.state_store.metrics.get("max_invested_value")
        if val is None:
            return None
        try:
            val_f = float(val)
        except Exception:
            return None
        date_val = self.state_store.metrics.get("max_invested_date")
        return val_f, str(date_val) if date_val is not None else None

    def _write_pnl_summary(self, iso_date: str, max_invested: tuple[float, str | None] | None = None) -> None:
        """Generate PnL summary for the given date."""
        try:
            base_pnl = Path(self.settings.PNL_LOG_FILE).expanduser()
            dated_pnl = base_pnl.parent / f"pnl-{iso_date}.log"

            def _has_content(path: Path) -> bool:
                return path.exists() and path.stat().st_size > 0

            # If the dated log is missing/empty, try to rebuild it from Alpaca fills before summarizing.
            if not _has_content(dated_pnl) and isinstance(self.broker, AlpacaBroker):
                try:
                    self._rebuild_pnl_log(dt.date.fromisoformat(iso_date))
                except Exception as exc:  # pragma: no cover - best effort
                    self.logger.warning("PnL rebuild failed for %s: %s", iso_date, exc)

            candidates: list[Path] = []
            if _has_content(dated_pnl):
                candidates.append(dated_pnl)
            if _has_content(base_pnl) and base_pnl not in candidates:
                candidates.append(base_pnl)
            for p in sorted(base_pnl.parent.glob(f"pnl-{iso_date}*.log")):
                if _has_content(p) and p not in candidates:
                    candidates.append(p)

            if not candidates:
                self.logger.warning("PnL log for %s not found or empty; skipping summary generation", iso_date)
                return

            pnl_path = candidates[0]
            output, out_file = summarise_and_write(pnl_path, max_invested=max_invested)
            self.logger.info("PnL summary written to %s", out_file)
            if out_file:
                try:
                    chart_path, _, _ = generate_threshold_chart(out_file, step=0.25)
                    self.logger.info("PnL threshold chart written to %s", chart_path)
                except Exception as exc:
                    self.logger.warning("PnL threshold chart failed: %s", exc)
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.error("Failed to generate PnL summary: %s", exc)

    def _reconcile_state_with_broker(self) -> None:
        """
        On startup, pull positions/orders/fills from the broker (Alpaca) to ensure
        local state matches broker state.
        """
        if not isinstance(self.broker, AlpacaBroker):
            return
        self.logger.info("Reconciling local state with Alpaca broker...")
        # Rebuild positions/fills from today's broker fills so PnL log can be reconstructed after restarts.
        today_local = now(self.settings.TIMEZONE).date()
        tz = ZoneInfo(self.settings.TIMEZONE)
        start_local = dt.datetime.combine(today_local, dt.time(0, 0), tzinfo=tz)
        start_utc = start_local.astimezone(dt.timezone.utc)
        broker_fills = self.broker.get_fills_since(start_utc)
        broker_fills = sorted(broker_fills, key=lambda f: f.timestamp)

        # Reset local caches (keep metrics/traded_dates/processed IDs).
        self.state_store.positions.clear()
        self.state_store.orders.clear()
        self.state_store.fills.clear()

        self._reconciling = True
        # Mark fills as processed so we don't replay them later; positions are sourced from broker positions.
        for f in broker_fills:
            self.state_store.record_processed_fill_id(f.id)
            if isinstance(self.broker, AlpacaBroker):
                self.broker.processed_fills.add(f.id)
        self._reconciling = False

        # Refresh open orders from broker (for fill matching).
        open_orders = self.broker.get_open_orders()
        for o in open_orders:
            self.state_store.upsert_order(o)

        # Advance broker fill poll cursor to "now" to avoid replaying old fills after reconcile.
        if isinstance(self.broker, AlpacaBroker):
            self.broker.last_fill_poll = dt.datetime.now(dt.timezone.utc)

        # Record sync timestamp.
        self.state_store.record_sync_timestamp(dt.datetime.now(dt.timezone.utc).isoformat())
        self.logger.info(
            "Reconciliation complete: %d positions, %d open orders",
            len(self.state_store.positions),
            len(open_orders),
        )
        # Ensure PnL log is rebuilt from broker truth on startup.
        self._rebuild_pnl_log(today_local)

    def _rebuild_pnl_log(self, target_date: dt.date) -> None:
        """
        Rebuild the PnL log for the given date directly from Alpaca fills.
        """
        if not isinstance(self.broker, AlpacaBroker):
            return
        tz = ZoneInfo(self.settings.TIMEZONE)
        start_local = dt.datetime.combine(target_date, dt.time(0, 0), tzinfo=tz)
        end_local = start_local + dt.timedelta(days=1)
        start_utc = start_local.astimezone(dt.timezone.utc)
        end_utc = end_local.astimezone(dt.timezone.utc)

        fills = self.broker.get_fills_since(start_utc, include_processed=True)
        fills = [f for f in fills if f.timestamp <= end_utc]
        fills.sort(key=lambda f: f.timestamp)

        if not fills:
            return

        out_path = Path(self.settings.PNL_LOG_FILE).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        dated = out_path.parent / f"pnl-{target_date.isoformat()}.log"

        positions: Dict[str, Dict[str, float]] = {}
        with dated.open("w", encoding="utf-8") as f:
            for fill in fills:
                side = fill.side or OrderSide.BUY
                sym = fill.symbol
                qty = int(fill.quantity)
                price = float(fill.price)
                pos = positions.setdefault(sym, {"qty": 0, "avg": 0.0})

                if side == OrderSide.BUY:
                    new_qty = pos["qty"] + qty
                    new_avg = (pos["avg"] * pos["qty"] + price * qty) / new_qty if new_qty else 0.0
                    pos["qty"], pos["avg"] = new_qty, new_avg
                    f.write(
                        json.dumps(
                            {
                                "event": "entry",
                                "symbol": sym,
                                "timestamp": fill.timestamp.isoformat(),
                                "price": price,
                                "quantity": qty,
                                "order_id": fill.order_id,
                            }
                        )
                        + "\n"
                    )
                else:
                    sell_qty = min(qty, pos["qty"])
                    pnl_delta = (price - pos["avg"]) * sell_qty if pos["qty"] > 0 else 0.0
                    pos["qty"] = max(0, pos["qty"] - sell_qty)
                    if pos["qty"] == 0:
                        pos["avg"] = 0.0
                    f.write(
                        json.dumps(
                            {
                                "event": "exit_fill",
                                "symbol": sym,
                                "timestamp": fill.timestamp.isoformat(),
                                "price": price,
                                "quantity": qty,
                                "pnl_delta": pnl_delta,
                                "order_id": fill.order_id,
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
                                    "timestamp": fill.timestamp.isoformat(),
                                    "realized_pnl": pnl_delta,
                                }
                            )
                            + "\n"
                        )

        # Also write to base PnL file for consistency.
        out_path.write_text(dated.read_text(encoding="utf-8"), encoding="utf-8")
        self.logger.info("Rebuilt PnL log from Alpaca fills: %s", dated)

    def _refresh_state_from_broker(self) -> None:
        """
        Lightweight refresh of positions/open orders from Alpaca each tick to keep
        local cache aligned with broker truth.
        """
        if not isinstance(self.broker, AlpacaBroker):
            return
        try:
            positions = self.broker.list_positions()
            self.state_store.positions.clear()
            for p in positions:
                sym = p.get("symbol")
                qty = int(p.get("qty") or p.get("quantity") or p.get("size") or 0)
                avg = float(p.get("avg_entry_price") or p.get("avg_price") or 0)
                if not sym or qty <= 0:
                    continue
                pos = Position(
                    symbol=sym,
                    total_shares=qty,
                    avg_price=avg,
                    cash_invested=avg * qty,
                    realized_pnl=float(p.get("unrealized_pl") or 0.0),
                    open_target_orders=[],
                    closed=False,
                )
                pos.last_entry_date = now(self.settings.TIMEZONE).date().isoformat()
                self.state_store.upsert_position(pos)
                self.state_store.mark_traded(sym, pos.last_entry_date)

            # Do not clear orders; we need existing IDs for fill matching.
            # Instead, update/add current open orders from broker.
            open_orders = {o.id: o for o in self.broker.get_open_orders()}
            for oid, o in open_orders.items():
                self.state_store.upsert_order(o)
            # Leave existing non-open orders as-is (filled/cancelled) to allow fill matching.

            # Ensure each position has targets totaling the position size.
            self._ensure_targets_for_positions()
        except Exception as exc:
            self.logger.warning("Broker state refresh failed: %s", exc)

    def _ensure_targets_for_positions(self) -> None:
        """
        For each open position, verify that open sell orders cover the full share count.
        If mismatch, cancel existing sells and place a fresh target ladder sized to total_shares.
        """
        now_ts = dt.datetime.now(dt.timezone.utc)
        cooldown_seconds = 300  # 5 minutes
        for sym, pos in self.state_store.get_open_positions().items():
            last_attempt = self._last_backfill_attempt.get(sym)
            if last_attempt and (now_ts - last_attempt).total_seconds() < cooldown_seconds:
                continue
            open_sells = [
                o for o in self.state_store.orders.values()
                if o.symbol == sym and o.side == OrderSide.SELL and o.status in {OrderStatus.NEW, OrderStatus.WORKING}
            ]
            qty_open = sum(o.quantity for o in open_sells)
            if pos.total_shares <= 0:
                continue
            # If open sells do not cover the position, or (for larger sizes) the ladder is incomplete, reset.
            ladder_required = 4 if pos.total_shares >= 4 else 1
            if qty_open != pos.total_shares or len(open_sells) < ladder_required:
                # Cancel existing sells for this symbol.
                for o in open_sells:
                    try:
                        if hasattr(self.broker, "cancel_order"):
                            self.broker.cancel_order(o.id)
                        o.mark_status(OrderStatus.CANCELLED)
                        self.state_store.upsert_order(o)
                    except Exception as exc:
                        # If already filled, ignore; otherwise warn once.
                        if "filled" not in str(exc).lower():
                            self.logger.debug("Cancel failed for %s (%s): %s", sym, o.id, exc)

                missing = pos.total_shares
            else:
                continue

            entry_price = pos.avg_price or 0.0
            if entry_price <= 0:
                continue
            # Place targets for the missing quantity using standard splits.
            # If position is very small, place a single target_100 for all shares.
            if missing < 4:
                targets = [("target_100", round(entry_price * 2.00, 2), missing)]
            else:
                first = math.floor(missing * 0.25)
                second = math.floor(missing * 0.25)
                third = math.floor(missing * 0.25)
                fourth = missing - (first + second + third)
                targets = [
                    ("target_10", round(entry_price * 1.10, 2), first),
                    ("target_20", round(entry_price * 1.20, 2), second),
                    ("target_50", round(entry_price * 1.50, 2), third),
                    ("target_100", round(entry_price * 2.00, 2), fourth),
                ]
            for tag, price, qty in targets:
                if qty <= 0:
                    continue
                order = Order(
                    symbol=sym,
                    side=OrderSide.SELL,
                    type=OrderType.LIMIT,
                    price=price,
                    quantity=qty,
                    status=OrderStatus.NEW,
                    tags=[tag],
                )
                try:
                    placed = self.broker.place_order(order)
                except Exception as exc:
                    self.logger.warning("Backfill target placement failed for %s (%s): %s", sym, tag, exc)
                    break
                pos.open_target_orders.append(placed.id)
                self.state_store.upsert_order(placed)
                self.logger.info(
                    "Backfilled %s targets for %s: %s shares @ %.2f (missing=%s)",
                    tag,
                    sym,
                    qty,
                    price,
                    missing,
                )
            self.state_store.upsert_position(pos)
            self._last_backfill_attempt[sym] = now_ts
