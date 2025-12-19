from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional
import logging

from .models import Fill, Order, OrderStatus, Position


class JsonStateStore:
    """
    Lightweight JSON-backed persistence layer.
    """

    def __init__(self, path: Path, logger: Optional[logging.Logger] = None) -> None:
        self.path = path
        self.logger = logger or logging.getLogger(__name__)
        self.positions: Dict[str, Position] = {}
        self.orders: Dict[str, Order] = {}
        self.fills: Dict[str, Fill] = {}
        # Map of symbol -> last traded ISO date to enforce per-day buy limits.
        self.traded_dates: Dict[str, str] = {}
        # Track processed fill IDs (e.g., from Alpaca activities) to avoid double counting across restarts.
        self.processed_fill_ids: set[str] = set()
        # Simple metrics store (e.g., max holdings value per day).
        self.metrics: Dict[str, object] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self.logger.info("State file not found, starting fresh: %s", self.path)
            self._persist()  # create an empty state file so downstream code can read it
            return
        data = json.loads(self.path.read_text())
        # Positions/orders/fills are always sourced from the broker; we do not reload them from disk.
        self.positions = {}
        self.orders = {}
        self.fills = {}
        self.traded_dates = data.get("traded_dates", {})
        self.processed_fill_ids = set(data.get("processed_fill_ids", []))
        self.metrics = data.get("metrics", {}) or {}
        self.logger.info("Loaded state (positions/orders pulled from broker at runtime)")

    def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "traded_dates": self.traded_dates,
            "processed_fill_ids": sorted(self.processed_fill_ids),
            "metrics": self.metrics,
        }
        self.path.write_text(json.dumps(payload, indent=2))

    def get_open_positions(self) -> Dict[str, Position]:
        return {sym: pos for sym, pos in self.positions.items() if not pos.closed}

    def upsert_position(self, position: Position) -> None:
        self.positions[position.symbol] = position
        self._persist()

    def upsert_order(self, order: Order) -> None:
        self.orders[order.id] = order
        self._persist()

    def record_fill(self, fill: Fill) -> None:
        self.fills[fill.id] = fill
        self._persist()

    def get_orders_by_status(self, status: OrderStatus) -> List[Order]:
        return [order for order in self.orders.values() if order.status == status]

    def get_order(self, order_id: str) -> Optional[Order]:
        return self.orders.get(order_id)

    def clear(self) -> None:
        self.positions.clear()
        self.orders.clear()
        self.fills.clear()
        self.traded_dates.clear()
        self.processed_fill_ids.clear()
        self.metrics.clear()
        self._persist()

    def mark_traded(self, symbol: str, iso_date: str) -> None:
        """Remember that a symbol traded on a given date (used to block re-entries)."""
        self.traded_dates[symbol] = iso_date
        self._persist()

    def traded_on_date(self, symbol: str, iso_date: str) -> bool:
        return self.traded_dates.get(symbol) == iso_date

    def record_processed_fill_id(self, fill_id: str) -> None:
        self.processed_fill_ids.add(fill_id)
        self._persist()

    def is_fill_processed(self, fill_id: str) -> bool:
        return fill_id in self.processed_fill_ids

    def record_holdings_value(self, value: float, iso_date: str) -> float:
        """
        Track max holdings value per day. Returns the current max for the day.
        """
        max_date = self.metrics.get("max_holdings_date")
        if max_date != iso_date:
            # New day: reset max.
            self.metrics["max_holdings_value"] = 0.0
            self.metrics["max_holdings_date"] = iso_date
        current_max = float(self.metrics.get("max_holdings_value", 0.0) or 0.0)
        if value > current_max:
            self.metrics["max_holdings_value"] = value
            self.metrics["max_holdings_date"] = iso_date
            self._persist()
            return value
        return current_max

    def record_invested_value(self, value: float, iso_date: str) -> tuple[float, bool]:
        """
        Track max invested capital (cost basis of open positions) per day.
        Returns (current_max, updated_flag).
        """
        max_date = self.metrics.get("max_invested_date")
        if max_date != iso_date:
            self.metrics["max_invested_value"] = 0.0
            self.metrics["max_invested_date"] = iso_date
        current_max = float(self.metrics.get("max_invested_value", 0.0) or 0.0)
        updated = False
        if value > current_max:
            self.metrics["max_invested_value"] = value
            self.metrics["max_invested_date"] = iso_date
            self._persist()
            return value, True
        return current_max, updated

    def get_last_sync_timestamp(self) -> str | None:
        """Return the last broker sync timestamp (ISO) if set."""
        return self.metrics.get("alpaca_last_sync")  # type: ignore[return-value]

    def record_sync_timestamp(self, iso_ts: str) -> None:
        """Record the last broker sync timestamp (ISO)."""
        self.metrics["alpaca_last_sync"] = iso_ts
        self._persist()

    def get_metric(self, key: str, default=None):
        return self.metrics.get(key, default)

    def set_metric(self, key: str, value) -> None:
        self.metrics[key] = value
        self._persist()
