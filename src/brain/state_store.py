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
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self.logger.info("State file not found, starting fresh: %s", self.path)
            self._persist()  # create an empty state file so downstream code can read it
            return
        data = json.loads(self.path.read_text())
        self.positions = {sym: Position.model_validate(pos) for sym, pos in data.get("positions", {}).items()}
        self.orders = {oid: Order.model_validate(ord_) for oid, ord_ in data.get("orders", {}).items()}
        self.fills = {fid: Fill.model_validate(fill) for fid, fill in data.get("fills", {}).items()}
        self.logger.info(
            "Loaded state: %d positions, %d orders, %d fills",
            len(self.positions),
            len(self.orders),
            len(self.fills),
        )

    def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "positions": {sym: pos.model_dump(mode="json") for sym, pos in self.positions.items()},
            "orders": {oid: order.model_dump(mode="json") for oid, order in self.orders.items()},
            "fills": {fid: fill.model_dump(mode="json") for fid, fill in self.fills.items()},
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
        self._persist()
