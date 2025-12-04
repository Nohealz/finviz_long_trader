from __future__ import annotations

import datetime as dt
import uuid
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, model_validator

Symbol = str


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(str, Enum):
    NEW = "NEW"
    WORKING = "WORKING"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class Quote(BaseModel):
    symbol: Symbol
    bid: float
    ask: float
    last: float
    mid: Optional[float] = None
    timestamp: dt.datetime = Field(default_factory=dt.datetime.utcnow)

    @model_validator(mode="after")
    def compute_mid(self) -> "Quote":
        if self.mid is None:
            self.mid = (self.bid + self.ask) / 2
        return self


class Order(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: Symbol
    side: OrderSide
    type: OrderType
    price: Optional[float] = None
    quantity: int
    status: OrderStatus = OrderStatus.NEW
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    tags: List[str] = Field(default_factory=list)

    def mark_status(self, status: OrderStatus) -> None:
        self.status = status
        self.updated_at = dt.datetime.utcnow()


class Fill(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    symbol: Symbol
    quantity: int
    price: float
    timestamp: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class Position(BaseModel):
    symbol: Symbol
    total_shares: int
    avg_price: float
    cash_invested: float
    realized_pnl: float = 0.0
    open_target_orders: List[str] = Field(default_factory=list)
    closed: bool = False

    def apply_buy_fill(self, fill: Fill) -> None:
        """
        Update average price and share count after a buy fill.
        """
        total_cost = self.avg_price * self.total_shares
        total_cost += fill.price * fill.quantity
        self.total_shares += fill.quantity
        self.cash_invested += fill.price * fill.quantity
        self.avg_price = total_cost / self.total_shares if self.total_shares else 0.0

    def apply_sell_fill(self, fill: Fill) -> None:
        self.total_shares -= fill.quantity
        proceeds = fill.price * fill.quantity
        cost_basis = self.avg_price * fill.quantity
        self.realized_pnl += proceeds - cost_basis
        if self.total_shares <= 0:
            self.closed = True
            self.total_shares = 0
