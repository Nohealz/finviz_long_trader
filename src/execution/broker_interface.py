from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List

from src.brain.models import Order, Fill, Quote


class MarketDataProvider(ABC):
    @abstractmethod
    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        ...


class Broker(ABC):
    @abstractmethod
    def place_order(self, order: Order) -> Order:
        ...

    @abstractmethod
    def get_open_orders(self) -> List[Order]:
        ...

    @abstractmethod
    def simulate_minute(self, quotes: Dict[str, Quote], use_high_for_limits: bool = False) -> List[Fill]:
        """
        Advance the simulation by one 'minute' of market data, determining
        which orders are filled based on the provided quotes.
        """
        ...
