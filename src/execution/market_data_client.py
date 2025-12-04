from __future__ import annotations

import logging
import time
from typing import Dict, List

from src.brain.models import Quote
from src.execution.broker_interface import MarketDataProvider


class SyntheticMarketDataProvider(MarketDataProvider):
    """
    Deterministic synthetic quotes for local development. Prices vary slightly
    minute-by-minute using symbol-derived seeds.
    """

    def __init__(self, base_price: float = 20.0, logger: logging.Logger | None = None) -> None:
        self.base_price = base_price
        self.logger = logger or logging.getLogger(__name__)

    def _price_for_symbol(self, symbol: str) -> float:
        seed = sum(ord(c) for c in symbol)
        minute = int(time.time() // 60)
        variation = ((minute + seed) % 5 - 2) * 0.01  # ±2% band
        price = self.base_price + (seed % 10)
        return price * (1 + variation / 10)

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        quotes: Dict[str, Quote] = {}
        for symbol in symbols:
            last = self._price_for_symbol(symbol)
            bid = last * 0.999
            ask = last * 1.001
            quotes[symbol] = Quote(symbol=symbol, bid=bid, ask=ask, last=last)
        self.logger.debug("Generated %d synthetic quotes", len(quotes))
        return quotes
