from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

from src.brain.models import Quote
from src.execution.broker_interface import MarketDataProvider
import requests


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


class FinnhubMarketDataProvider(MarketDataProvider):
    """
    Finnhub real-time quote provider. Uses the /quote endpoint per symbol.
    """

    def __init__(self, api_key: str, base_url: str = "https://finnhub.io/api/v1", logger: logging.Logger | None = None) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.logger = logger or logging.getLogger(__name__)

    def _fetch_quote(self, symbol: str) -> Optional[Quote]:
        url = f"{self.base_url}/quote"
        try:
            resp = self.session.get(url, params={"symbol": symbol, "token": self.api_key}, timeout=10)
            if resp.status_code == 429:
                self.logger.warning("Finnhub rate limit hit for %s; status %s", symbol, resp.status_code)
                return None
            resp.raise_for_status()
            data = resp.json()
            # Finnhub fields: c=current, h=high, l=low, o=open, pc=prev close, t=timestamp, dp/per change fields may exist
            bid = data.get("c") or 0.0
            ask = data.get("c") or 0.0
            last = data.get("c") or 0.0
            if last == 0:
                self.logger.warning("Finnhub returned zero quote for %s: %s", symbol, data)
                return None
            return Quote(symbol=symbol, bid=bid, ask=ask, last=last)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Finnhub quote failed for %s: %s", symbol, exc)
            return None

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        quotes: Dict[str, Quote] = {}
        for idx, sym in enumerate(symbols):
            q = self._fetch_quote(sym)
            if q:
                quotes[sym] = q
            # Throttle between requests to respect rate limits.
            if idx < len(symbols) - 1:
                time.sleep(0.2)
        self.logger.debug("Fetched %d/%d quotes from Finnhub", len(quotes), len(symbols))
        return quotes
