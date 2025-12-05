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

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://finnhub.io/api/v1",
        logger: logging.Logger | None = None,
        delay_ms: int = 200,
        max_symbols_per_minute: int = 30,
        max_symbols_per_second: int = 5,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.logger = logger or logging.getLogger(__name__)
        self.delay_ms = delay_ms
        self.max_symbols_per_minute = max_symbols_per_minute
        self.max_symbols_per_second = max_symbols_per_second
        self._offset = 0  # rotate symbols across ticks
        self._window_start = time.time()
        self._minute_key: int | None = None
        self._used_in_window = 0
        self._warned_window_start: float | None = None
        self._recent_requests: list[float] = []

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

    def _respect_rate_limits(self, allow: int) -> int:
        now_ts = time.time()
        minute_key = int(now_ts // 60)
        # Reset window when clock minute changes, so each wall-clock minute gets a fresh budget.
        if self._minute_key != minute_key:
            self._minute_key = minute_key
            self._window_start = now_ts
            self._used_in_window = 0
            self._warned_window_start = None
        remaining_minute = max(0, self.max_symbols_per_minute - self._used_in_window)
        allowed = min(allow, remaining_minute)
        return allowed

    def _sleep_for_per_second_limit(self) -> None:
        now_ts = time.time()
        # Drop entries older than 1 second
        self._recent_requests = [t for t in self._recent_requests if now_ts - t < 1]
        if len(self._recent_requests) >= self.max_symbols_per_second:
            sleep_time = 1 - (now_ts - self._recent_requests[0])
            if sleep_time > 0:
                time.sleep(sleep_time)

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        if not symbols:
            return {}
        # Deduplicate and limit symbols per minute; rotate across ticks to cover all.
        unique_symbols = list(dict.fromkeys(symbols))
        allowed = self._respect_rate_limits(len(unique_symbols))
        if allowed <= 0:
            if self._warned_window_start != self._window_start:
                self.logger.warning(
                    "Finnhub per-minute cap reached (%d); skipping quotes for %d symbols.",
                    self.max_symbols_per_minute,
                    len(unique_symbols),
                )
                self._warned_window_start = self._window_start
            return {}

        if allowed < len(unique_symbols):
            start = self._offset % len(unique_symbols)
            end = start + allowed
            if end <= len(unique_symbols):
                selected = unique_symbols[start:end]
            else:
                selected = unique_symbols[start:] + unique_symbols[: end - len(unique_symbols)]
            self._offset = (start + allowed) % len(unique_symbols)
            self.logger.warning(
                "Finnhub symbol list truncated to %d (of %d) this tick to respect rate limits.",
                len(selected),
                len(unique_symbols),
            )
        else:
            selected = unique_symbols

        quotes: Dict[str, Quote] = {}
        for idx, sym in enumerate(selected):
            self._sleep_for_per_second_limit()
            start = time.time()
            q = self._fetch_quote(sym)
            self._used_in_window += 1
            self._recent_requests.append(start)
            if q:
                quotes[sym] = q
            # Throttle between requests to respect rate limits.
            if idx < len(selected) - 1 and self.delay_ms > 0:
                time.sleep(self.delay_ms / 1000.0)
        self.logger.debug("Fetched %d/%d quotes from Finnhub", len(quotes), len(selected))
        return quotes
