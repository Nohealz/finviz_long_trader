from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

from src.brain.models import Quote
from src.execution.broker_interface import MarketDataProvider
import requests
try:
    import yfinance as yf  # type: ignore
except Exception:  # noqa: BLE001
    yf = None


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
        self._last_trunc_warn_minute: int | None = None
        self._recent_requests: list[float] = []

    def _fetch_quote(self, symbol: str) -> Optional[Quote]:
        url = f"{self.base_url}/quote"
        for attempt in range(3):
            try:
                resp = self.session.get(url, params={"symbol": symbol, "token": self.api_key}, timeout=10)
                if resp.status_code == 429:
                    self.logger.warning("Finnhub rate limit hit for %s; status %s", symbol, resp.status_code)
                    return None
                if resp.status_code >= 500:
                    if attempt < 2:
                        time.sleep(0.5)
                        continue
                resp.raise_for_status()
                data = resp.json()
                # Finnhub fields: c=current, h=high, l=low, o=open, pc=prev close, t=timestamp, dp/per change fields may exist
                bid = data.get("c") or 0.0
                ask = data.get("c") or 0.0
                last = data.get("c") or 0.0
                high = data.get("h")
                if last == 0:
                    self.logger.warning("Finnhub returned zero quote for %s: %s", symbol, data)
                    return None
                return Quote(symbol=symbol, bid=bid, ask=ask, last=last, high=high if high is not None else None)
            except Exception as exc:  # noqa: BLE001
                if attempt < 2:
                    time.sleep(0.5)
                    continue
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
            minute_key = int(time.time() // 60)
            if self._last_trunc_warn_minute != minute_key:
                self.logger.warning(
                    "Finnhub symbol list truncated to %d (of %d) this tick to respect rate limits.",
                    len(selected),
                    len(unique_symbols),
                )
                self._last_trunc_warn_minute = minute_key
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


class YFinanceMarketDataProvider(MarketDataProvider):
    """
    5-minute OHLC-based quotes sourced from yfinance. Caches per-symbol for a
    configurable TTL to avoid excessive network calls. Uses a custom session to
    reduce 429s by setting a browser-y User-Agent.
    """

    def __init__(self, ttl_seconds: int = 300, logger: logging.Logger | None = None) -> None:
        if yf is None:
            raise ImportError("yfinance is required for YFinanceMarketDataProvider; please pip install yfinance")
        self.logger = logger or logging.getLogger(__name__)
        self.ttl_seconds = ttl_seconds
        self._cache: dict[str, tuple[float, Quote]] = {}
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
                "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

    def _is_fresh(self, symbol: str) -> bool:
        if symbol not in self._cache:
            return False
        ts, _ = self._cache[symbol]
        return (time.time() - ts) < self.ttl_seconds

    def _fetch_symbol(self, symbol: str) -> Optional[Quote]:
        try:
            data = yf.Ticker(symbol, session=self._session)
            hist = data.history(period="1d", interval="5m")
            if hist.empty:
                return None
            last_row = hist.iloc[-1]
            high = float(last_row.get("High", 0.0))
            close = float(last_row.get("Close", 0.0))
            if close == 0.0:
                return None
            quote = Quote(symbol=symbol, bid=close, ask=close, last=close, mid=close, high=high)
            return quote
        except Exception as exc:  # noqa: BLE001
            # Suppress noisy repeats; log at debug to avoid spamming.
            self.logger.debug("yfinance quote failed for %s: %s", symbol, exc)
            return None

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        quotes: Dict[str, Quote] = {}
        unique_symbols = list(dict.fromkeys(symbols))
        for sym in unique_symbols:
            if self._is_fresh(sym):
                quotes[sym] = self._cache[sym][1]
                continue
            q = self._fetch_symbol(sym)
            if q:
                quotes[sym] = q
                self._cache[sym] = (time.time(), q)
        return quotes


class CompositeMarketDataProvider(MarketDataProvider):
    """
    Chain providers in order. First provider to return a quote wins for each symbol.
    Helpful for falling back to Finnhub when yfinance is missing data.
    """

    def __init__(self, providers: List[MarketDataProvider]) -> None:
        self.providers = providers

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        remaining = list(dict.fromkeys(symbols))
        combined: Dict[str, Quote] = {}
        for provider in self.providers:
            if not remaining:
                break
            partial = provider.get_quotes(remaining)
            combined.update(partial)
            remaining = [s for s in remaining if s not in combined]
        return combined
