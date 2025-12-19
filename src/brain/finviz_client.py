from __future__ import annotations

import logging
import re
from typing import List, Optional

import requests
from bs4 import BeautifulSoup


_SYMBOL_PATTERN = re.compile(r"^[A-Z](?:[A-Z0-9]{0,4})(?:[.-][A-Z0-9]{1,2})?$")
_ANCHOR_PATTERN = re.compile(r"quote\.ashx\?t=", re.IGNORECASE)


def _is_valid_symbol(text: str) -> bool:
    """
    Ensure the parsed text looks like a real ticker (reject stray '-' rows, etc.).
    """
    return bool(_SYMBOL_PATTERN.fullmatch(text))


class FinvizScreenerClient:
    """
    Lightweight Finviz Elite screener client. All scraping logic is contained here
    so it can be swapped for an API-based implementation later.
    """

    def __init__(self, url: str, cookie: Optional[str] = None, logger: Optional[logging.Logger] = None) -> None:
        self.url = url
        self.cookie = cookie
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()

    def fetch_html(self) -> str:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; finviz-trader/0.1)",
        }
        if self.cookie:
            headers["Cookie"] = self.cookie
        response = self.session.get(self.url, headers=headers, timeout=15)
        response.raise_for_status()
        self.logger.debug("Fetched screener HTML (%s bytes)", len(response.text))
        return response.text

    def parse_symbols(self, html: str) -> List[str]:
        soup = BeautifulSoup(html, "lxml")
        symbols = set()

        # Primary: pull only the ticker cell anchors (tab-link) from the screener grid.
        grid_rows = soup.select("table.screener_table tr.styled-row, table.screener-view-table tr.styled-row")
        for row in grid_rows:
            ticker_cell = row.select_one("a.tab-link")
            if ticker_cell:
                text = ticker_cell.get_text(strip=True).upper()
                if _is_valid_symbol(text):
                    symbols.add(text)

        # Fallback: if none found (HTML variant), try anchors inside screener tables but still require validity.
        if not symbols:
            screener_tables = soup.select("table.screener-view-table, table.screener-table")
            for table in screener_tables:
                for anchor in table.find_all("a", href=_ANCHOR_PATTERN):
                    text = anchor.get_text(strip=True).upper()
                    if _is_valid_symbol(text):
                        symbols.add(text)

        parsed = sorted(symbols)
        if not parsed:
            self.logger.warning("Parsed 0 symbols from screener HTML")
        else:
            self.logger.debug("Parsed %d symbols from screener", len(parsed))
        return parsed

    def parse_symbols_with_prices(self, html: str) -> List[tuple[str, float | None]]:
        """
        Parse tickers with their associated price column from the screener grid.
        Returns list of (symbol, price or None).
        """
        soup = BeautifulSoup(html, "lxml")
        results: List[tuple[str, float | None]] = []

        # Finviz screener grid rows
        grid_rows = soup.select("table.screener_table tr.styled-row, table.screener-view-table tr.styled-row")
        for row in grid_rows:
            ticker_cell = row.select_one("a.tab-link")
            if not ticker_cell:
                continue
            sym = ticker_cell.get_text(strip=True).upper()
            if not _is_valid_symbol(sym):
                continue
            price_val: float | None = None

            def _try_parse(txt: str) -> float | None:
                txt = txt.strip().replace("$", "").replace(",", "")
                # Skip percentages or values with letters (M/B/K etc.)
                if not txt or any(ch.isalpha() for ch in txt) or "%" in txt:
                    return None
                try:
                    return float(txt)
                except Exception:
                    return None

            # First, try to parse a price from the same cell as the ticker (some variants include it).
            same_td = ticker_cell.find_parent("td")
            if same_td:
                for span in same_td.find_all("span"):
                    candidate = _try_parse(span.get_text())
                    if candidate is not None:
                        price_val = candidate
                        break
                if price_val is None:
                    candidate = _try_parse(same_td.get_text())
                    if candidate is not None:
                        price_val = candidate

            # If not found, scan cells from right to left to pick the price column (just before change/volume).
            if price_val is None:
                cells = row.find_all("td")
                for td in reversed(cells):
                    txt = td.get_text()
                    if "," in txt or "%" in txt:
                        continue  # skip volume/change
                    candidate = _try_parse(txt)
                    if candidate is not None:
                        price_val = candidate
                        break

            if price_val is None:
                self.logger.warning("No price parsed for %s; skipping symbol", sym)
                continue
            results.append((sym, price_val))

        if not results:
            self.logger.warning("Parsed 0 symbols with prices from screener HTML")
        return results

    def get_symbols(self, html: Optional[str] = None) -> List[str]:
        """
        Fetch and parse the screener page. `html` can be injected for testing.
        """
        raw_html = html if html is not None else self.fetch_html()
        return self.parse_symbols(raw_html)

    def get_symbols_with_prices(self, html: Optional[str] = None) -> List[tuple[str, float | None]]:
        raw_html = html if html is not None else self.fetch_html()
        return self.parse_symbols_with_prices(raw_html)
