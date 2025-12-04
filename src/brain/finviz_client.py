from __future__ import annotations

import logging
import re
from typing import List, Optional

import requests
from bs4 import BeautifulSoup


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
        anchor_pattern = re.compile(r"quote\.ashx\?t=", re.IGNORECASE)
        for anchor in soup.find_all("a", href=anchor_pattern):
            text = anchor.get_text(strip=True).upper()
            if re.fullmatch(r"[A-Z\.\-]{1,6}", text):
                symbols.add(text)
        table_cells = soup.select("td.screener-body-table-nw a.screener-link-primary")
        for anchor in table_cells:
            text = anchor.get_text(strip=True).upper()
            if re.fullmatch(r"[A-Z\.\-]{1,6}", text):
                symbols.add(text)
        parsed = sorted(symbols)
        self.logger.debug("Parsed %d symbols from screener", len(parsed))
        return parsed

    def get_symbols(self, html: Optional[str] = None) -> List[str]:
        """
        Fetch and parse the screener page. `html` can be injected for testing.
        """
        raw_html = html if html is not None else self.fetch_html()
        return self.parse_symbols(raw_html)
