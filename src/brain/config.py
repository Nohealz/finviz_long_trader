from __future__ import annotations

import datetime as dt
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict


class Settings(BaseSettings):
    """
    Central configuration for the brains service.
    """

    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    FINVIZ_URL: str = Field(
        "https://elite.finviz.com/screener.ashx?v=111&f=sh_curvol_o1000,ta_perf_d15o&ft=4&o=-change&ar=60",
        description="Finviz Elite screener URL to poll.",
    )
    REFRESH_INTERVAL_SECONDS: int = 60
    BASE_POSITION_DOLLARS: float = 1000.0

    PREMARKET_START: dt.time = dt.time(hour=4, minute=0)
    REGULAR_OPEN: dt.time = dt.time(hour=9, minute=30)
    REGULAR_CLOSE: dt.time = dt.time(hour=16, minute=0)
    TIMEZONE: str = "America/New_York"
    ALLOW_WEEKEND_TRADING: bool = False
    BROKER_BACKEND: str = Field(default="paper", description="Which broker backend to use: paper or alpaca.")
    PREMARKET_BUY_SLIPPAGE_BPS: float = Field(
        default=30.0,
        description="Basis points added to last/ask for premarket limit buys (Alpaca requires limit in extended hours).",
        ge=0.0,
    )
    FINVIZ_REQUIRE_REFRESH_BEFORE_TRADING: bool = Field(
        default=True,
        description="If true, wait to see the screener list change (new day refresh) before placing new buys.",
    )
    FINVIZ_MIN_SYMBOLS: int = Field(
        default=5,
        description="Minimum symbols required from the screener to consider it a valid refreshed list.",
        ge=1,
    )

    STATE_FILE: Path = Path("./data/state.json")
    LOG_FILE: Path = Path("./logs/finviz_trader.log")

    FINVIZ_COOKIE: str | None = Field(
        default=None,
        description="Optional cookie string for Finviz Elite authentication.",
    )

    FINNHUB_API_KEY: str | None = Field(
        default=None,
        description="Optional Finnhub API key for real-time quotes.",
    )
    FINNHUB_REQUEST_DELAY_MS: int = Field(
        default=200,
        description="Delay between Finnhub requests in milliseconds (rate limiting).",
        ge=0,
    )
    FINNHUB_MAX_SYMBOLS_PER_MINUTE: int = Field(
        default=30,
        description="Max symbols to request per minute from Finnhub (rotated per tick).",
        ge=1,
    )
    FINNHUB_MAX_SYMBOLS_PER_SECOND: int = Field(
        default=5,
        description="Max symbols to request per second from Finnhub.",
        ge=1,
    )
    POST_BUY_FILL_POLL_SECONDS: float = Field(
        default=2.0,
        description="Seconds to wait after submitting buys before a quick fill poll to place targets immediately.",
        ge=0.0,
    )
    YFINANCE_CACHE_TTL_SECONDS: int = Field(
        default=300,
        description="How long to cache yfinance 5m bars (seconds).",
        ge=60,
    )
    PNL_LOG_FILE: Path = Path("./data/pnl.log")
    EOD_AUTO_LIQUIDATE: bool = True
    EOD_CLEAR_STATE: bool = True
    EOD_POLL_INTERVAL_SECONDS: int = Field(default=3, ge=1, description="Polling cadence (seconds) while waiting for EOD fills.")
    EOD_POLL_TIMEOUT_SECONDS: int = Field(default=180, ge=30, description="Maximum time to wait for EOD closeout to complete.")

    # Alpaca configuration
    ALPACA_API_KEY: str | None = None
    ALPACA_API_SECRET: str | None = None
    ALPACA_API_BASE_URL: str = "https://paper-api.alpaca.markets"
    ALPACA_DATA_BASE_URL: str = "https://data.alpaca.markets"
