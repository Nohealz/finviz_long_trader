from __future__ import annotations

import datetime as dt
from pathlib import Path
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """
    Central configuration for the brains service.
    """

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

    STATE_FILE: Path = Path("./data/state.json")
    LOG_FILE: Path = Path("./logs/finviz_trader.log")

    FINVIZ_COOKIE: str | None = Field(
        default=None,
        description="Optional cookie string for Finviz Elite authentication.",
    )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
