from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional


def now(tz: str | ZoneInfo = "America/New_York") -> dt.datetime:
    zone = ZoneInfo(tz) if isinstance(tz, str) else tz
    return dt.datetime.now(zone)


def is_within_trading_hours(
    current: Optional[dt.datetime],
    premarket_start: dt.time,
    regular_open: dt.time,
    regular_close: dt.time,
) -> bool:
    """
    Determine if current time is within premarket and regular session window.
    """
    if current is None:
        return False
    current_time = current.time()
    return premarket_start <= current_time <= regular_close
