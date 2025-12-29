from __future__ import annotations

import asyncio
import logging
import datetime as dt
from typing import Callable

from .config import Settings
from ..shared.time_utils import is_within_trading_hours, now


class MinuteScheduler:
    """
    Simple asyncio-based minute scheduler that respects configured trading hours.
    """

    def __init__(self, settings: Settings, tick: Callable[[], None], logger: logging.Logger | None = None) -> None:
        self.settings = settings
        self.tick = tick
        self.logger = logger or logging.getLogger(__name__)
        self._stop_event = asyncio.Event()
        self._eod_callback: Callable[[], bool] | None = None
        self._eod_done_date: str | None = None
        self._market_hours_provider: Callable[[dt.datetime], tuple[dt.time, dt.time, dt.time]] | None = None

    def set_eod_callback(self, fn: Callable[[], bool]) -> None:
        self._eod_callback = fn

    def set_market_hours_provider(
        self, fn: Callable[[dt.datetime], tuple[dt.time, dt.time, dt.time]]
    ) -> None:
        """
        Optional provider to override premarket/open/close times (e.g., early close days).
        """
        self._market_hours_provider = fn

    async def start(self) -> None:
        self.logger.info("Starting minute scheduler (interval=%ss)", self.settings.REFRESH_INTERVAL_SECONDS)
        while not self._stop_event.is_set():
            current = now(self.settings.TIMEZONE)
            if self._market_hours_provider:
                premarket_start, regular_open, regular_close = self._market_hours_provider(current)
            else:
                premarket_start = self.settings.PREMARKET_START
                regular_open = self.settings.REGULAR_OPEN
                regular_close = self.settings.REGULAR_CLOSE
            if is_within_trading_hours(
                current,
                premarket_start,
                regular_open,
                regular_close,
                allow_weekends=self.settings.ALLOW_WEEKEND_TRADING,
            ):
                self.logger.info("Tick at %s", current)
                try:
                    self.tick()
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("Tick failed: %s", exc)
            else:
                self.logger.info(
                    "Outside trading hours; skipping tick at %s (window %s-%s %s)",
                    current,
                    premarket_start,
                    regular_close,
                    self.settings.TIMEZONE,
                )
                # If we are past the regular close and haven't run EOD yet, run it once here.
                if (
                    self._eod_callback
                    and current.time() >= regular_close
                    and self._eod_done_date != current.date().isoformat()
                    and (self.settings.ALLOW_WEEKEND_TRADING or current.weekday() < 5)
                ):
                    try:
                        eod_complete = self._eod_callback()
                        if eod_complete:
                            self._eod_done_date = current.date().isoformat()
                        else:
                            self.logger.warning("EOD liquidation incomplete; will retry")
                    except Exception as exc:  # noqa: BLE001
                        self.logger.exception("EOD callback failed: %s", exc)
            # Align to the next wall-clock minute boundary in the configured timezone.
            current = now(self.settings.TIMEZONE)
            next_minute = (current.replace(second=0, microsecond=0) + dt.timedelta(minutes=1))
            sleep_seconds = max(1.0, (next_minute - current).total_seconds())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_seconds)
            except asyncio.TimeoutError:
                continue

    def stop(self) -> None:
        self._stop_event.set()
