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

    async def start(self) -> None:
        self.logger.info("Starting minute scheduler (interval=%ss)", self.settings.REFRESH_INTERVAL_SECONDS)
        while not self._stop_event.is_set():
            current = now(self.settings.TIMEZONE)
            if is_within_trading_hours(
                current,
                self.settings.PREMARKET_START,
                self.settings.REGULAR_OPEN,
                self.settings.REGULAR_CLOSE,
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
                    self.settings.PREMARKET_START,
                    self.settings.REGULAR_CLOSE,
                    self.settings.TIMEZONE,
                )
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
