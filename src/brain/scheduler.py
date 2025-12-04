from __future__ import annotations

import asyncio
import logging
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
                try:
                    self.tick()
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("Tick failed: %s", exc)
            else:
                self.logger.debug("Outside trading hours; skipping tick.")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.settings.REFRESH_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue

    def stop(self) -> None:
        self._stop_event.set()
