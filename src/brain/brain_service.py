from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from dotenv import load_dotenv

from src.brain.config import Settings
from src.brain.finviz_client import FinvizScreenerClient
from src.brain.scheduler import MinuteScheduler
from src.brain.state_store import JsonStateStore
from src.brain.strategy import Strategy
from src.execution.market_data_client import SyntheticMarketDataProvider
from src.execution.paper_broker import PaperBroker
from src.shared.logging_setup import configure_logging


def build_services(logger: logging.Logger | None = None) -> MinuteScheduler:
    settings = Settings()
    log_path = Path(settings.LOG_FILE)
    app_logger = logger or configure_logging(str(log_path))
    screener = FinvizScreenerClient(settings.FINVIZ_URL, cookie=settings.FINVIZ_COOKIE, logger=app_logger)
    market_data = SyntheticMarketDataProvider(logger=app_logger)
    broker = PaperBroker(market_data=market_data, logger=app_logger)
    state_store = JsonStateStore(settings.STATE_FILE, logger=app_logger)
    strategy = Strategy(settings, screener, market_data, broker, state_store, logger=app_logger)
    scheduler = MinuteScheduler(settings, tick=strategy.run_tick, logger=app_logger)
    return scheduler


async def main() -> None:
    load_dotenv()
    scheduler = build_services()
    await scheduler.start()


if __name__ == "__main__":
    asyncio.run(main())
