import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional


def configure_logging(log_path: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """
    Configure application-wide logging with a daily rotating file handler and console output.
    """
    logger = logging.getLogger("finviz_trader")
    if logger.handlers:
        return logger  # Already configured

    log_dir = Path(log_path).expanduser().parent if log_path else Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    file_path = Path(log_path) if log_path else log_dir / "finviz_trader.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotate daily, keep all history (backupCount=0 means no deletion)
    file_handler = TimedRotatingFileHandler(file_path, when="midnight", interval=1, backupCount=0, utc=False)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    logger.setLevel(level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    logger.debug("Logging configured")
    return logger
