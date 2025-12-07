from __future__ import annotations

import json
import logging
from pathlib import Path
from datetime import datetime, date


class PnLLogger:
    """
    Append-only JSON-lines logger for per-trade metrics.
    Each line is a JSON object with an 'event' field.
    """

    def __init__(self, path: Path, logger: logging.Logger | None = None) -> None:
        self.base_dir = Path(path).expanduser().parent
        self.base_stem = Path(path).stem or "pnl"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger or logging.getLogger(__name__)
        self._current_date: date | None = None
        self._current_path: Path | None = None

    def _path_for_today(self) -> Path:
        today = date.today()
        if self._current_date != today or self._current_path is None:
            self._current_date = today
            self._current_path = self.base_dir / f"{self.base_stem}-{today.isoformat()}.log"
        return self._current_path

    def _write(self, payload: dict) -> None:
        try:
            path = self._path_for_today()
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("PnL log write failed: %s", exc)

    def log_entry(self, symbol: str, ts: datetime, price: float, qty: int, order_id: str) -> None:
        self._write(
            {
                "event": "entry",
                "symbol": symbol,
                "timestamp": ts.isoformat(),
                "price": price,
                "quantity": qty,
                "order_id": order_id,
            }
        )

    def log_exit_fill(self, symbol: str, ts: datetime, price: float, qty: int, pnl_delta: float, order_id: str) -> None:
        self._write(
            {
                "event": "exit_fill",
                "symbol": symbol,
                "timestamp": ts.isoformat(),
                "price": price,
                "quantity": qty,
                "pnl_delta": pnl_delta,
                "order_id": order_id,
            }
        )

    def log_close_summary(self, symbol: str, ts: datetime, total_realized: float) -> None:
        self._write(
            {
                "event": "close",
                "symbol": symbol,
                "timestamp": ts.isoformat(),
                "realized_pnl": total_realized,
            }
        )
