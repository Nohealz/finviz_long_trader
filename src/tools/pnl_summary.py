"""
Summarize a PnL JSONL log into per-symbol stats.

Usage:
  python -m src.tools.pnl_summary --file data/pnl-2025-12-08.log
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


@dataclass
class SymbolStats:
    entry_qty: int = 0
    entry_cost: float = 0.0
    exit_qty: int = 0
    realized: float = 0.0
    first_entry_ts: Optional[str] = None
    last_close_ts: Optional[str] = None

    def record_entry(self, ts: str, price: float, qty: int) -> None:
        self.entry_qty += qty
        self.entry_cost += price * qty
        if not self.first_entry_ts:
            self.first_entry_ts = ts

    def record_exit_fill(self, pnl_delta: float, qty: int) -> None:
        self.exit_qty += qty
        self.realized += pnl_delta

    def record_close(self, ts: str, realized: float) -> None:
        self.last_close_ts = ts
        self.realized += realized

    @property
    def avg_entry(self) -> float:
        return self.entry_cost / self.entry_qty if self.entry_qty else 0.0

    @property
    def net_qty(self) -> int:
        return self.entry_qty - self.exit_qty


def summarise(file_path: Path) -> Dict[str, SymbolStats]:
    stats: Dict[str, SymbolStats] = defaultdict(SymbolStats)
    with file_path.open() as fh:
        for line in fh:
            if not line.strip():
                continue
            evt = json.loads(line)
            sym = evt.get("symbol")
            if not sym:
                continue
            rec = stats[sym]
            event_type = evt.get("event")
            if event_type == "entry":
                rec.record_entry(evt["timestamp"], float(evt["price"]), int(evt["quantity"]))
            elif event_type == "exit_fill":
                rec.record_exit_fill(float(evt.get("pnl_delta", 0.0)), int(evt["quantity"]))
            elif event_type == "close":
                rec.record_close(evt["timestamp"], float(evt.get("realized_pnl", 0.0)))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize a PnL JSONL log.")
    parser.add_argument("--file", required=True, help="Path to pnl-YYYY-MM-DD.log")
    parser.add_argument(
        "--out",
        help="Optional path to write a text summary log. Default: data/pnl-summary-<date>.log next to the PnL file.",
    )
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        raise SystemExit(f"File not found: {file_path}")

    stats = summarise(file_path)
    total_realized = sum(rec.realized for rec in stats.values())
    wins = [rec.realized for rec in stats.values() if rec.realized > 0]
    losses = [rec.realized for rec in stats.values() if rec.realized < 0]
    flats = [rec.realized for rec in stats.values() if rec.realized == 0]
    win_count = len(wins)
    loss_count = len(losses)
    flat_count = len(flats)
    avg_win = sum(wins) / win_count if win_count else 0.0
    avg_loss = sum(losses) / loss_count if loss_count else 0.0

    lines = []
    lines.append(f"Summary for {file_path.name}")
    lines.append(f"Symbols: {len(stats)} | Total realized PnL: {total_realized:.2f}")
    lines.append(
        f"Wins: {win_count} | Losses: {loss_count} | Flats: {flat_count} | "
        f"Avg win: {avg_win:.2f} | Avg loss: {avg_loss:.2f}"
    )
    lines.append("-" * 110)
    lines.append(
        f"{'Symbol':<8} {'AvgEntry':>10} {'QtyIn':>8} {'QtyOut':>8} {'NetQty':>8} "
        f"{'Realized':>12} {'FirstEntry':>22} {'LastClose':>22}"
    )
    for sym, rec in sorted(stats.items()):
        lines.append(
            f"{sym:<8} {rec.avg_entry:>10.4f} {rec.entry_qty:>8} {rec.exit_qty:>8} {rec.net_qty:>8} "
            f"{rec.realized:>12.2f} {rec.first_entry_ts or '-':>22} {rec.last_close_ts or '-':>22}"
        )

    output = "\n".join(lines)
    print(output)

    # Write to a dated summary log if requested or by default.
    out_path: Path
    if args.out:
        out_path = Path(args.out)
    else:
        out_dir = file_path.parent
        out_name = f"pnl-summary-{file_path.stem.replace('pnl-', '')}.log"
        out_path = out_dir / out_name
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(output + "\n")


if __name__ == "__main__":
    main()
