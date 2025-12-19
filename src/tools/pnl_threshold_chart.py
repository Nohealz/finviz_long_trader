"""
Plot realized PnL as you progressively filter out low-priced symbols.

Usage:
  python -m src.tools.pnl_threshold_chart --file data/pnl-summary-2025-12-16.log --step 0.25 --output data/pnl-threshold-2025-12-16.png
"""

from __future__ import annotations

import argparse
import pathlib
from typing import List, Tuple

import matplotlib.pyplot as plt


def parse_summary(path: pathlib.Path) -> List[Tuple[str, float, float]]:
    """Return list of (symbol, avg_entry, realized_pnl)."""
    rows: List[Tuple[str, float, float]] = []
    for line in path.read_text().splitlines():
        if (
            not line
            or line.startswith("Summary")
            or line.startswith("Symbols")
            or line.startswith("-")
        ):
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        try:
            sym = parts[0]
            avg = float(parts[1])
            realized = float(parts[5])
        except Exception:
            continue
        rows.append((sym, avg, realized))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="PnL summary log (e.g., data/pnl-summary-2025-12-16.log)")
    parser.add_argument("--step", type=float, default=0.25, help="Increment for thresholds (default: 0.25)")
    parser.add_argument("--output", default=None, help="Output PNG path (default: alongside input with -threshold.png)")
    args = parser.parse_args()

    summary_path = pathlib.Path(args.file)
    rows = parse_summary(summary_path)
    if not rows:
        raise SystemExit("No rows parsed from summary file")

    max_price = max(r[1] for r in rows)
    thresholds = []
    pnl_values = []
    t = 0.0
    while t <= max_price + 1e-9:
        thresholds.append(round(t, 2))
        pnl_values.append(round(sum(r[2] for r in rows if r[1] >= t), 2))
        t += args.step

    if args.output:
        out_path = pathlib.Path(args.output)
    else:
        out_path = summary_path.with_name(f"{summary_path.stem}-threshold.png")

    plt.figure(figsize=(8, 4))
    plt.plot(thresholds, pnl_values, marker="o")
    plt.title(f"PnL vs. Min Entry Price ({summary_path.name})")
    plt.xlabel("Min entry price filter ($)")
    plt.ylabel("Realized PnL")
    plt.grid(True, alpha=0.3)
    plt.axhline(0, color="black", linewidth=0.8)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    print(f"Saved plot to {out_path}")
    for th, pnl in zip(thresholds, pnl_values):
        print(f"threshold >= ${th:.2f}: PnL = {pnl:.2f}")


if __name__ == "__main__":
    main()
