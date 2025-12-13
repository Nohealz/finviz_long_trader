Finviz Long Trader
==================

A two-part trading scaffold for a Finviz Elite screener strategy with a brains service (signal + state + orchestration) and an execution service (paper broker now, real broker later).

Features
--------
- Minute-level polling of a configurable Finviz Elite screener.
- Strategy: buy $1,000 per symbol (configurable), create 4 staged limit-sell targets (+10%, +20%, +50%, +100%).
- Clean separation between brain (what to trade) and execution (how to trade).
- Pluggable Finviz client, market data provider, and broker interface.
- Paper broker simulation with deterministic synthetic quotes for local development.

Getting Started
---------------
1) Python 3.11+ and `pip` available.
2) Create a virtual environment:
```
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```
3) Copy `.env.example` to `.env` and adjust values if needed (e.g., Finviz cookies, state file path).
4) Run the brains service (includes scheduler):
```
python -m src.brain.brain_service
```
5) Broker backend selection:
   - Default: in-memory paper broker (no external calls).
   - Alpaca paper: set `BROKER_BACKEND=alpaca` and provide `ALPACA_API_KEY`, `ALPACA_API_SECRET`. Optional overrides: `ALPACA_API_BASE_URL` (paper endpoint by default) and `ALPACA_DATA_BASE_URL`. Extended-hours flag is enabled on all orders.
5) Run tests:
```
pytest
```

Architecture
------------
- `src/brain`: strategy, state, Finviz client, scheduling, orchestration.
- `src/execution`: broker abstraction and paper implementation.
- `src/shared`: logging setup, time utilities.
- `tests`: basic coverage for strategy, broker, and Finviz parsing.

Notes & Assumptions
-------------------
- Finviz Elite may require authentication; provide cookies via environment variables if needed.
- Paper fills:
  - Market buys fill on the next `simulate_minute` call at `last * 1.001` to approximate the bar high.
  - Limit sells fill when the minute mid-price meets/exceeds the limit.
- State persistence uses a JSON file for v1 and can be swapped for a database later.

Next Steps
----------
- Wire a real market data feed and broker implementation.
- Harden Finviz scraping (or replace with an API).
- Add richer risk management, stops, and intraday resets.
