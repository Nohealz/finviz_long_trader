from pathlib import Path

from src.brain.config import Settings
from src.brain.models import OrderSide, OrderStatus, Quote
from src.brain.state_store import JsonStateStore
from src.brain.strategy import Strategy
from src.execution.broker_interface import MarketDataProvider
from src.execution.paper_broker import PaperBroker


class StubScreener:
    def __init__(self, symbols):
        self.symbols = symbols

    def get_symbols(self):
        return self.symbols


class StubMarketData(MarketDataProvider):
    def __init__(self, price_map):
        self.price_map = price_map

    def get_quotes(self, symbols):
        quotes = {}
        for sym in symbols:
            price = self.price_map.get(sym, 10.0)
            quotes[sym] = Quote(symbol=sym, bid=price * 0.99, ask=price * 1.01, last=price)
        return quotes


def test_strategy_places_buys_and_targets(tmp_path):
    state_file = Path(tmp_path) / "state.json"
    settings = Settings(STATE_FILE=state_file, BASE_POSITION_DOLLARS=1000.0)
    screener = StubScreener(["ABC"])
    market_data = StubMarketData({"ABC": 50.0})
    broker = PaperBroker(market_data=market_data)
    store = JsonStateStore(state_file)
    strategy = Strategy(settings, screener, market_data, market_data, broker, store)

    strategy.run_tick()

    assert "ABC" in store.positions
    position = store.positions["ABC"]
    assert position.total_shares == 20  # ceil(1000 / 50)

    entry_orders = [o for o in store.orders.values() if o.side == OrderSide.BUY]
    assert len(entry_orders) == 1
    assert entry_orders[0].status == OrderStatus.FILLED

    sell_orders = [o for o in store.orders.values() if o.side == OrderSide.SELL]
    assert len(sell_orders) == 4
    assert sum(o.quantity for o in sell_orders) == position.total_shares
    assert set(position.open_target_orders) == {o.id for o in sell_orders}
