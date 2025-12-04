from src.brain.models import Order, OrderSide, OrderType, Quote
from src.execution.market_data_client import SyntheticMarketDataProvider
from src.execution.paper_broker import PaperBroker


def test_limit_sell_fills_when_mid_crosses():
    market_data = SyntheticMarketDataProvider()
    broker = PaperBroker(market_data=market_data)
    order = Order(symbol="ABC", side=OrderSide.SELL, type=OrderType.LIMIT, price=10.0, quantity=10)
    broker.place_order(order)
    quote = Quote(symbol="ABC", bid=10.5, ask=11.5, last=11.0)  # mid = 11.0
    fills = broker.simulate_minute({"ABC": quote})
    assert len(fills) == 1
    assert fills[0].order_id == order.id
    assert fills[0].price == 10.0
    assert broker.get_open_orders() == []
