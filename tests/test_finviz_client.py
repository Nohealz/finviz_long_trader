from src.brain.finviz_client import FinvizScreenerClient


def test_parse_symbols_basic():
    html = """
    <html>
      <body>
        <table>
          <tr><td class="screener-body-table-nw"><a class="screener-link-primary" href="quote.ashx?t=ABC">ABC</a></td></tr>
          <tr><td class="screener-body-table-nw"><a class="screener-link-primary" href="quote.ashx?t=XYZ">XYZ</a></td></tr>
        </table>
      </body>
    </html>
    """
    client = FinvizScreenerClient(url="http://example.com")
    symbols = client.get_symbols(html=html)
    assert symbols == ["ABC", "XYZ"]
