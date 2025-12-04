from src.brain.finviz_client import FinvizScreenerClient


def test_parse_symbols_basic():
    html = """
    <html>
      <body>
        <table class="screener_table">
          <tr class="styled-row">
            <td class="screener-body-table-nw"><a class="tab-link" href="quote.ashx?t=ABC">ABC</a></td>
            <td>Other column</td>
          </tr>
          <tr class="styled-row">
            <td class="screener-body-table-nw"><a class="tab-link" href="quote.ashx?t=XYZ">XYZ</a></td>
            <td>Other column</td>
          </tr>
          <tr class="styled-row">
            <td class="screener-body-table-nw"><a class="tab-link" href="quote.ashx?t=-">-</a></td>
            <td>Other column</td>
          </tr>
          <tr><td><a href="quote.ashx?t=SHOULD_NOT_APPEAR">SHOULD_NOT_APPEAR</a></td></tr>
        </table>
        <div><a href="quote.ashx?t=NOPE">NOPE</a></div>
      </body>
    </html>
    """
    client = FinvizScreenerClient(url="http://example.com")
    symbols = client.get_symbols(html=html)
    assert symbols == ["ABC", "XYZ"]
