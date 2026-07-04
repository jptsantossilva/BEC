# Kraken Public Data

PR 5 adds Kraken through CCXT for public spot-market data only. The adapter
supports market metadata, normalized symbols, OHLCV candles, tickers, order
books, precision, limits and public API health checks.

Kraken aliases such as `XBT/EUR` are normalized to canonical BEC symbols such
as `BTC/EUR`. Native Kraken market identifiers remain available as exchange
symbols for persistence and diagnostics.

## Safety Boundary

- Kraken API keys are not read or required.
- Private balances, order creation, order lookup and cancellation raise an
  explicit disabled-operation error.
- Selecting Kraken never enables scheduled trading jobs automatically.
- Existing Binance positions or unsettled orders continue to block switching.
- Binance remains the only exchange with live trading support.

The Trading settings page labels Kraken as public-data-only and reports public
API health. Kraken-specific backtesting isolation belongs to PR 6; private
execution belongs to the explicitly gated PR 7.

## Dependency and Validation

CCXT is pinned in `requirements.in`; `requirements.txt` is generated with
`pip-compile requirements.in --output-file requirements.txt`.

Automated tests mock CCXT HTTP behavior. A release candidate should also verify
Kraken market loading, `BTC/EUR` ticker, order book and recent OHLCV against the
public API without credentials.
