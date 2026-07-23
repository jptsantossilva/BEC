# Kraken Public Data

PR 5 adds Kraken through CCXT for public spot-market data only. The adapter
supports market metadata, normalized symbols, OHLCV candles, tickers, order
books, precision, limits and public API health checks.

Kraken aliases such as `XBT/USDC` are normalized to canonical BEC symbols such
as `BTC/USDC`. Native Kraken market identifiers remain available as exchange
symbols for persistence and diagnostics.

## Safety Boundary

This was the enforced boundary through PR 6. PR 7 private behavior is described
in `agent-docs/kraken-live-execution.md` and remains disabled by default.

- Kraken API keys are not read or required.
- Private balances, order creation, order lookup and cancellation raise an
  explicit disabled-operation error.
- Selecting Kraken enables only the public market-phase analysis schedule;
  live 1d/4h/1h trading schedules remain disabled.
- Existing Binance positions or unsettled orders continue to block switching.
- Binance remains the only exchange with live trading support.

The Trading settings page checks public API health only when requested by the
operator. Kraken-specific backtesting isolation belongs to PR 6.

## Dependency and Validation

CCXT is pinned in `requirements.in`; `requirements.txt` is generated with
`pip-compile requirements.in --output-file requirements.txt`.

Automated tests mock CCXT HTTP behavior. A release candidate should also verify
Kraken market loading, `BTC/USDC` ticker, order book and recent OHLCV against the
public API without credentials.

## Public Rate-Limit Resilience

Kraken public reads are serialized across dashboard, jobs-runner and manual
processes through a shared lock under `static/backtest_results/rate_limits/`.
Normal requests are spaced by at least 1.1 seconds. The directory is backed by
the shared persistence volume in Docker, so coordination also applies across
the dashboard and jobs-runner containers.

Read-only CCXT failures classified as rate limiting, DDoS protection, request
timeout or temporary exchange unavailability are retried with exponential
backoff, a 60-second cap and jitter. OHLCV calls accept `max_retries`,
`backoff_sec` and `max_backoff_sec` overrides; the defaults are seven retries,
two seconds initial backoff and a 60-second cap.

After retry exhaustion, market-phase processing stops without replacing a
previous backtest result, revoking its approval, or marking remaining symbols
complete. Rerunning resumes through the normal current-result cache. Invalid
symbols, authentication failures and other non-transient exchange errors are
not retried.
