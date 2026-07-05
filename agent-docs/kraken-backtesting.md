# Kraken-Specific Backtesting

PR 6 enables Kraken backtesting and Monte Carlo analysis using the public CCXT
market-data adapter. Private balances and order execution remain disabled.

## Configuration

Exchange configuration is managed in the Exchange Configuration table under
Trading Settings. The spot taker fee and quote asset are editable there; the
fee is read-only in Backtesting Settings. Kraken starts at 0.4% and USDC, while
Binance retains its existing 0.06% fee. A zero fee is allowed but produces an
operator warning.

Kraken is enabled and selected by default only on new installations. Upgrades
preserve the existing active exchange. Operators may disable every exchange,
which leaves exchange-dependent jobs disabled. Quote changes require a
successful public market load and must select an active Kraken spot quote.

The configured fee is captured when a job is queued. Each subprocess receives
the exchange id, exchange code, fee and work fingerprint. Execution stops if
the active exchange, configured fee or strategy/settings fingerprint changed
after the job was queued; requeue the job to use the new context.

## Isolation Guarantees

- Results, trades, jobs and Monte Carlo records remain scoped by `Exchange_Id`.
- Work fingerprints include exchange identity, fee, strategy definition, risk
  configuration and applicable backtesting settings.
- Trading approval requires the stored exchange result, commission and work
  fingerprint to match the current context.
- Binance results cannot approve Kraken candidates or future Kraken entries.
- HTML, CSV, JSON and Monte Carlo output filenames include the exchange code.

Normalized symbols such as `BTC/USDC` are used by candidate selection and
backtesting. The Kraken adapter resolves aliases such as `XBT/USDC` and
persists the native exchange symbol separately.

## Validation and Rollout

Automated tests must mock exchange HTTP calls. Before production rollout, run
the version 4 and version 5 migrations against a current database copy and
verify that the existing Binance fee and active exchange were preserved.
Validate Kraken backtests for BTC/USDC, ETH/USDC, XRP/USDC and HYPE/USDC on 1d,
4h and 1h for all selected main strategies; unavailable pairs must be skipped
and reported. Review the exchange, fee, fingerprint, approval, report files and
stored trades before using those results in later execution work.

PR 6 does not authorize Kraken API credentials or real orders. Those remain
part of the separately gated PR 7.
