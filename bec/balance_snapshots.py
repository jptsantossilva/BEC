"""Exchange-neutral daily spot balance snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import logging
import time
from typing import Callable

import ccxt
import pandas as pd
import requests

from bec.exchanges.base import Balance, ExchangeAdapter, MarketInfo
from bec.exchanges.registry import get_default_adapter
from bec.utils import database


logger = logging.getLogger(__name__)

USD_ANCHORS = ("USD", "USDC", "USDT")
MINIMUM_USD_VALUE = Decimal("1")
TRANSIENT_BALANCE_ERRORS = (
    ccxt.NetworkError,
    ccxt.RequestTimeout,
    ccxt.ExchangeNotAvailable,
    ccxt.DDoSProtection,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    TimeoutError,
    ConnectionError,
)


@dataclass(frozen=True)
class BalanceSnapshotResult:
    exchange_id: int
    exchange_code: str
    snapshot_date: str
    assets_received: int
    assets_stored: int
    skipped_assets: tuple[str, ...]
    total_usd: Decimal
    total_btc: Decimal


class _SnapshotPrices:
    """Resolve direct, inverse and one-bridge spot conversion prices."""

    def __init__(
        self,
        adapter: ExchangeAdapter,
        markets: Mapping[str, MarketInfo],
        *,
        preferred_quote: str,
    ) -> None:
        self.adapter = adapter
        self.preferred_quote = str(preferred_quote or "").strip().upper()
        self.markets: dict[tuple[str, str], MarketInfo] = {}
        self.tickers: dict[str, Decimal] = {}
        for market in markets.values():
            if (
                market.active
                and market.spot
                and not market.contract
                and market.market_type == "spot"
            ):
                self.markets[
                    (market.base_asset.upper(), market.quote_asset.upper())
                ] = market

    def _market_price(self, market: MarketInfo) -> Decimal:
        if market.symbol not in self.tickers:
            price = Decimal(str(self.adapter.fetch_ticker(market.symbol).last))
            if price <= 0:
                raise ValueError(f"Ticker price is not positive for {market.symbol}")
            self.tickers[market.symbol] = price
        return self.tickers[market.symbol]

    def direct_rate(self, source: str, target: str) -> Decimal | None:
        """Return units of target for one unit of source."""
        source = str(source).upper()
        target = str(target).upper()
        if source == target:
            return Decimal("1")

        direct = self.markets.get((source, target))
        if direct is not None:
            return self._market_price(direct)

        inverse = self.markets.get((target, source))
        if inverse is not None:
            return Decimal("1") / self._market_price(inverse)
        return None

    def usd_price(self, asset: str) -> Decimal | None:
        asset = str(asset).upper()
        if asset in USD_ANCHORS:
            return Decimal("1")

        anchors = tuple(
            dict.fromkeys(
                (
                    self.preferred_quote if self.preferred_quote in USD_ANCHORS else "",
                    *USD_ANCHORS,
                )
            )
        )
        for anchor in anchors:
            if not anchor:
                continue
            rate = self.direct_rate(asset, anchor)
            if rate is not None:
                return rate

        bridges = tuple(
            dict.fromkeys(
                (
                    self.preferred_quote
                    if self.preferred_quote not in USD_ANCHORS
                    else "",
                    "BTC",
                )
            )
        )
        for bridge in bridges:
            if not bridge or bridge == asset:
                continue
            first_leg = self.direct_rate(asset, bridge)
            if first_leg is None:
                continue
            for anchor in USD_ANCHORS:
                second_leg = self.direct_rate(bridge, anchor)
                if second_leg is not None:
                    return first_leg * second_leg
        return None


def _snapshot_date(value: date | datetime | str | None) -> str:
    if value is None:
        value = datetime.now(timezone.utc).date() - timedelta(days=1)
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


def _fetch_balances(
    adapter: ExchangeAdapter,
    *,
    attempts: int,
    sleeper: Callable[[float], None],
) -> Mapping[str, Balance]:
    for attempt in range(1, attempts + 1):
        try:
            balances = adapter.fetch_balance()
            if not isinstance(balances, Mapping):
                raise TypeError("Exchange returned an invalid balance collection")
            return balances
        except TRANSIENT_BALANCE_ERRORS:
            if attempt >= attempts:
                raise
            sleeper(float(2 ** (attempt - 1)))
    raise RuntimeError("Balance retrieval exhausted without a result")


def create_daily_balance_snapshot(
    *,
    exchange: dict | None = None,
    adapter: ExchangeAdapter | None = None,
    snapshot_date: date | datetime | str | None = None,
    attempts: int = 3,
    sleeper: Callable[[float], None] = time.sleep,
) -> BalanceSnapshotResult:
    """Fetch, value and atomically persist the active exchange's free balances."""
    exchange = exchange or database.get_active_exchange(required=True)
    adapter = adapter or get_default_adapter()
    exchange_id = int(exchange["id"])
    exchange_code = str(exchange["code"]).lower()
    if str(adapter.code).lower() != exchange_code:
        raise RuntimeError("Active exchange changed before balance snapshot initialization")
    if not adapter.capabilities.supports_private_balance_reads:
        raise RuntimeError(f"{adapter.name} does not support private balance reads")

    balances = _fetch_balances(
        adapter,
        attempts=max(1, int(attempts)),
        sleeper=sleeper,
    )
    nonzero_balances: dict[str, Decimal] = {}
    for key, balance in balances.items():
        if not isinstance(balance, Balance):
            raise TypeError(f"Invalid balance value for {key}")
        asset = str(balance.asset or key).strip().upper()
        free = Decimal(str(balance.free))
        if asset and free > 0:
            nonzero_balances[asset] = nonzero_balances.get(asset, Decimal("0")) + free

    snapshot_day = _snapshot_date(snapshot_date)
    if not nonzero_balances:
        return BalanceSnapshotResult(
            exchange_id,
            exchange_code,
            snapshot_day,
            0,
            0,
            (),
            Decimal("0"),
            Decimal("0"),
        )

    prices = _SnapshotPrices(
        adapter,
        adapter.load_markets(),
        preferred_quote=str(exchange.get("quote_asset") or ""),
    )
    btc_price = prices.usd_price("BTC")
    if btc_price is None or btc_price <= 0:
        raise RuntimeError(
            "BTC price in USD could not be resolved for the active exchange"
        )

    calculated: list[tuple[str, Decimal, Decimal, Decimal, Decimal]] = []
    skipped_assets: list[str] = []
    for asset, free in nonzero_balances.items():
        usd_price = prices.usd_price(asset)
        if usd_price is None:
            skipped_assets.append(asset)
            continue
        balance_usd = free * usd_price
        if balance_usd <= MINIMUM_USD_VALUE:
            continue
        balance_btc = balance_usd / btc_price
        calculated.append((asset, free, usd_price, balance_usd, balance_btc))

    total_btc = sum((row[4] for row in calculated), Decimal("0"))
    total_usd = sum((row[3] for row in calculated), Decimal("0"))
    rows = [
        [
            snapshot_day,
            asset,
            float(free),
            float(usd_price),
            float(btc_price),
            float(balance_usd),
            float(balance_btc),
            float(total_btc),
        ]
        for asset, free, usd_price, balance_usd, balance_btc in calculated
    ]
    frame = pd.DataFrame(
        rows,
        columns=(
            "Date",
            "Asset",
            "Balance",
            "USD_Price",
            "BTC_Price",
            "Balance_USD",
            "Balance_BTC",
            "Total_Balance_BTC",
        ),
    )
    if not frame.empty:
        frame.sort_values(by="Balance_USD", ascending=False, inplace=True)
        database.add_balances(frame, exchange_id=exchange_id)

    if skipped_assets:
        logger.warning(
            "[%s:%s] Balance snapshot omitted assets without a USD valuation route: %s",
            exchange_id,
            exchange_code,
            ", ".join(sorted(skipped_assets)),
        )
    return BalanceSnapshotResult(
        exchange_id,
        exchange_code,
        snapshot_day,
        len(nonzero_balances),
        len(calculated),
        tuple(sorted(skipped_assets)),
        total_usd,
        total_btc,
    )
