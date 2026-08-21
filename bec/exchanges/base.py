"""Canonical exchange contracts used by BEC application code."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
import re
from typing import Any, Mapping, Sequence

import pandas as pd


class OrderStatus(str, Enum):
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ExchangeCapabilities:
    """Static exchange behavior used by application-level routing.

    Credentials and operator settings remain runtime gates. These flags only
    describe which workflows an adapter can support when those gates are met.
    """

    supports_public_market_data: bool = True
    supports_private_balance_reads: bool = False
    supports_backtesting: bool = False
    supports_live_trading: bool = False
    requires_explicit_live_flags: bool = True
    supports_signal_schedules: bool = False
    supports_reconciliation: bool = False
    uses_native_private_workflows: bool = False
    uses_gated_live_execution: bool = False
    uses_exchange_symbols_for_legacy_workflows: bool = False


@dataclass(frozen=True)
class MarketInfo:
    symbol: str
    exchange_symbol: str
    base_asset: str
    quote_asset: str
    active: bool
    amount_step: Decimal | None = None
    price_step: Decimal | None = None
    min_amount: Decimal | None = None
    max_amount: Decimal | None = None
    min_cost: Decimal | None = None
    max_cost: Decimal | None = None
    quote_market_buy_allowed: bool = False
    market_type: str = "spot"
    spot: bool = True
    contract: bool = False
    contract_size: Decimal | None = None
    linear: bool = False
    inverse: bool = False
    settle_asset: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


@dataclass(frozen=True)
class SpotMarketAvailability:
    """Read-only native spot-market state used by operator diagnostics."""

    symbol: str
    base_asset: str
    quote_asset: str
    active: bool
    status: str


def native_market_status(raw: Mapping[str, Any], *, active: bool) -> str:
    """Return a compact exchange-native market status for diagnostics."""
    info = raw.get("info")
    sources = (info, raw) if isinstance(info, Mapping) else (raw,)
    for source in sources:
        for key in ("status", "state"):
            value = str(source.get(key) or "").strip().lower()
            if value:
                return re.sub(r"[\s-]+", "_", value)
    return "online" if active else "inactive"


@dataclass(frozen=True)
class Balance:
    asset: str
    free: Decimal
    locked: Decimal = Decimal("0")

    @property
    def total(self) -> Decimal:
        return self.free + self.locked


@dataclass(frozen=True)
class Ticker:
    symbol: str
    last: Decimal
    bid: Decimal | None = None
    ask: Decimal | None = None
    timestamp: datetime | None = None


@dataclass(frozen=True)
class OrderBook:
    symbol: str
    bids: Sequence[tuple[Decimal, Decimal]]
    asks: Sequence[tuple[Decimal, Decimal]]
    timestamp: datetime | None = None


@dataclass(frozen=True)
class OrderRequest:
    symbol: str
    side: str
    amount: Decimal | None = None
    quote_amount: Decimal | None = None
    price: Decimal | None = None
    client_order_id: str | None = None


@dataclass(frozen=True)
class OrderValidation:
    valid: bool
    errors: tuple[str, ...] = ()
    normalized_amount: Decimal | None = None
    normalized_price: Decimal | None = None
    estimated_cost: Decimal | None = None


@dataclass(frozen=True)
class OrderFill:
    price: Decimal
    quantity: Decimal
    fee_asset: str | None = None
    fee_amount: Decimal = Decimal("0")
    trade_id: str | None = None
    timestamp: datetime | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


@dataclass(frozen=True)
class OrderResult:
    exchange_order_id: str
    symbol: str
    exchange_symbol: str
    side: str
    status: OrderStatus
    requested_quantity: Decimal | None
    executed_quantity: Decimal
    average_price: Decimal | None
    fills: tuple[OrderFill, ...] = ()
    client_order_id: str | None = None
    timestamp: datetime | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


@dataclass(frozen=True)
class ExchangeHealth:
    available: bool
    message: str
    checked_at: datetime


class ExchangeAdapter(ABC):
    """Infrastructure-only contract for spot exchange operations."""

    code: str
    name: str
    capabilities = ExchangeCapabilities()

    def validate_client_order_id(self, client_order_id: str) -> str:
        """Validate an idempotency key before it is persisted or submitted.

        Concrete adapters can narrow these generic constraints to their native
        exchange format. The default accepts BEC-generated identifiers while
        rejecting whitespace and control characters in durable identifiers.
        """
        value = str(client_order_id or "").strip()
        if not value or len(value) > 64 or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
            raise ValueError(
                "Client order ID must be 1-64 ASCII letters, digits, '-' or '_'"
            )
        return value

    def is_known_submission_rejection(self, exc: Exception) -> bool:
        """Whether an exception proves an order was never submitted."""
        return isinstance(exc, ValueError)

    def is_known_persisted_submission_rejection(self, error: str) -> bool:
        """Whether a stored submission error proves no order was accepted.

        This narrowly scoped reconciliation hook lets an adapter settle a
        previously unknown intent after support for a definitive native error
        is added. Transport and ambiguous exchange errors must remain unknown.
        """
        return False

    def check_reconciliation_access(self) -> None:
        """Verify read-only access required to resolve submitted orders.

        Adapters without a reconciliation workflow report the capability as
        unsupported without performing any network operation.
        """
        raise NotImplementedError(
            f"{self.name} does not support read-only order reconciliation checks"
        )

    @abstractmethod
    def load_markets(self, *, force: bool = False) -> Mapping[str, MarketInfo]:
        raise NotImplementedError

    def load_spot_market_availability(
        self, *, force: bool = False
    ) -> Sequence[SpotMarketAvailability]:
        """Return all known spot markets, including inactive native states."""
        result = []
        for market in self.load_markets(force=force).values():
            if not market.spot or market.contract or market.market_type != "spot":
                continue
            result.append(
                SpotMarketAvailability(
                    symbol=market.symbol,
                    base_asset=market.base_asset,
                    quote_asset=market.quote_asset,
                    active=bool(market.active),
                    status=native_market_status(
                        market.raw,
                        active=bool(market.active),
                    ),
                )
            )
        return tuple(result)

    @abstractmethod
    def fetch_ohlcv(self, symbol: str, interval: str, **kwargs: Any) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_balance(self, asset: str | None = None) -> Balance | Mapping[str, Balance]:
        raise NotImplementedError

    @abstractmethod
    def fetch_ticker(self, symbol: str) -> Ticker:
        raise NotImplementedError

    @abstractmethod
    def fetch_order_book(self, symbol: str, limit: int | None = None) -> OrderBook:
        raise NotImplementedError

    @abstractmethod
    def create_market_buy(
        self,
        symbol: str,
        *,
        amount: Decimal | None = None,
        quote_amount: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def create_market_sell(
        self,
        symbol: str,
        amount: Decimal,
        *,
        client_order_id: str | None = None,
    ) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def fetch_order(self, exchange_order_id: str, symbol: str) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def fetch_order_by_client_id(
        self, client_order_id: str, symbol: str
    ) -> OrderResult | None:
        """Resolve an order without submitting a replacement order."""
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, exchange_order_id: str, symbol: str) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def normalize_amount(self, symbol: str, amount: Decimal) -> Decimal:
        raise NotImplementedError

    @abstractmethod
    def normalize_price(self, symbol: str, price: Decimal) -> Decimal:
        raise NotImplementedError

    @abstractmethod
    def validate_order(self, request: OrderRequest) -> OrderValidation:
        raise NotImplementedError

    @abstractmethod
    def health_check(self) -> ExchangeHealth:
        raise NotImplementedError
