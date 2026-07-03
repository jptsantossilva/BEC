"""Canonical exchange contracts used by BEC application code."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
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
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)


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

    @abstractmethod
    def load_markets(self, *, force: bool = False) -> Mapping[str, MarketInfo]:
        raise NotImplementedError

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

