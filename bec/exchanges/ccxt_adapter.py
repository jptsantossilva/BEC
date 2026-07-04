"""Reusable public-market-data adapter backed by CCXT."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping

import ccxt
import pandas as pd

from bec.exchanges.base import (
    Balance,
    ExchangeAdapter,
    ExchangeHealth,
    MarketInfo,
    OrderBook,
    OrderRequest,
    OrderResult,
    OrderValidation,
    Ticker,
)


class PrivateExchangeOperationDisabled(NotImplementedError):
    """Raised when PR5 code attempts to use private exchange functionality."""


def _decimal(value: Any, default: str = "0") -> Decimal:
    if value in (None, ""):
        return Decimal(default)
    return Decimal(str(value))


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    converted = _decimal(value)
    return converted if converted != 0 else None


def _timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)


class CcxtExchangeAdapter(ExchangeAdapter):
    """CCXT adapter with public data enabled and all private APIs disabled."""

    asset_aliases: Mapping[str, str] = {}

    def __init__(
        self,
        exchange_id: str,
        *,
        client: Any | None = None,
        name: str | None = None,
        market_cache_ttl_seconds: float = 900,
        clock=time.monotonic,
    ):
        self.code = str(exchange_id).lower()
        self.name = name or self.code.title()
        self._client = client or self._create_public_client(self.code)
        self._market_cache_ttl_seconds = float(market_cache_ttl_seconds)
        self._clock = clock
        self._markets: dict[str, MarketInfo] | None = None
        self._market_lookup: dict[str, str] = {}
        self._markets_loaded_at = 0.0

    @staticmethod
    def _create_public_client(exchange_id: str):
        try:
            exchange_class = getattr(ccxt, exchange_id)
        except AttributeError as exc:
            raise ValueError(f"Unknown CCXT exchange: {exchange_id}") from exc
        return exchange_class(
            {
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
            }
        )

    @property
    def client(self):
        return self._client

    def _canonical_asset(self, asset: Any) -> str:
        value = str(asset or "").strip().upper()
        return self.asset_aliases.get(value, value)

    def _precision_step(self, value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        if getattr(self.client, "precisionMode", None) == ccxt.DECIMAL_PLACES:
            places = int(value)
            return Decimal("1").scaleb(-places)
        return _optional_decimal(value)

    def load_markets(self, *, force: bool = False) -> Mapping[str, MarketInfo]:
        now = float(self._clock())
        cache_valid = (
            self._markets is not None
            and not force
            and now - self._markets_loaded_at < self._market_cache_ttl_seconds
        )
        if cache_valid:
            return self._markets

        raw_markets = self.client.load_markets(
            reload=bool(force or self._markets is not None)
        )
        quote_market_buy = bool(
            getattr(self.client, "has", {}).get("createMarketBuyOrderWithCost", False)
        )
        markets: dict[str, MarketInfo] = {}
        lookup: dict[str, str] = {}
        for raw in raw_markets.values():
            if not (raw.get("spot") or raw.get("type") == "spot"):
                continue
            base = self._canonical_asset(raw.get("base"))
            quote = self._canonical_asset(raw.get("quote"))
            exchange_symbol = str(raw.get("id") or "").strip()
            if not base or not quote or not exchange_symbol:
                continue
            symbol = f"{base}/{quote}"
            limits = raw.get("limits") or {}
            amount_limits = limits.get("amount") or {}
            cost_limits = limits.get("cost") or {}
            precision = raw.get("precision") or {}
            market = MarketInfo(
                symbol=symbol,
                exchange_symbol=exchange_symbol,
                base_asset=base,
                quote_asset=quote,
                active=bool(raw.get("active", True)),
                amount_step=self._precision_step(precision.get("amount")),
                price_step=self._precision_step(precision.get("price")),
                min_amount=_optional_decimal(amount_limits.get("min")),
                max_amount=_optional_decimal(amount_limits.get("max")),
                min_cost=_optional_decimal(cost_limits.get("min")),
                max_cost=_optional_decimal(cost_limits.get("max")),
                quote_market_buy_allowed=quote_market_buy,
                raw=raw,
            )
            markets[symbol] = market
            aliases = {
                symbol,
                str(raw.get("symbol") or ""),
                exchange_symbol,
                str((raw.get("info") or {}).get("altname") or ""),
                str((raw.get("info") or {}).get("wsname") or ""),
            }
            for alias in aliases:
                if alias:
                    lookup[alias.strip().upper()] = symbol
            for alias, canonical in self.asset_aliases.items():
                if canonical == base:
                    lookup[f"{alias}/{quote}"] = symbol

        self._markets = markets
        self._market_lookup = lookup
        self._markets_loaded_at = now
        return markets

    def normalize_symbol(self, symbol: str) -> str:
        value = str(symbol or "").strip().upper()
        if not value:
            raise ValueError("Symbol is required")
        self.load_markets()
        if "/" in value:
            base, quote = value.split("/", 1)
            value = f"{self._canonical_asset(base)}/{self._canonical_asset(quote)}"
        try:
            return self._market_lookup[value]
        except KeyError as exc:
            raise ValueError(f"Unknown {self.name} spot symbol: {symbol}") from exc

    def _market(self, symbol: str) -> MarketInfo:
        canonical = self.normalize_symbol(symbol)
        return self.load_markets()[canonical]

    def normalize_amount(self, symbol: str, amount: Decimal) -> Decimal:
        canonical = self.normalize_symbol(symbol)
        return _decimal(self.client.amount_to_precision(canonical, str(amount)))

    def normalize_price(self, symbol: str, price: Decimal) -> Decimal:
        canonical = self.normalize_symbol(symbol)
        return _decimal(self.client.price_to_precision(canonical, str(price)))

    def validate_order(self, request: OrderRequest) -> OrderValidation:
        market = self._market(request.symbol)
        errors: list[str] = []
        if request.amount is not None and request.quote_amount is not None:
            errors.append("amount and quote_amount are mutually exclusive")
        if request.amount is None and request.quote_amount is None:
            errors.append("amount or quote_amount is required")
        normalized_amount = None
        normalized_price = None
        estimated_cost = None
        if request.amount is not None:
            normalized_amount = self.normalize_amount(market.symbol, request.amount)
            if normalized_amount <= 0:
                errors.append("amount must be greater than zero")
            if market.min_amount is not None and normalized_amount < market.min_amount:
                errors.append("amount is below the exchange minimum")
            if market.max_amount is not None and normalized_amount > market.max_amount:
                errors.append("amount is above the exchange maximum")
        if request.price is not None:
            normalized_price = self.normalize_price(market.symbol, request.price)
        if normalized_amount is not None and normalized_price is not None:
            estimated_cost = normalized_amount * normalized_price
        elif request.quote_amount is not None:
            estimated_cost = _decimal(request.quote_amount)
        if estimated_cost is not None:
            if market.min_cost is not None and estimated_cost < market.min_cost:
                errors.append("cost is below the exchange minimum")
            if market.max_cost is not None and estimated_cost > market.max_cost:
                errors.append("cost is above the exchange maximum")
        return OrderValidation(
            valid=not errors,
            errors=tuple(errors),
            normalized_amount=normalized_amount,
            normalized_price=normalized_price,
            estimated_cost=estimated_cost,
        )

    @staticmethod
    def _milliseconds(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        parsed = pd.Timestamp(value)
        if parsed.tzinfo is None:
            parsed = parsed.tz_localize("UTC")
        else:
            parsed = parsed.tz_convert("UTC")
        return int(parsed.timestamp() * 1000)

    def fetch_ohlcv(self, symbol: str, interval: str, **kwargs: Any) -> pd.DataFrame:
        canonical = self.normalize_symbol(symbol)
        start_ms = self._milliseconds(kwargs.pop("start_date", kwargs.pop("since", None)))
        end_ms = self._milliseconds(kwargs.pop("end_date", None))
        limit = int(kwargs.pop("limit", 720))
        drop_last = bool(kwargs.pop("drop_last", False))
        drop_incomplete = bool(kwargs.pop("drop_incomplete", False))
        include_symbol = bool(kwargs.pop("include_symbol", False))
        set_index = bool(kwargs.pop("set_index", True))
        keep_time_col = bool(kwargs.pop("keep_time_col", True))
        kwargs.pop("max_retries", None)
        kwargs.pop("backoff_sec", None)
        params = dict(kwargs.pop("params", {}))
        if kwargs:
            raise TypeError(f"Unsupported OHLCV options: {', '.join(sorted(kwargs))}")

        duration_ms = int(self.client.parse_timeframe(interval) * 1000)
        cursor = start_ms
        rows: dict[int, list[Any]] = {}
        for _ in range(1000):
            batch = self.client.fetch_ohlcv(
                canonical,
                timeframe=interval,
                since=cursor,
                limit=limit,
                params=params,
            )
            if not batch:
                break
            for candle in batch:
                timestamp_ms = int(candle[0])
                if end_ms is None or timestamp_ms <= end_ms:
                    rows[timestamp_ms] = list(candle[:6])
            next_cursor = int(batch[-1][0]) + duration_ms
            if cursor is None or len(batch) < limit or next_cursor <= cursor:
                break
            if end_ms is not None and next_cursor > end_ms:
                break
            cursor = next_cursor

        if not rows:
            return pd.DataFrame()
        data = [rows[key] for key in sorted(rows)]
        frame = pd.DataFrame(
            data,
            columns=["Timestamp", "Open", "High", "Low", "Close", "Volume"],
        )
        if drop_incomplete:
            now_ms = int(self.client.milliseconds())
            frame = frame[frame["Timestamp"] + duration_ms <= now_ms]
        if drop_last and not frame.empty:
            frame = frame.iloc[:-1]
        if frame.empty:
            return frame
        frame["Time"] = pd.to_datetime(frame.pop("Timestamp"), unit="ms", utc=True)
        frame["Time"] = frame["Time"].dt.tz_localize(None)
        numeric = ["Open", "High", "Low", "Close", "Volume"]
        frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="coerce")
        if include_symbol:
            frame["Symbol"] = canonical
        if set_index:
            frame.index = pd.DatetimeIndex(frame["Time"])
        if not keep_time_col:
            frame = frame.drop(columns=["Time"])
        return frame

    def fetch_ticker(self, symbol: str) -> Ticker:
        canonical = self.normalize_symbol(symbol)
        raw = self.client.fetch_ticker(canonical)
        return Ticker(
            symbol=canonical,
            last=_decimal(raw.get("last") or raw.get("close")),
            bid=_optional_decimal(raw.get("bid")),
            ask=_optional_decimal(raw.get("ask")),
            timestamp=_timestamp(raw.get("timestamp")),
        )

    def fetch_order_book(self, symbol: str, limit: int | None = None) -> OrderBook:
        canonical = self.normalize_symbol(symbol)
        raw = self.client.fetch_order_book(canonical, limit=limit)
        return OrderBook(
            symbol=canonical,
            bids=tuple(
                (_decimal(price), _decimal(amount))
                for price, amount, *_ in raw.get("bids", [])
            ),
            asks=tuple(
                (_decimal(price), _decimal(amount))
                for price, amount, *_ in raw.get("asks", [])
            ),
            timestamp=_timestamp(raw.get("timestamp")),
        )

    def health_check(self) -> ExchangeHealth:
        checked_at = datetime.now(timezone.utc)
        try:
            if getattr(self.client, "has", {}).get("fetchStatus"):
                status = self.client.fetch_status() or {}
                state = str(status.get("status", "ok")).lower()
                available = state not in {"error", "maintenance", "shutdown"}
                message = str(status.get("updated") or status.get("status") or "ok")
            else:
                self.load_markets()
                available = True
                message = "public API available"
            return ExchangeHealth(available, message, checked_at)
        except Exception as exc:
            return ExchangeHealth(False, f"public API unavailable: {exc!r}", checked_at)

    def _private_disabled(self, operation: str):
        raise PrivateExchangeOperationDisabled(
            f"{self.name} {operation} is disabled until gated live execution is implemented"
        )

    def fetch_balance(self, asset: str | None = None) -> Balance | Mapping[str, Balance]:
        del asset
        self._private_disabled("balances")

    def create_market_buy(
        self,
        symbol: str,
        *,
        amount: Decimal | None = None,
        quote_amount: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> OrderResult:
        del symbol, amount, quote_amount, client_order_id
        self._private_disabled("market buys")

    def create_market_sell(
        self,
        symbol: str,
        amount: Decimal,
        *,
        client_order_id: str | None = None,
    ) -> OrderResult:
        del symbol, amount, client_order_id
        self._private_disabled("market sells")

    def fetch_order(self, exchange_order_id: str, symbol: str) -> OrderResult:
        del exchange_order_id, symbol
        self._private_disabled("order lookup")

    def cancel_order(self, exchange_order_id: str, symbol: str) -> OrderResult:
        del exchange_order_id, symbol
        self._private_disabled("order cancellation")
