"""Reusable public-market-data adapter backed by CCXT."""

from __future__ import annotations

import logging
import random
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping

import ccxt
import pandas as pd

from bec.exchanges.base import (
    Balance,
    ExchangeAdapter,
    ExchangeCapabilities,
    ExchangeHealth,
    MarketInfo,
    OrderBook,
    OrderFill,
    OrderStatus,
    OrderRequest,
    OrderResult,
    OrderValidation,
    Ticker,
)
from bec.exchanges.public_rate_limit import SharedPublicRequestThrottle


logger = logging.getLogger(__name__)

TRANSIENT_PUBLIC_READ_ERRORS = (
    ccxt.RateLimitExceeded,
    ccxt.DDoSProtection,
    ccxt.RequestTimeout,
    ccxt.ExchangeNotAvailable,
)


class TransientPublicMarketDataError(RuntimeError):
    """Raised after retry-safe public market-data reads exhaust their budget."""


class PrivateExchangeOperationDisabled(NotImplementedError):
    """Raised when private exchange functionality lacks explicit credentials."""


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
    """CCXT spot adapter with explicitly gated private operations."""

    asset_aliases: Mapping[str, str] = {}
    capabilities = ExchangeCapabilities(
        supports_backtesting=True,
        supports_live_trading=True,
        requires_explicit_live_flags=True,
        supports_reconciliation=True,
        uses_gated_live_execution=True,
    )

    def __init__(
        self,
        exchange_id: str,
        *,
        client: Any | None = None,
        name: str | None = None,
        market_cache_ttl_seconds: float = 900,
        api_key: str = "",
        api_secret: str = "",
        api_password: str = "",
        private_enabled: bool = False,
        sizing_buffer_pct: Decimal = Decimal("1"),
        clock=time.monotonic,
        public_request_throttle: SharedPublicRequestThrottle | None = None,
        public_read_retry_enabled: bool = False,
        sleeper: Callable[[float], None] = time.sleep,
        random_uniform: Callable[[float, float], float] = random.uniform,
    ):
        self.code = str(exchange_id).lower()
        self.name = name or self.code.title()
        self._client = client or self._create_client(
            self.code,
            api_key=api_key,
            api_secret=api_secret,
            api_password=api_password,
        )
        self._private_enabled = bool(private_enabled)
        self._sizing_buffer_pct = _decimal(sizing_buffer_pct)
        self._market_cache_ttl_seconds = float(market_cache_ttl_seconds)
        self._clock = clock
        self._markets: dict[str, MarketInfo] | None = None
        self._market_lookup: dict[str, str] = {}
        self._markets_loaded_at = 0.0
        self._public_request_throttle = public_request_throttle
        self._public_read_retry_enabled = bool(public_read_retry_enabled)
        self._sleep = sleeper
        self._random_uniform = random_uniform

    @staticmethod
    def _create_client(
        exchange_id: str,
        *,
        api_key: str = "",
        api_secret: str = "",
        api_password: str = "",
    ):
        try:
            exchange_class = getattr(ccxt, exchange_id)
        except AttributeError as exc:
            raise ValueError(f"Unknown CCXT exchange: {exchange_id}") from exc
        config = {
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        }
        if api_key:
            config["apiKey"] = api_key
        if api_secret:
            config["secret"] = api_secret
        if api_password:
            # CCXT maps this field to an exchange-specific API passphrase.
            config["password"] = api_password
        return exchange_class(config)

    @property
    def client(self):
        return self._client

    @property
    def private_enabled(self) -> bool:
        return self._private_enabled

    def _call_public_read(
        self,
        operation: str,
        callback: Callable[[], Any],
        *,
        max_retries: int = 7,
        backoff_sec: float = 2.0,
        max_backoff_sec: float = 60.0,
        context: str = "",
    ):
        if (
            not self._public_read_retry_enabled
            and self._public_request_throttle is None
        ):
            return callback()

        retries = max(int(max_retries), 0) if self._public_read_retry_enabled else 0
        initial_backoff = max(float(backoff_sec), 0.0)
        maximum_backoff = max(float(max_backoff_sec), initial_backoff)

        for retry_index in range(retries + 1):
            throttle_context = (
                self._public_request_throttle.request_slot()
                if self._public_request_throttle is not None
                else nullcontext(None)
            )
            retry_delay = 0.0
            caught: Exception | None = None
            with throttle_context as lease:
                try:
                    return callback()
                except TRANSIENT_PUBLIC_READ_ERRORS as exc:
                    caught = exc
                    base_delay = min(
                        initial_backoff * (2**retry_index),
                        maximum_backoff,
                    )
                    jitter = self._random_uniform(0.0, base_delay * 0.25)
                    retry_delay = min(base_delay + jitter, maximum_backoff)
                    if lease is not None:
                        lease.defer(retry_delay)

            if caught is None:
                continue

            attempts = retry_index + 1
            detail = f" ({context})" if context else ""
            if retry_index >= retries:
                raise TransientPublicMarketDataError(
                    f"{self.name} public {operation}{detail} failed after "
                    f"{attempts} attempts: {caught!r}"
                ) from caught

            logger.warning(
                "%s public %s%s retry %s/%s in %.2fs after %s",
                self.name,
                operation,
                detail,
                attempts,
                retries,
                retry_delay,
                type(caught).__name__,
            )
            if self._public_request_throttle is None and retry_delay:
                self._sleep(retry_delay)

    def is_known_submission_rejection(self, exc: Exception) -> bool:
        return isinstance(
            exc,
            (
                ValueError,
                PrivateExchangeOperationDisabled,
                ccxt.AuthenticationError,
                ccxt.PermissionDenied,
                ccxt.InsufficientFunds,
                ccxt.InvalidOrder,
                ccxt.BadRequest,
                ccxt.BadSymbol,
            ),
        )

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

        raw_markets = self._call_public_read(
            "load_markets",
            lambda: self.client.load_markets(
                reload=bool(force or self._markets is not None)
            ),
        )
        quote_market_buy = bool(
            getattr(self.client, "has", {}).get("createMarketBuyOrderWithCost", False)
        )
        markets: dict[str, MarketInfo] = {}
        lookup: dict[str, str] = {}

        def register_alias(alias: object, canonical: str) -> None:
            value = str(alias or "").strip().upper()
            if not value:
                return
            existing = lookup.get(value)
            if existing is not None and existing != canonical:
                # Some exchanges retain legacy markets whose display alias is
                # also the canonical symbol of a newer market (Kraken exposes
                # REP/EUR for both REP/EUR and legacy REPV1/EUR).  Keep the
                # real canonical market addressable and leave the legacy
                # market available through its own canonical/native symbols.
                if value in markets:
                    lookup[value] = value
                    return
                raise ValueError(
                    f"{self.name} market alias collision for {value}: "
                    f"{existing} and {canonical}"
                )
            lookup[value] = canonical

        for raw in raw_markets.values():
            if not (raw.get("spot") or raw.get("type") == "spot"):
                continue
            if bool(raw.get("contract", False)) or not bool(raw.get("active", True)):
                continue
            base = self._canonical_asset(raw.get("base"))
            quote = self._canonical_asset(raw.get("quote"))
            exchange_symbol = str(raw.get("id") or "").strip()
            if not base or not quote or not exchange_symbol:
                continue
            symbol = f"{base}/{quote}"
            if symbol in markets:
                raise ValueError(f"{self.name} canonical market collision for {symbol}")
            limits = raw.get("limits") or {}
            amount_limits = limits.get("amount") or {}
            cost_limits = limits.get("cost") or {}
            precision = raw.get("precision") or {}
            market = MarketInfo(
                symbol=symbol,
                exchange_symbol=exchange_symbol,
                base_asset=base,
                quote_asset=quote,
                active=True,
                amount_step=self._precision_step(precision.get("amount")),
                price_step=self._precision_step(precision.get("price")),
                min_amount=_optional_decimal(amount_limits.get("min")),
                max_amount=_optional_decimal(amount_limits.get("max")),
                min_cost=_optional_decimal(cost_limits.get("min")),
                max_cost=_optional_decimal(cost_limits.get("max")),
                quote_market_buy_allowed=quote_market_buy,
                market_type=str(raw.get("type") or "spot").lower(),
                spot=bool(raw.get("spot") or raw.get("type") == "spot"),
                contract=bool(raw.get("contract", False)),
                contract_size=_optional_decimal(raw.get("contractSize")),
                linear=bool(raw.get("linear", False)),
                inverse=bool(raw.get("inverse", False)),
                settle_asset=self._canonical_asset(raw.get("settle")) or None,
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
                register_alias(alias, symbol)
            for alias, canonical in self.asset_aliases.items():
                if canonical == base:
                    register_alias(f"{alias}/{quote}", symbol)

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
        if not market.active:
            errors.append("market is not active")
        if str(request.side).lower() not in {"buy", "sell"}:
            errors.append("side must be buy or sell")
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
            if estimated_cost <= 0:
                errors.append("quote amount must be greater than zero")
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
        max_retries = int(kwargs.pop("max_retries", 7))
        backoff_sec = float(kwargs.pop("backoff_sec", 2.0))
        max_backoff_sec = float(kwargs.pop("max_backoff_sec", 60.0))
        params = dict(kwargs.pop("params", {}))
        if kwargs:
            raise TypeError(f"Unsupported OHLCV options: {', '.join(sorted(kwargs))}")

        duration_ms = int(self.client.parse_timeframe(interval) * 1000)
        cursor = start_ms
        rows: dict[int, list[Any]] = {}
        for _ in range(1000):
            batch = self._call_public_read(
                "fetch_ohlcv",
                lambda: self.client.fetch_ohlcv(
                    canonical,
                    timeframe=interval,
                    since=cursor,
                    limit=limit,
                    params=params,
                ),
                max_retries=max_retries,
                backoff_sec=backoff_sec,
                max_backoff_sec=max_backoff_sec,
                context=f"{canonical} {interval}",
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
        raw = self._call_public_read(
            "fetch_ticker",
            lambda: self.client.fetch_ticker(canonical),
            context=canonical,
        )
        return Ticker(
            symbol=canonical,
            last=_decimal(raw.get("last") or raw.get("close")),
            bid=_optional_decimal(raw.get("bid")),
            ask=_optional_decimal(raw.get("ask")),
            timestamp=_timestamp(raw.get("timestamp")),
        )

    def fetch_order_book(self, symbol: str, limit: int | None = None) -> OrderBook:
        canonical = self.normalize_symbol(symbol)
        raw = self._call_public_read(
            "fetch_order_book",
            lambda: self.client.fetch_order_book(canonical, limit=limit),
            context=canonical,
        )
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
                status = self._call_public_read(
                    "fetch_status",
                    self.client.fetch_status,
                ) or {}
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
        if not self._private_enabled:
            raise PrivateExchangeOperationDisabled(
                f"{self.name} {operation} requires configured private credentials"
            )

    @staticmethod
    def _order_status(value: Any) -> OrderStatus:
        normalized = str(value or "").strip().lower()
        return {
            "pending": OrderStatus.PENDING,
            "open": OrderStatus.OPEN,
            "partially_filled": OrderStatus.PARTIALLY_FILLED,
            "partial": OrderStatus.PARTIALLY_FILLED,
            "closed": OrderStatus.FILLED,
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELED,
            "cancelled": OrderStatus.CANCELED,
            "rejected": OrderStatus.REJECTED,
            "expired": OrderStatus.EXPIRED,
        }.get(normalized, OrderStatus.UNKNOWN)

    def _parse_order(self, raw: Mapping[str, Any], canonical: str) -> OrderResult:
        filled = _decimal(raw.get("filled"))
        requested = _optional_decimal(raw.get("amount"))
        average = _optional_decimal(raw.get("average"))
        cost = _optional_decimal(raw.get("cost"))
        if average is None and cost is not None and filled > 0:
            average = cost / filled

        fills = []
        for item in raw.get("trades") or raw.get("fills") or ():
            fee = item.get("fee") or {}
            fills.append(
                OrderFill(
                    price=_decimal(item.get("price")),
                    quantity=_decimal(item.get("amount") or item.get("qty")),
                    fee_asset=(
                        self._canonical_asset(fee.get("currency"))
                        if fee.get("currency")
                        else None
                    ),
                    fee_amount=_decimal(fee.get("cost")),
                    trade_id=(
                        str(item.get("id")) if item.get("id") is not None else None
                    ),
                    timestamp=_timestamp(item.get("timestamp")),
                    raw=item,
                )
            )
        if not fills and raw.get("fee"):
            fee = raw.get("fee") or {}
            fills.append(
                OrderFill(
                    price=average or Decimal("0"),
                    quantity=filled,
                    fee_asset=(
                        self._canonical_asset(fee.get("currency"))
                        if fee.get("currency")
                        else None
                    ),
                    fee_amount=_decimal(fee.get("cost")),
                    raw=raw,
                )
            )
        if not fills and raw.get("fees"):
            for fee in raw.get("fees") or ():
                fills.append(
                    OrderFill(
                        price=average or Decimal("0"),
                        quantity=filled,
                        fee_asset=(
                            self._canonical_asset(fee.get("currency"))
                            if fee.get("currency")
                            else None
                        ),
                        fee_amount=_decimal(fee.get("cost")),
                        raw=raw,
                    )
                )

        market = self._market(canonical)
        status = self._order_status(raw.get("status"))
        if (
            status is OrderStatus.OPEN
            and filled > 0
            and requested is not None
            and filled < requested
        ):
            status = OrderStatus.PARTIALLY_FILLED
        return OrderResult(
            exchange_order_id=str(raw.get("id") or ""),
            symbol=canonical,
            exchange_symbol=market.exchange_symbol,
            side=str(raw.get("side") or "").lower(),
            status=status,
            requested_quantity=requested,
            executed_quantity=filled,
            average_price=average,
            fills=tuple(fills),
            client_order_id=(
                str(raw.get("clientOrderId"))
                if raw.get("clientOrderId") not in (None, "")
                else None
            ),
            timestamp=_timestamp(raw.get("timestamp")),
            raw=raw,
        )

    def _parse_balance_response(self, raw: Mapping[str, Any]) -> dict[str, Balance]:
        """Convert a CCXT balance response into the application balance model."""
        free = raw.get("free") or {}
        used = raw.get("used") or {}
        balances = {}
        for source in set(free) | set(used):
            canonical = self._canonical_asset(source)
            if not canonical:
                continue
            previous = balances.get(
                canonical, Balance(canonical, Decimal("0"), Decimal("0"))
            )
            balances[canonical] = Balance(
                asset=canonical,
                free=previous.free + _decimal(free.get(source)),
                locked=previous.locked + _decimal(used.get(source)),
            )
        return balances

    def fetch_balance(self, asset: str | None = None) -> Balance | Mapping[str, Balance]:
        self._private_disabled("balances")
        balances = self._parse_balance_response(self.client.fetch_balance())
        if asset is not None:
            canonical = self._canonical_asset(asset)
            return balances.get(canonical, Balance(canonical, Decimal("0")))
        return balances

    def create_market_buy(
        self,
        symbol: str,
        *,
        amount: Decimal | None = None,
        quote_amount: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> OrderResult:
        self._private_disabled("market buys")
        canonical = self.normalize_symbol(symbol)
        params = {"clientOrderId": client_order_id} if client_order_id else {}
        if quote_amount is not None and amount is not None:
            raise ValueError("amount and quote_amount are mutually exclusive")
        if quote_amount is not None:
            quote_amount = _decimal(quote_amount)
            market = self._market(canonical)
            if market.quote_market_buy_allowed and hasattr(
                self.client, "create_market_buy_order_with_cost"
            ):
                raw = self.client.create_market_buy_order_with_cost(
                    canonical, float(quote_amount), params
                )
                return self._parse_order(raw, canonical)
            ticker = self.fetch_ticker(canonical)
            price = ticker.ask or ticker.last
            buffer = (Decimal("100") - self._sizing_buffer_pct) / Decimal("100")
            amount = self.normalize_amount(canonical, (quote_amount / price) * buffer)
        if amount is None:
            raise ValueError("amount or quote_amount is required")
        amount = self.normalize_amount(canonical, _decimal(amount))
        ticker = self.fetch_ticker(canonical)
        validation = self.validate_order(
            OrderRequest(canonical, "buy", amount=amount, price=ticker.ask or ticker.last)
        )
        if not validation.valid:
            raise ValueError("; ".join(validation.errors))
        raw = self.client.create_order(
            canonical, "market", "buy", float(amount), None, params
        )
        return self._parse_order(raw, canonical)

    def create_market_sell(
        self,
        symbol: str,
        amount: Decimal,
        *,
        client_order_id: str | None = None,
    ) -> OrderResult:
        self._private_disabled("market sells")
        canonical = self.normalize_symbol(symbol)
        amount = self.normalize_amount(canonical, _decimal(amount))
        ticker = self.fetch_ticker(canonical)
        validation = self.validate_order(
            OrderRequest(canonical, "sell", amount=amount, price=ticker.bid or ticker.last)
        )
        if not validation.valid:
            raise ValueError("; ".join(validation.errors))
        params = {"clientOrderId": client_order_id} if client_order_id else {}
        raw = self.client.create_order(
            canonical, "market", "sell", float(amount), None, params
        )
        return self._parse_order(raw, canonical)

    def fetch_order(self, exchange_order_id: str, symbol: str) -> OrderResult:
        self._private_disabled("order lookup")
        canonical = self.normalize_symbol(symbol)
        return self._parse_order(
            self.client.fetch_order(str(exchange_order_id), canonical), canonical
        )

    def fetch_order_by_client_id(
        self, client_order_id: str, symbol: str
    ) -> OrderResult | None:
        """Resolve an uncertain submission without ever resubmitting it."""
        self._private_disabled("order lookup")
        canonical = self.normalize_symbol(symbol)
        params = {"clientOrderId": str(client_order_id)}
        direct_lookup = getattr(self.client, "fetch_order_by_client_order_id", None)
        if callable(direct_lookup):
            raw = direct_lookup(str(client_order_id), canonical, params)
            if raw:
                return self._parse_order(raw, canonical)
        methods = [
            getattr(self.client, "fetch_open_orders", None),
            getattr(self.client, "fetch_closed_orders", None),
        ]
        if not any(callable(method) for method in methods):
            methods.append(getattr(self.client, "fetch_orders", None))
        for method in methods:
            if not callable(method):
                continue
            orders = method(canonical, params=params)
            for raw in orders or ():
                if str(raw.get("clientOrderId") or "") == str(client_order_id):
                    return self._parse_order(raw, canonical)
        return None

    def cancel_order(self, exchange_order_id: str, symbol: str) -> OrderResult:
        self._private_disabled("order cancellation")
        canonical = self.normalize_symbol(symbol)
        return self._parse_order(
            self.client.cancel_order(str(exchange_order_id), canonical), canonical
        )
