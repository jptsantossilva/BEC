"""Native Binance implementation of the canonical exchange contract."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Mapping, Optional

from binance.client import Client
from binance.exceptions import (
    BinanceAPIException,
    BinanceOrderException,
    BinanceRequestException,
)
from binance.helpers import round_step_size

import bec.utils.telegram as telegram
from bec.exchanges.base import (
    Balance,
    ExchangeAdapter,
    ExchangeHealth,
    MarketInfo,
    OrderBook,
    OrderFill,
    OrderRequest,
    OrderResult,
    OrderStatus,
    OrderValidation,
    Ticker,
)

_client: Optional[Client] = None


def _decimal(value: Any, default: str = "0") -> Decimal:
    if value in (None, ""):
        return Decimal(default)
    return Decimal(str(value))


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    decimal_value = _decimal(value)
    return decimal_value if decimal_value != 0 else None


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        msg = f"[binance] Missing env var: {name}"
        print(msg)
        telegram.send_telegram_message(
            telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg
        )
        raise RuntimeError(msg)
    return value


def connect(
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    test_ping: bool = True,
) -> Client:
    """Create or replace the shared native Binance client."""
    global _client
    api_key = api_key or _env("binance_api")
    api_secret = api_secret or _env("binance_secret")

    max_retry = 3
    for attempt in range(1, max_retry + 1):
        try:
            _client = Client(api_key, api_secret, requests_params={"timeout": 15})
            if test_ping:
                _client.ping()
            return _client
        except BinanceAPIException as exc:
            msg = f"[binance] Error connecting to Binance: {exc!r}"
            print(msg)
            logging.exception(msg)
            if exc.status_code in (418, 429, 503) and attempt < max_retry:
                time.sleep(2**attempt)
                continue
            telegram.send_telegram_message(
                telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg
            )
            _client = None
            raise
        except Exception as exc:
            msg = f"[binance] Error connecting to Binance: {exc!r}"
            print(msg)
            logging.exception(msg)
            if attempt < max_retry:
                time.sleep(2**attempt)
                continue
            telegram.send_telegram_message(
                telegram.telegram_token_errors, telegram.EMOJI_WARNING, msg
            )
            _client = None
            raise
    raise RuntimeError("Unable to create Binance client")


def get_client() -> Client:
    global _client
    if _client is None:
        return connect()
    return _client


class BinanceAdapter(ExchangeAdapter):
    code = "binance"
    name = "Binance"

    def __init__(self, client: Client | None = None):
        self._injected_client = client
        self._markets: dict[str, MarketInfo] | None = None

    @property
    def client(self) -> Client:
        return self._injected_client or get_client()

    def get_exchange_info(self) -> Mapping[str, Any]:
        return self.client.get_exchange_info()

    def load_markets(self, *, force: bool = False) -> Mapping[str, MarketInfo]:
        if self._markets is not None and not force:
            return self._markets

        markets: dict[str, MarketInfo] = {}
        for raw in self.get_exchange_info().get("symbols", []):
            base = str(raw.get("baseAsset", "")).upper()
            quote = str(raw.get("quoteAsset", "")).upper()
            exchange_symbol = str(raw.get("symbol", "")).upper()
            if not base or not quote or not exchange_symbol:
                continue
            filters = {
                item.get("filterType"): item for item in raw.get("filters", [])
            }
            lot = filters.get("LOT_SIZE", {})
            price_filter = filters.get("PRICE_FILTER", {})
            notional = filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {}))
            symbol = f"{base}/{quote}"
            markets[symbol] = MarketInfo(
                symbol=symbol,
                exchange_symbol=exchange_symbol,
                base_asset=base,
                quote_asset=quote,
                active=raw.get("status") == "TRADING",
                amount_step=_optional_decimal(lot.get("stepSize")),
                price_step=_optional_decimal(price_filter.get("tickSize")),
                min_amount=_optional_decimal(lot.get("minQty")),
                max_amount=_optional_decimal(lot.get("maxQty")),
                min_cost=_optional_decimal(
                    notional.get("minNotional", notional.get("notional"))
                ),
                max_cost=_optional_decimal(notional.get("maxNotional")),
                quote_market_buy_allowed=bool(
                    raw.get("quoteOrderQtyMarketAllowed", False)
                ),
                raw=raw,
            )
        self._markets = markets
        return markets

    def _market(self, symbol: str) -> MarketInfo:
        normalized = self.normalize_symbol(symbol)
        try:
            return self.load_markets()[normalized]
        except KeyError as exc:
            raise ValueError(f"Unknown Binance spot symbol: {symbol}") from exc

    def to_exchange_symbol(self, symbol: str) -> str:
        return self._market(symbol).exchange_symbol

    def fetch_ohlcv(self, symbol: str, interval: str, **kwargs: Any):
        # Import lazily so the legacy compatibility module can use this adapter's
        # client without creating an import cycle.
        from bec.exchanges import binance as legacy_binance

        exchange_symbol = self.to_exchange_symbol(symbol) if "/" in symbol else symbol
        return legacy_binance.get_ohlcv(exchange_symbol, interval, **kwargs)

    def fetch_balance(self, asset: str | None = None) -> Balance | Mapping[str, Balance]:
        if asset is not None:
            raw = self.client.get_asset_balance(asset=asset.upper()) or {}
            return Balance(
                asset=asset.upper(),
                free=_decimal(raw.get("free")),
                locked=_decimal(raw.get("locked")),
            )
        balances = self.client.get_account().get("balances", [])
        return {
            str(item["asset"]).upper(): Balance(
                asset=str(item["asset"]).upper(),
                free=_decimal(item.get("free")),
                locked=_decimal(item.get("locked")),
            )
            for item in balances
        }

    def fetch_ticker(self, symbol: str) -> Ticker:
        canonical = self.normalize_symbol(symbol)
        exchange_symbol = self.to_exchange_symbol(canonical)
        raw = self.client.get_symbol_ticker(symbol=exchange_symbol)
        return Ticker(symbol=canonical, last=_decimal(raw.get("price")))

    def fetch_order_book(self, symbol: str, limit: int | None = None) -> OrderBook:
        canonical = self.normalize_symbol(symbol)
        params: dict[str, Any] = {"symbol": self.to_exchange_symbol(canonical)}
        if limit is not None:
            params["limit"] = limit
        raw = self.client.get_order_book(**params)
        return OrderBook(
            symbol=canonical,
            bids=tuple((_decimal(price), _decimal(amount)) for price, amount in raw.get("bids", [])),
            asks=tuple((_decimal(price), _decimal(amount)) for price, amount in raw.get("asks", [])),
        )

    def create_market_buy(
        self,
        symbol: str,
        *,
        amount: Decimal | None = None,
        quote_amount: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> OrderResult:
        canonical = self.normalize_symbol(symbol)
        market = self._market(canonical)
        if amount is not None and quote_amount is not None:
            raise ValueError("Market buy accepts either amount or quote_amount, not both")
        if quote_amount is not None and not market.quote_market_buy_allowed:
            raise ValueError(f"Quote-based market buys are not enabled for {canonical}")
        params: dict[str, Any] = {
            "symbol": self.to_exchange_symbol(canonical),
            "side": Client.SIDE_BUY,
            "type": Client.ORDER_TYPE_MARKET,
            "newOrderRespType": "FULL",
        }
        if quote_amount is not None:
            params["quoteOrderQty"] = str(quote_amount)
        elif amount is not None:
            params["quantity"] = str(self.normalize_amount(canonical, amount))
        else:
            raise ValueError("Market buy requires amount or quote_amount")
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return self._parse_order(self.client.create_order(**params), canonical)

    def create_market_sell(
        self,
        symbol: str,
        amount: Decimal,
        *,
        client_order_id: str | None = None,
    ) -> OrderResult:
        canonical = self.normalize_symbol(symbol)
        normalized_amount = self.normalize_amount(canonical, amount)
        if normalized_amount <= 0:
            raise ValueError("Market sell amount must be greater than zero")
        params: dict[str, Any] = {
            "symbol": self.to_exchange_symbol(canonical),
            "side": Client.SIDE_SELL,
            "type": Client.ORDER_TYPE_MARKET,
            "quantity": str(normalized_amount),
            "newOrderRespType": "FULL",
        }
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return self._parse_order(self.client.create_order(**params), canonical)

    def fetch_order(self, exchange_order_id: str, symbol: str) -> OrderResult:
        canonical = self.normalize_symbol(symbol)
        raw = self.client.get_order(
            symbol=self.to_exchange_symbol(canonical), orderId=exchange_order_id
        )
        return self._parse_order(raw, canonical)

    def cancel_order(self, exchange_order_id: str, symbol: str) -> OrderResult:
        canonical = self.normalize_symbol(symbol)
        raw = self.client.cancel_order(
            symbol=self.to_exchange_symbol(canonical), orderId=exchange_order_id
        )
        return self._parse_order(raw, canonical)

    def normalize_symbol(self, symbol: str) -> str:
        raw_symbol = str(symbol).strip().upper()
        if "/" in raw_symbol:
            base, quote = raw_symbol.split("/", 1)
            canonical = f"{base}/{quote}"
            if canonical in self.load_markets():
                return canonical
        else:
            for canonical, market in self.load_markets().items():
                if market.exchange_symbol == raw_symbol:
                    return canonical
        raise ValueError(f"Unknown Binance spot symbol: {symbol}")

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal | None) -> Decimal:
        if step is None or step <= 0:
            return value
        units = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return units * step

    def normalize_amount(self, symbol: str, amount: Decimal) -> Decimal:
        market = self._market(symbol)
        return self._floor_to_step(_decimal(amount), market.amount_step)

    def normalize_price(self, symbol: str, price: Decimal) -> Decimal:
        market = self._market(symbol)
        return self._floor_to_step(_decimal(price), market.price_step)

    def validate_order(self, request: OrderRequest) -> OrderValidation:
        market = self._market(request.symbol)
        errors: list[str] = []
        normalized_amount = (
            self.normalize_amount(market.symbol, request.amount)
            if request.amount is not None
            else None
        )
        normalized_price = (
            self.normalize_price(market.symbol, request.price)
            if request.price is not None
            else None
        )
        estimated_cost = request.quote_amount
        if estimated_cost is None and normalized_amount is not None and normalized_price is not None:
            estimated_cost = normalized_amount * normalized_price

        if not market.active:
            errors.append("market is not active")
        if normalized_amount is not None:
            if normalized_amount <= 0:
                errors.append("amount must be greater than zero")
            if market.min_amount is not None and normalized_amount < market.min_amount:
                errors.append("amount is below the exchange minimum")
            if market.max_amount is not None and normalized_amount > market.max_amount:
                errors.append("amount is above the exchange maximum")
        if estimated_cost is not None:
            if market.min_cost is not None and estimated_cost < market.min_cost:
                errors.append("cost is below the exchange minimum")
            if market.max_cost is not None and estimated_cost > market.max_cost:
                errors.append("cost is above the exchange maximum")
        if request.side.lower() not in {"buy", "sell"}:
            errors.append("side must be buy or sell")
        if request.amount is not None and request.quote_amount is not None:
            errors.append("amount and quote_amount are mutually exclusive")
        if request.quote_amount is not None and request.side.lower() != "buy":
            errors.append("quote_amount is supported only for market buys")
        if request.quote_amount is not None and not market.quote_market_buy_allowed:
            errors.append("quote-based market buys are not enabled for this market")
        if request.amount is None and request.quote_amount is None:
            errors.append("amount or quote_amount is required")
        return OrderValidation(
            valid=not errors,
            errors=tuple(errors),
            normalized_amount=normalized_amount,
            normalized_price=normalized_price,
            estimated_cost=estimated_cost,
        )

    def health_check(self) -> ExchangeHealth:
        checked_at = datetime.now(timezone.utc)
        try:
            self.client.ping()
            return ExchangeHealth(True, "Binance API is available", checked_at)
        except Exception as exc:
            return ExchangeHealth(False, f"Binance API unavailable: {exc!r}", checked_at)

    @staticmethod
    def _status(raw_status: Any, executed: Decimal, requested: Decimal | None) -> OrderStatus:
        value = str(raw_status or "").upper()
        mapping = {
            "NEW": OrderStatus.OPEN,
            "PENDING_NEW": OrderStatus.PENDING,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELED,
            "CANCELLED": OrderStatus.CANCELED,
            "REJECTED": OrderStatus.REJECTED,
            "EXPIRED": OrderStatus.EXPIRED,
            "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
        }
        if value in mapping:
            return mapping[value]
        if executed > 0 and requested is not None and executed < requested:
            return OrderStatus.PARTIALLY_FILLED
        return OrderStatus.UNKNOWN

    def _parse_order(self, raw: Mapping[str, Any], canonical: str) -> OrderResult:
        requested = _optional_decimal(raw.get("origQty"))
        executed = _decimal(raw.get("executedQty"))
        fills = tuple(
            OrderFill(
                price=_decimal(fill.get("price")),
                quantity=_decimal(fill.get("qty")),
                fee_asset=fill.get("commissionAsset"),
                fee_amount=_decimal(fill.get("commission")),
                trade_id=(str(fill["tradeId"]) if fill.get("tradeId") is not None else None),
            )
            for fill in raw.get("fills", [])
        )
        average_price: Decimal | None = None
        if executed > 0:
            quote_quantity = _decimal(raw.get("cummulativeQuoteQty"))
            if quote_quantity > 0:
                average_price = quote_quantity / executed
            elif fills:
                average_price = sum(
                    (fill.price * fill.quantity for fill in fills), Decimal("0")
                ) / executed
        timestamp_ms = raw.get("transactTime", raw.get("updateTime", raw.get("time")))
        timestamp = (
            datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
            if timestamp_ms is not None
            else None
        )
        return OrderResult(
            exchange_order_id=str(raw.get("orderId", "")),
            client_order_id=raw.get("clientOrderId"),
            symbol=canonical,
            exchange_symbol=str(raw.get("symbol") or self.to_exchange_symbol(canonical)),
            side=str(raw.get("side", "")).lower(),
            status=self._status(raw.get("status"), executed, requested),
            requested_quantity=requested,
            executed_quantity=executed,
            average_price=average_price,
            fills=fills,
            timestamp=timestamp,
            raw=raw,
        )


__all__ = [
    "BinanceAdapter",
    "BinanceAPIException",
    "BinanceOrderException",
    "BinanceRequestException",
    "Client",
    "connect",
    "get_client",
    "round_step_size",
]
