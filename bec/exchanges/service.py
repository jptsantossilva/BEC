"""Exchange-neutral compatibility boundary for BEC application code.

The domain-level buy/sell workflows remain behavior-compatible during Phase 1.
They are reached only through this module, while primitive exchange operations
use the canonical adapter contract.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from bec.exchanges import binance as _legacy_binance
from bec.exchanges.base import ExchangeAdapter, OrderRequest
from bec.exchanges.registry import get_default_adapter, set_default_adapter


def get_adapter() -> ExchangeAdapter:
    return get_default_adapter()


def set_adapter(adapter: ExchangeAdapter | None) -> None:
    set_default_adapter(adapter)


def exchange_code() -> str:
    return get_adapter().code


def format_chart_symbol(symbol: str) -> str:
    return f"{exchange_code().upper()}:{symbol}"


def load_markets(*, force: bool = False):
    return get_adapter().load_markets(force=force)


def get_tradable_symbols(
    quote_asset: str,
    *,
    excluded_base_assets: set[str] | None = None,
) -> list[str]:
    quote = str(quote_asset).upper()
    excluded = {str(asset).upper() for asset in (excluded_base_assets or set())}
    symbols = []
    for market in load_markets().values():
        if (
            market.active
            and market.quote_asset == quote
            and market.base_asset not in excluded
            and not market.base_asset.endswith(("UP", "DOWN"))
        ):
            symbols.append(market.exchange_symbol)
    return sorted(set(symbols))


def fetch_balance(asset: str | None = None):
    return get_adapter().fetch_balance(asset)


def fetch_ticker(symbol: str):
    return get_adapter().fetch_ticker(symbol)


def fetch_order_book(symbol: str, limit: int | None = None):
    return get_adapter().fetch_order_book(symbol, limit)


def create_market_buy(
    symbol: str,
    *,
    amount: Decimal | None = None,
    quote_amount: Decimal | None = None,
    client_order_id: str | None = None,
):
    return get_adapter().create_market_buy(
        symbol,
        amount=amount,
        quote_amount=quote_amount,
        client_order_id=client_order_id,
    )


def create_market_sell(
    symbol: str,
    amount: Decimal,
    *,
    client_order_id: str | None = None,
):
    return get_adapter().create_market_sell(
        symbol, amount, client_order_id=client_order_id
    )


def fetch_order(exchange_order_id: str, symbol: str):
    return get_adapter().fetch_order(exchange_order_id, symbol)


def cancel_order(exchange_order_id: str, symbol: str):
    return get_adapter().cancel_order(exchange_order_id, symbol)


def normalize_symbol(symbol: str) -> str:
    return get_adapter().normalize_symbol(symbol)


def normalize_amount(symbol: str, amount: Decimal) -> Decimal:
    return get_adapter().normalize_amount(symbol, amount)


def normalize_price(symbol: str, price: Decimal) -> Decimal:
    return get_adapter().normalize_price(symbol, price)


def validate_order(request: OrderRequest):
    return get_adapter().validate_order(request)


def health_check():
    return get_adapter().health_check()


def get_ohlcv(symbol: str, interval: str, *args: Any, **kwargs: Any):
    if args:
        positional = ("start_date", "end_date")
        if len(args) > len(positional):
            raise TypeError("get_ohlcv accepts at most two positional date arguments")
        kwargs.update(dict(zip(positional, args)))
    return get_adapter().fetch_ohlcv(symbol, interval, **kwargs)


def get_close_df(symbol: str, interval: str, *args: Any, **kwargs: Any):
    price_col = str(kwargs.pop("price_col", "Close"))
    include_symbol = bool(kwargs.get("include_symbol", False))
    keep_time_col = bool(kwargs.get("keep_time_col", False))
    kwargs.setdefault("drop_incomplete", True)
    kwargs.setdefault("keep_time_col", False)
    frame = get_ohlcv(symbol, interval, *args, **kwargs)
    if frame.empty:
        return frame
    columns = []
    if keep_time_col and "Time" in frame.columns:
        columns.append("Time")
    if include_symbol and "Symbol" in frame.columns:
        columns.append("Symbol")
    columns.append("Close")
    result = frame[columns].copy()
    if price_col != "Close":
        result = result.rename(columns={"Close": price_col})
    return result


def get_exchange_info():
    """Temporary raw-info compatibility API for callers migrated in Phase 1."""
    adapter = get_adapter()
    getter = getattr(adapter, "get_exchange_info", None)
    if getter is None:
        raise NotImplementedError("The active adapter has no raw exchange-info view")
    return getter()


def _require_native_private_trading(operation: str) -> None:
    if exchange_code() != "binance":
        from bec.exchanges.ccxt_adapter import PrivateExchangeOperationDisabled

        raise PrivateExchangeOperationDisabled(
            f"{exchange_code().title()} {operation} is disabled in public-data mode"
        )


def get_symbol_balance(symbol: str):
    _require_native_private_trading("balances")
    return _legacy_binance.get_symbol_balance(symbol)


def calc_stake_amount(symbol: str, bot: str):
    _require_native_private_trading("position sizing")
    return _legacy_binance.calc_stake_amount(symbol, bot)


def create_buy_order(*args: Any, **kwargs: Any):
    _require_native_private_trading("buy orders")
    return _legacy_binance.create_buy_order(*args, **kwargs)


def create_sell_order(*args: Any, **kwargs: Any):
    _require_native_private_trading("sell orders")
    return _legacy_binance.create_sell_order(*args, **kwargs)


def create_balance_snapshot(*args: Any, **kwargs: Any):
    _require_native_private_trading("balance snapshots")
    return _legacy_binance.create_balance_snapshot(*args, **kwargs)
