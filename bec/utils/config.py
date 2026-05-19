import sys
import importlib
import json
from dataclasses import dataclass

import pandas as pd

# from bec.utils import telegram
# from bec.utils import database
import bec.utils.telegram as telegram
import bec.utils.database as database

_settings_cache = None
DEFAULT_MAIN_STRATEGIES = ["ema_cross_with_market_phases"]
_UPDATABLE_SETTING_KEYS = {
    "stake_amount_type",
    "max_number_of_open_positions",
    "tradable_balance_ratio",
    "min_position_size",
    "trade_against",
    "stop_loss",
    "atr_trailing_enabled",
    "atr_period",
    "atr_multiplier",
    "atr_activation_pnl",
    "trade_top_performance",
    "main_strategies",
    "btc_strategy",
    "trade_against_switch",
    "take_profit_enabled",
    "run_mode",
    "lock_values",
    "bot_prefix",
    "telegram_routine_trade_logs",
    "trade_against_switch_stablecoin",
    "delisting_start_date",
}

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)


@dataclass(frozen=True)
class AppSettings:
    stake_amount_type: str
    max_number_of_open_positions: int
    tradable_balance_ratio: float
    min_position_size: float
    trade_against: str
    stop_loss: float
    atr_trailing_enabled: bool
    atr_period: int
    atr_multiplier: float
    atr_activation_pnl: float
    trade_top_performance: int
    main_strategies: list
    main_strategy_configs: list
    btc_strategy: str
    btc_strategy_name: str
    btc_strategy_backtest_optimize: bool
    trade_against_switch: bool
    take_profit_enabled: bool
    run_mode: str
    lock_values: bool
    bot_prefix: str
    telegram_routine_trade_logs: str
    trade_against_switch_stablecoin: str
    delisting_start_date: str
    btc_strategy_impl: object
    n_decimals: int


def _invalidate_settings_cache():
    global _settings_cache
    _settings_cache = None


def update_settings(patch: dict, *, refresh: bool = True) -> AppSettings:
    """Persist one or many settings and return updated settings snapshot."""
    if not patch:
        return load_settings(refresh=refresh)

    invalid = sorted(
        [key for key in patch.keys() if key not in _UPDATABLE_SETTING_KEYS]
    )
    if invalid:
        raise ValueError(f"Invalid setting keys: {', '.join(invalid)}")

    for name, value in patch.items():
        database.set_setting(name, value)

    _invalidate_settings_cache()
    return load_settings(refresh=refresh)


def update_setting(name: str, value, *, refresh: bool = True) -> AppSettings:
    """Persist one setting and return updated settings snapshot."""
    return update_settings({name: value}, refresh=refresh)


def _parse_main_strategies(raw_value) -> list[str]:
    if raw_value in (None, ""):
        return list(DEFAULT_MAIN_STRATEGIES)

    if isinstance(raw_value, list):
        values = raw_value
    else:
        try:
            parsed = json.loads(str(raw_value))
            values = parsed if isinstance(parsed, list) else [str(parsed)]
        except (TypeError, ValueError, json.JSONDecodeError):
            values = [part.strip() for part in str(raw_value).split(",")]

    result = []
    for value in values:
        strategy_id = str(value).strip()
        if strategy_id and strategy_id not in result:
            result.append(strategy_id)

    return result or list(DEFAULT_MAIN_STRATEGIES)


def read_setting(name: str):
    """Read setting value from the current settings snapshot."""
    settings = load_settings()
    if not hasattr(settings, name):
        raise ValueError(f"Unknown setting name: {name}")
    return getattr(settings, name)


# Function to handle errors (common code for error handling)
def handle_error(error, message):
    msg = f"Error: {message}. {sys._getframe().f_code.co_name} - {repr(error)}"
    print(msg)
    # logging.exception(msg)
    telegram.send_telegram_message(
        telegram.telegramToken_errors, telegram.EMOJI_WARNING, msg
    )
    sys.exit(msg)


def load_settings(refresh: bool = False) -> AppSettings:
    """Load settings from DB once per process and return an immutable object."""
    global _settings_cache
    if _settings_cache is not None and not refresh:
        return _settings_cache

    stake_amount_type = database.get_setting("stake_amount_type")
    max_number_of_open_positions = database.get_setting("max_number_of_open_positions")
    tradable_balance_ratio = database.get_setting("tradable_balance_ratio")
    min_position_size = database.get_setting("min_position_size")
    trade_against = database.get_setting("trade_against")
    stop_loss = float(database.get_setting("stop_loss"))
    atr_trailing_enabled = database.get_setting("atr_trailing_enabled")
    atr_period = database.get_setting("atr_period")
    atr_multiplier = database.get_setting("atr_multiplier")
    atr_activation_pnl = database.get_setting("atr_activation_pnl")
    trade_top_performance = database.get_setting("trade_top_performance")
    btc_strategy = database.get_setting("btc_strategy")
    trade_against_switch = database.get_setting("trade_against_switch")
    take_profit_enabled = database.get_setting("take_profit_enabled")
    run_mode = database.get_setting("run_mode")
    lock_values = database.get_setting("lock_values")
    bot_prefix = database.get_setting("bot_prefix")
    telegram_routine_trade_logs = database.get_setting("telegram_routine_trade_logs")
    trade_against_switch_stablecoin = database.get_setting(
        "trade_against_switch_stablecoin"
    )
    delisting_start_date = database.get_setting("delisting_start_date")

    main_strategies_raw = database.get_setting("main_strategies")
    main_strategies = _parse_main_strategies(main_strategies_raw)
    # Persist the migrated list so future reads do not depend on the legacy value.
    normalized_main_strategies = json.dumps(main_strategies)
    if str(main_strategies_raw) != normalized_main_strategies:
        database.set_setting("main_strategies", normalized_main_strategies)

    main_strategy_configs = []
    strategy_module = importlib.import_module("bec.my_backtesting")
    for selected_strategy_id in main_strategies:
        df_selected_strategy = database.get_strategy_by_id(selected_strategy_id)
        if df_selected_strategy.empty:
            continue
        main_strategy_configs.append(
            {
                "id": selected_strategy_id,
                "name": str(df_selected_strategy.Name.values[0]),
                "backtest_optimize": bool(
                    df_selected_strategy.Backtest_Optimize.values[0]
                ),
                "strategy": (
                    strategy_module.resolve_strategy(selected_strategy_id)
                    if hasattr(strategy_module, "resolve_strategy")
                    else getattr(strategy_module, selected_strategy_id, None)
                ),
            }
        )

    btc_strategy_name = ""
    btc_strategy_backtest_optimize = False
    df_btc_strategy = database.get_strategy_by_id(btc_strategy)
    if not df_btc_strategy.empty:
        btc_strategy_name = str(df_btc_strategy.Name.values[0])
        btc_strategy_backtest_optimize = bool(
            df_btc_strategy.Backtest_Optimize.values[0]
        )

    # Dynamically get the strategy class
    btc_strategy_impl = (
        strategy_module.resolve_strategy(btc_strategy)
        if hasattr(strategy_module, "resolve_strategy")
        else getattr(strategy_module, btc_strategy, None)
    )

    if trade_against == "BTC":
        n_decimals = 8
    elif trade_against in ["USDT", "USDC"]:
        n_decimals = 2
    else:
        n_decimals = 2

    settings = AppSettings(
        stake_amount_type=stake_amount_type,
        max_number_of_open_positions=max_number_of_open_positions,
        tradable_balance_ratio=tradable_balance_ratio,
        min_position_size=min_position_size,
        trade_against=trade_against,
        stop_loss=stop_loss,
        atr_trailing_enabled=atr_trailing_enabled,
        atr_period=atr_period,
        atr_multiplier=atr_multiplier,
        atr_activation_pnl=atr_activation_pnl,
        trade_top_performance=trade_top_performance,
        main_strategies=main_strategies,
        main_strategy_configs=main_strategy_configs,
        btc_strategy=btc_strategy,
        btc_strategy_name=btc_strategy_name,
        btc_strategy_backtest_optimize=btc_strategy_backtest_optimize,
        trade_against_switch=trade_against_switch,
        take_profit_enabled=take_profit_enabled,
        run_mode=run_mode,
        lock_values=lock_values,
        bot_prefix=bot_prefix,
        telegram_routine_trade_logs=telegram_routine_trade_logs,
        trade_against_switch_stablecoin=trade_against_switch_stablecoin,
        delisting_start_date=delisting_start_date,
        btc_strategy_impl=btc_strategy_impl,
        n_decimals=n_decimals,
    )

    _settings_cache = settings
    return settings
