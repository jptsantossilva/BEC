import sys
import importlib
from dataclasses import dataclass

import pandas as pd

# from utils import telegram
# from utils import database
import utils.telegram as telegram
import utils.database as database

_settings_cache = None
_UPDATABLE_SETTING_KEYS = {
    "stake_amount_type",
    "max_number_of_open_positions",
    "tradable_balance_ratio",
    "min_position_size",
    "trade_against",
    "stop_loss",
    "trade_top_performance",
    "main_strategy",
    "btc_strategy",
    "trade_against_switch",
    "take_profit_1",
    "take_profit_1_amount",
    "take_profit_2",
    "take_profit_2_amount",
    "take_profit_3",
    "take_profit_3_amount",
    "take_profit_4",
    "take_profit_4_amount",
    "run_mode",
    "lock_values",
    "bot_prefix",
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
    trade_top_performance: int
    main_strategy: str
    main_strategy_name: str
    main_strategy_backtest_optimize: bool
    btc_strategy: str
    btc_strategy_name: str
    btc_strategy_backtest_optimize: bool
    trade_against_switch: bool
    take_profit_1: float
    take_profit_1_amount: float
    take_profit_2: float
    take_profit_2_amount: float
    take_profit_3: float
    take_profit_3_amount: float
    take_profit_4: float
    take_profit_4_amount: float
    run_mode: str
    lock_values: bool
    bot_prefix: str
    trade_against_switch_stablecoin: str
    delisting_start_date: str
    strategy_id: str
    strategy_name: str
    strategy_backtest_optimize: bool
    strategy: object
    btc_strategy_impl: object
    n_decimals: int


def _invalidate_settings_cache():
    global _settings_cache
    _settings_cache = None

def update_settings(patch: dict, *, refresh: bool = True) -> AppSettings:
    """Persist one or many settings and return updated settings snapshot."""
    if not patch:
        return load_settings(refresh=refresh)

    invalid = sorted([key for key in patch.keys() if key not in _UPDATABLE_SETTING_KEYS])
    if invalid:
        raise ValueError(f"Invalid setting keys: {', '.join(invalid)}")

    for name, value in patch.items():
        database.set_setting(name, value)

    _invalidate_settings_cache()
    return load_settings(refresh=refresh)

def update_setting(name: str, value, *, refresh: bool = True) -> AppSettings:
    """Persist one setting and return updated settings snapshot."""
    return update_settings({name: value}, refresh=refresh)

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
    telegram.send_telegram_message(telegram.telegramToken_errors, telegram.EMOJI_WARNING, msg)
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
    stop_loss = database.get_setting("stop_loss")
    trade_top_performance = database.get_setting("trade_top_performance")
    main_strategy = database.get_setting("main_strategy")
    btc_strategy = database.get_setting("btc_strategy")
    trade_against_switch = database.get_setting("trade_against_switch")
    take_profit_1 = database.get_setting("take_profit_1")
    take_profit_1_amount = database.get_setting("take_profit_1_amount")
    take_profit_2 = database.get_setting("take_profit_2")
    take_profit_2_amount = database.get_setting("take_profit_2_amount")
    take_profit_3 = database.get_setting("take_profit_3")
    take_profit_3_amount = database.get_setting("take_profit_3_amount")
    take_profit_4 = database.get_setting("take_profit_4")
    take_profit_4_amount = database.get_setting("take_profit_4_amount")
    run_mode = database.get_setting("run_mode")
    lock_values = database.get_setting("lock_values")
    bot_prefix = database.get_setting("bot_prefix")
    trade_against_switch_stablecoin = database.get_setting("trade_against_switch_stablecoin")
    delisting_start_date = database.get_setting("delisting_start_date")

    main_strategy_name = ""
    main_strategy_backtest_optimize = False
    df_main_strategy = database.get_strategy_by_id(main_strategy)
    if not df_main_strategy.empty:
        main_strategy_name = str(df_main_strategy.Name.values[0])
        main_strategy_backtest_optimize = bool(df_main_strategy.Backtest_Optimize.values[0])

    btc_strategy_name = ""
    btc_strategy_backtest_optimize = False
    df_btc_strategy = database.get_strategy_by_id(btc_strategy)
    if not df_btc_strategy.empty:
        btc_strategy_name = str(df_btc_strategy.Name.values[0])
        btc_strategy_backtest_optimize = bool(df_btc_strategy.Backtest_Optimize.values[0])

    # if trade_against == "USDT":
    #     strategy_id = main_strategy
    #     strategy_name = main_strategy_name
    #     strategy_backtest_optimize = main_strategy_backtest_optimize
    # elif trade_against == "BTC":
    #     strategy_id = btc_strategy
    #     strategy_name = btc_strategy_name
    #     strategy_backtest_optimize = btc_strategy_backtest_optimize

    strategy_id = main_strategy
    strategy_name = main_strategy_name
    strategy_backtest_optimize = main_strategy_backtest_optimize

    # Dynamically import the entire strategies module
    strategy_module = importlib.import_module('my_backtesting')
    # Dynamically get the strategy class
    strategy = getattr(strategy_module, strategy_id, None)
    btc_strategy_impl = getattr(strategy_module, btc_strategy, None)

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
        trade_top_performance=trade_top_performance,
        main_strategy=main_strategy,
        main_strategy_name=main_strategy_name,
        main_strategy_backtest_optimize=main_strategy_backtest_optimize,
        btc_strategy=btc_strategy,
        btc_strategy_name=btc_strategy_name,
        btc_strategy_backtest_optimize=btc_strategy_backtest_optimize,
        trade_against_switch=trade_against_switch,
        take_profit_1=take_profit_1,
        take_profit_1_amount=take_profit_1_amount,
        take_profit_2=take_profit_2,
        take_profit_2_amount=take_profit_2_amount,
        take_profit_3=take_profit_3,
        take_profit_3_amount=take_profit_3_amount,
        take_profit_4=take_profit_4,
        take_profit_4_amount=take_profit_4_amount,
        run_mode=run_mode,
        lock_values=lock_values,
        bot_prefix=bot_prefix,
        trade_against_switch_stablecoin=trade_against_switch_stablecoin,
        delisting_start_date=delisting_start_date,
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        strategy_backtest_optimize=strategy_backtest_optimize,
        strategy=strategy,
        btc_strategy_impl=btc_strategy_impl,
        n_decimals=n_decimals,
    )

    _settings_cache = settings
    return settings
