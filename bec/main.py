import logging
import json
import math
import sys
import time
import timeit
from dataclasses import dataclass
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import ccxt
from backtesting.lib import crossover

import bec.exchanges.service as binance
import bec.utils.config as config
import bec.utils.database as database
import bec.utils.telegram as telegram
from bec.utils import telegram_reporting
from bec.strategy_builder import engine as strategy_engine
from bec.ohlcv_policy import MIN_CLOSED_CANDLES
from bec.exchanges.ccxt_adapter import TransientPublicMarketDataError
from bec.utils.risk import get_runtime_risk_settings
from bec.utils.take_profit import normalize_take_profit_levels, take_profit_enabled

# sets the output display precision in terms of decimal places to 8.
# this is helpful when trading against BTC. The value in the dataframe has the precision 8 but when we display it
# by printing or sending to telegram only shows precision 6
pd.set_option("display.precision", 8)

# log file to store error messages
log_filename = "main.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p -",
)


# Global Vars
telegram_token = telegram.telegram_token_main

# sl = single line message; ml = multi line message
telegram_prefix_sl = ""
telegram_prefix_ml = ""

MIN_LIVE_CLOSED_CANDLES = MIN_CLOSED_CANDLES

# strategy
# strategy_name = ''


def read_arguments():
    # total arguments
    n = len(sys.argv)

    if n < 2:
        print("Argument is missing")
        time_frame = input("Enter time frame (15m, 1d, 4h or 1h):")
        # run_mode = input('Enter run mode (test, prod):')
    else:
        # argv[0] in Python is always the name of the script.
        time_frame = sys.argv[1]

        # run modes
        # test - does not execute orders on the exchange
        # prod - execute orders on the exchange
        # run_mode = sys.argv[2]

    return time_frame  # , run_mode


def apply_arguments(time_frame):

    global telegram_token, telegram_prefix_ml, telegram_prefix_sl

    if time_frame == "15m":
        telegram_prefix_sl = telegram.telegram_prefix_bot_1h_sl
        telegram_prefix_ml = telegram.telegram_prefix_bot_1h_ml
    elif time_frame == "1h":
        telegram_prefix_sl = telegram.telegram_prefix_bot_1h_sl
        telegram_prefix_ml = telegram.telegram_prefix_bot_1h_ml
    elif time_frame == "4h":
        telegram_prefix_sl = telegram.telegram_prefix_bot_4h_sl
        telegram_prefix_ml = telegram.telegram_prefix_bot_4h_ml
    elif time_frame == "1d":
        telegram_prefix_sl = telegram.telegram_prefix_bot_1d_sl
        telegram_prefix_ml = telegram.telegram_prefix_bot_1d_ml
    else:
        # Invalid timeframe
        raise ValueError("Incorrect time frame. Use one of: 15m, 1h, 4h, 1d")


def get_data(
    symbol,
    timeframe,
    required_closed_candles=MIN_LIVE_CLOSED_CANDLES,
):
    """Load the latest closed candles needed for one live evaluation."""
    df = binance.get_recent_ohlcv(
        symbol=symbol,
        interval=timeframe,
        required_closed_candles=int(required_closed_candles),
        keep_time_col=False,
    )
    if df.empty:
        msg = (
            f"No closed OHLCV candles returned for {symbol} ({timeframe}); "
            "the symbol will be skipped."
        )
        print(msg)
        return pd.DataFrame()
    return df


class _TradeCycleMarketData:
    """One-run OHLCV cache and failure collector for fail-closed trading."""

    def __init__(
        self,
        requirements: dict[tuple[str, str], int],
        *,
        frames: dict[tuple[str, str], pd.DataFrame] | None = None,
        failures: dict[tuple[str, str], dict] | None = None,
        allow_network: bool = True,
    ):
        self.requirements = dict(requirements)
        self.cache: dict[tuple[str, str], pd.DataFrame] = {
            self._key(*key): frame.copy()
            for key, frame in (frames or {}).items()
        }
        self.failures: dict[tuple[str, str], dict] = {
            self._key(*key): dict(failure)
            for key, failure in (failures or {}).items()
        }
        self.allow_network = bool(allow_network)

    @staticmethod
    def _key(symbol: str, timeframe: str) -> tuple[str, str]:
        return str(symbol).strip().upper(), str(timeframe).strip()

    def _record_failure(
        self,
        key: tuple[str, str],
        *,
        reason: str,
        required: int,
        available: int = 0,
        error_type: str = "",
    ) -> None:
        self.failures.setdefault(
            key,
            {
                "reason": reason,
                "required": int(required),
                "available": int(available),
                "error_type": str(error_type),
            },
        )

    def load(self, symbol: str, timeframe: str) -> pd.DataFrame:
        key = self._key(symbol, timeframe)
        if key in self.cache:
            return self.cache[key].copy()

        required = max(
            int(self.requirements.get(key, MIN_LIVE_CLOSED_CANDLES)),
            MIN_LIVE_CLOSED_CANDLES,
        )
        if not self.allow_network:
            self._record_failure(
                key,
                reason="OHLCV requirement was not prefetched",
                required=required,
            )
            frame = pd.DataFrame()
            self.cache[key] = frame
            return frame.copy()
        try:
            frame = get_data(
                symbol=key[0],
                timeframe=key[1],
                required_closed_candles=required,
            )
        except TransientPublicMarketDataError as exc:
            logging.warning(
                "Transient OHLCV failure for %s %s: %s",
                key[0],
                key[1],
                type(exc).__name__,
            )
            self._record_failure(
                key,
                reason="transient public market-data failure after retries",
                required=required,
                error_type=type(exc).__name__,
            )
            frame = pd.DataFrame()
        except (ccxt.BadSymbol, ccxt.BadRequest, ValueError) as exc:
            logging.warning(
                "Permanent OHLCV request failure for %s %s: %s",
                key[0],
                key[1],
                type(exc).__name__,
            )
            self._record_failure(
                key,
                reason="public market-data request was rejected",
                required=required,
                error_type=type(exc).__name__,
            )
            frame = pd.DataFrame()

        if frame.empty and key not in self.failures:
            self._record_failure(
                key,
                reason="exchange returned no closed candles",
                required=required,
            )
        elif not frame.empty and len(frame) < required:
            self._record_failure(
                key,
                reason="insufficient closed-candle history",
                required=required,
                available=len(frame),
            )
            frame = pd.DataFrame()

        self.cache[key] = frame.copy()
        return frame.copy()

    def send_failure_summary(self, timeframe: str) -> None:
        if not self.failures:
            return
        grouped: dict[str, list[str]] = {}
        for (symbol, failed_timeframe), failure in sorted(self.failures.items()):
            detail = f"{symbol} ({failed_timeframe})"
            if failure["available"]:
                detail += (
                    f" {failure['available']}/{failure['required']} candles"
                )
            if failure["error_type"]:
                detail += f" [{failure['error_type']}]"
            grouped.setdefault(failure["reason"], []).append(detail)
        lines = []
        for reason, markets in grouped.items():
            shown = markets[:25]
            suffix = f" (+{len(markets) - 25} more)" if len(markets) > 25 else ""
            lines.append(f"{reason}: {', '.join(shown)}{suffix}")
        telegram.send_error_event(
            action="load trade-cycle OHLCV",
            timeframe=timeframe,
            reason="\n".join(lines),
            impact="Affected symbols were skipped before any buy or sell order decision.",
            next_step="Review the active-exchange public market-data logs before the next cycle.",
            notify_main=False,
        )


def calculate_atr(df: pd.DataFrame, period: int) -> float:
    if period <= 0 or df.empty:
        return 0.0
    if not {"High", "Low", "Close"}.issubset(df.columns):
        return 0.0

    prev_close = df["Close"].shift(1)
    tr = pd.concat(
        [
            (df["High"] - df["Low"]).abs(),
            (df["High"] - prev_close).abs(),
            (df["Low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr_series = tr.rolling(period, min_periods=period).mean()
    if atr_series.empty or pd.isna(atr_series.iloc[-1]):
        return 0.0
    return float(atr_series.iloc[-1])


# calc current pnl
def get_current_pnl(symbol, current_price, timeframe, position_id=None):

    try:
        # get buy price
        if position_id is not None:
            df_buy_price = database.get_position_by_id(position_id)
        else:
            df_buy_price = database.get_positions_by_bot_symbol_position(
                bot=timeframe, symbol=symbol, position=1
            )
        buy_price = 0
        pnl_perc = 0

        if not df_buy_price.empty:
            # get buy price
            buy_price = df_buy_price["Buy_Price"].iloc[0]
            # check if buy price is fulfilled
            if not math.isnan(buy_price) and buy_price > 0:
                # calc pnl percentage
                pnl_perc = ((current_price - buy_price) / buy_price) * 100
                pnl_perc = round(pnl_perc, 2)

        return pnl_perc

    except Exception as e:
        msg = sys._getframe().f_code.co_name + " - " + repr(e)
        msg = telegram_prefix_sl + msg
        print(msg)
        telegram.send_error_event(
            action="calculate current PnL",
            symbol=symbol,
            timeframe=timeframe,
            reason="Unexpected exception",
            impact="PnL-dependent exit checks may be incomplete for this symbol.",
            next_step="Check position data and logs.",
            exception=e,
            main_token=telegram_token,
            main_prefix=telegram_prefix_sl,
        )


def get_strategy_display_name(strategy_id: str) -> str:
    strategy_name = database.get_strategy_name(strategy_id)
    return strategy_name or strategy_id


def get_default_main_strategy_id(settings) -> str:
    return settings.main_strategies[0] if settings.main_strategies else ""


def _active_main_strategy_ids(settings) -> set[str]:
    return {
        str(strategy_id).strip()
        for strategy_id in getattr(settings, "main_strategies", [])
        if str(strategy_id).strip()
    }


def _filter_buy_candidates_for_active_strategies(df_buy: pd.DataFrame, settings) -> pd.DataFrame:
    if df_buy.empty:
        return df_buy

    active_strategy_ids = _active_main_strategy_ids(settings)
    if not active_strategy_ids:
        return df_buy.iloc[0:0].copy()

    strategy_ids = (
        df_buy.get("Strategy_Id", pd.Series([""] * len(df_buy), index=df_buy.index))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    return df_buy[strategy_ids.isin(active_strategy_ids)].copy()


def strategy_uses_tuned_parameters(strategy_id: str) -> bool:
    try:
        definition = database.get_strategy_definition(strategy_id)
    except Exception:
        return False
    return bool(_optimizable_parameter_names(definition))


def strategy_uses_declarative_engine(strategy_id: str) -> bool:
    try:
        definition = database.get_strategy_definition(strategy_id)
    except Exception:
        return False
    return isinstance(definition, dict) and definition.get("engine") == "bec_strategy_ast_v2"


def _safe_int_or_zero(value) -> int:
    try:
        if pd.isna(value):
            return 0
    except TypeError:
        pass
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _strategy_parameter_names(definition: dict) -> list[str]:
    parameters = definition.get("parameters", {}) if isinstance(definition, dict) else {}
    return list(parameters.keys()) if isinstance(parameters, dict) else []


def _optimizable_parameter_names(definition: dict) -> list[str]:
    parameters = definition.get("parameters", {}) if isinstance(definition, dict) else {}
    if not isinstance(parameters, dict):
        return []
    return [name for name, spec in parameters.items() if isinstance(spec, dict) and bool(spec.get("optimizable", False))]


def _primary_parameter_pair_names(definition: dict) -> tuple[str, str]:
    names = _strategy_parameter_names(definition)
    fast_name = next((name for name in names if str(name).lower() == "fast" or "fast" in str(name).lower()), "")
    slow_name = next((name for name in names if str(name).lower() == "slow" or "slow" in str(name).lower()), "")
    if fast_name and slow_name:
        return fast_name, slow_name

    optimizable = _optimizable_parameter_names(definition)
    if len(optimizable) >= 2:
        return optimizable[0], optimizable[1]
    if len(names) >= 2:
        return names[0], names[1]
    return "", ""


def _snapshot_parameters(pos_row=None) -> dict:
    if pos_row is None:
        return {}
    snapshot = database.parse_strategy_params(pos_row.get("Strategy_Params_JSON", ""))
    if not isinstance(snapshot, dict):
        return {}
    parameters = snapshot.get("parameters")
    if isinstance(parameters, dict):
        return dict(parameters)
    return {
        key: value
        for key, value in snapshot.items()
        if key not in {"engine", "definition", "risk", "entry_state"}
    }


def _snapshot_definition(pos_row=None) -> dict:
    if pos_row is None:
        return {}
    snapshot = database.parse_strategy_params(pos_row.get("Strategy_Params_JSON", ""))
    definition = snapshot.get("definition") if isinstance(snapshot, dict) else None
    return definition if isinstance(definition, dict) else {}


def _snapshot_entry_state(pos_row=None) -> dict:
    if pos_row is None:
        return {}
    snapshot = database.parse_strategy_params(pos_row.get("Strategy_Params_JSON", ""))
    entry_state = snapshot.get("entry_state") if isinstance(snapshot, dict) else None
    return dict(entry_state) if isinstance(entry_state, dict) else {}


def _build_live_strategy_snapshot(strategy_id: str, definition: dict, parameters: dict, entry_state: dict | None = None) -> str:
    if not isinstance(definition, dict) or definition.get("engine") != "bec_strategy_ast_v2":
        return database.build_strategy_params_json(strategy_id)
    snapshot = {
        "engine": definition.get("engine", "bec_strategy_ast_v2"),
        "definition": definition,
        "parameters": strategy_engine.resolve_parameters(definition, parameters),
        "risk": database.get_strategy_risk(strategy_id),
    }
    if entry_state:
        snapshot["entry_state"] = dict(entry_state)
    return json.dumps(snapshot, separators=(",", ":"))


def _apply_pair_to_parameters(definition: dict, parameters: dict, first_value=0, second_value=0, *, overwrite: bool = True) -> dict:
    first_name, second_name = _primary_parameter_pair_names(definition)
    result = dict(parameters)
    first = _safe_int_or_zero(first_value)
    second = _safe_int_or_zero(second_value)
    if first_name and first and (overwrite or first_name not in result):
        result[first_name] = first
    if second_name and second and (overwrite or second_name not in result):
        result[second_name] = second
    return result


def _backtesting_parameter_overrides(strategy_id: str, symbol: str, timeframe: str, definition: dict) -> tuple[dict, int, int]:
    df = database.get_backtesting_results_by_symbol_timeframe_strategy(
        symbol=symbol,
        time_frame=timeframe,
        strategy_id=strategy_id,
    )
    if df.empty:
        return {}, 0, 0

    row = df.iloc[0]
    config = database.parse_strategy_params(row.get("Backtest_Config_JSON", ""))
    strategy_parameters = config.get("strategy_parameters") if isinstance(config, dict) else {}
    parameters = strategy_parameters.get("parameters") if isinstance(strategy_parameters, dict) else {}
    if isinstance(parameters, dict) and parameters:
        first_name, second_name = _primary_parameter_pair_names(definition)
        return (
            dict(parameters),
            _safe_int_or_zero(parameters.get(first_name, 0)),
            _safe_int_or_zero(parameters.get(second_name, 0)),
        )

    return {}, 0, 0


def _position_has_strategy_params_snapshot(pos_row) -> bool:
    snapshot = database.parse_strategy_params(pos_row.get("Strategy_Params_JSON", ""))
    parameters = snapshot.get("parameters") if isinstance(snapshot, dict) else None
    return isinstance(parameters, dict) and bool(parameters)


def _has_approved_backtest(strategy_id: str, symbol: str, timeframe: str) -> bool:
    return database.is_active_backtest_approved_for_live_buy(
        symbol=symbol,
        time_frame=timeframe,
        strategy_id=strategy_id,
    )


def get_strategy_parameter_overrides(strategy_id: str, first_value=0, second_value=0, pos_row=None) -> dict:
    try:
        definition = database.get_strategy_definition(strategy_id)
    except Exception:
        definition = {}
    overrides = _snapshot_parameters(pos_row)
    return _apply_pair_to_parameters(definition, overrides, first_value, second_value)


def get_strategy_runtime_context(
    strategy_id: str,
    symbol: str,
    timeframe: str,
    pos_row=None,
    *,
    prefer_position_params: bool = False,
) -> dict:
    definition = _snapshot_definition(pos_row)
    if not definition:
        try:
            definition = database.get_strategy_definition(strategy_id)
        except Exception:
            definition = {}

    declarative = isinstance(definition, dict) and definition.get("engine") == "bec_strategy_ast_v2"
    defaults = strategy_engine.resolve_parameters(definition) if declarative else {}
    snapshot = _snapshot_parameters(pos_row)
    backtesting_parameters, bt_first, bt_second = _backtesting_parameter_overrides(
        strategy_id,
        symbol,
        timeframe,
        definition,
    )

    parameters = dict(defaults)
    if prefer_position_params:
        parameters.update(backtesting_parameters)
        parameters.update(snapshot)
    else:
        parameters.update(snapshot)
        parameters.update(backtesting_parameters)

    first_name, second_name = _primary_parameter_pair_names(definition)
    first_value = _safe_int_or_zero(parameters.get(first_name, bt_first))
    second_value = _safe_int_or_zero(parameters.get(second_name, bt_second))

    return {
        "definition": definition,
        "declarative": declarative,
        "parameters": parameters,
        "primary_parameter_names": (first_name, second_name),
        "order_first": first_value,
        "order_second": second_value,
        "setup": "" if first_value == 0 or second_value == 0 else f"{first_value}/{second_value}",
    }


def get_strategy_parameters(strategy_id: str, symbol: str, timeframe: str, pos_row=None, prefer_position_params: bool = False):
    context = get_strategy_runtime_context(
        strategy_id,
        symbol,
        timeframe,
        pos_row=pos_row,
        prefer_position_params=prefer_position_params,
    )
    return context["order_first"], context["order_second"]


def apply_strategy_technicals(df, strategy_id: str, first_value: int = 0, second_value: int = 0, symbol: str = "", timeframe: str = "current", parameters: dict | None = None, data_loader=None):
    if strategy_uses_declarative_engine(strategy_id):
        definition = database.get_strategy_definition(strategy_id)
        parameters = parameters or get_strategy_parameter_overrides(strategy_id, first_value, second_value)
        prepared = strategy_engine.add_indicators(
            df,
            definition,
            parameters,
            symbol=symbol,
            base_timeframe=timeframe,
            data_loader=data_loader or get_data,
        )
        for column in prepared.columns:
            if column not in df.columns:
                df[column] = prepared[column].to_numpy()
        return
    return


def get_strategy_sell_condition(strategy_id: str, symbol: str, timeframe: str, df, lastrow, parameters: dict | None = None, pos_row=None, data_loader=None):
    if strategy_uses_declarative_engine(strategy_id):
        definition = database.get_strategy_definition(strategy_id)
        parameters = parameters or get_strategy_parameter_overrides(strategy_id)
        return strategy_engine.evaluate_exit(
            df,
            definition,
            parameters,
            symbol=symbol,
            base_timeframe=timeframe,
            data_loader=data_loader or get_data,
            context={"entry_state": _snapshot_entry_state(pos_row)},
        )
    return False


def get_strategy_buy_condition(strategy_id: str, symbol: str, timeframe: str, df, lastrow, parameters: dict | None = None, data_loader=None):
    detail = ""
    if strategy_uses_declarative_engine(strategy_id):
        definition = database.get_strategy_definition(strategy_id)
        parameters = parameters or get_strategy_parameter_overrides(strategy_id)
        entry_ok = strategy_engine.evaluate_entry(
            df,
            definition,
            parameters,
            symbol=symbol,
            base_timeframe=timeframe,
            data_loader=data_loader or get_data,
        )
        return entry_ok, f"Entry: {entry_ok}"
    return False, detail


def _lastrow_float(lastrow, column: str) -> float | None:
    if column not in lastrow.index:
        return None
    try:
        value = float(lastrow[column])
    except (TypeError, ValueError):
        return None
    if pd.isna(value):
        return None
    return value


def _new_trade_cycle_summary(timeframe: str, routine_log_mode: str) -> dict:
    return {
        "timeframe": timeframe,
        "routine_log_mode": str(routine_log_mode or "summary").strip().lower(),
        "sell_evaluated": 0,
        "buy_evaluated": 0,
        "sell_no_action": 0,
        "buy_no_action": 0,
        "sell_actions": 0,
        "buy_actions": 0,
        "take_profit_actions": 0,
        "missing_parameters": 0,
        "empty_data": 0,
        "warnings": 0,
        "examples": [],
    }


def _sell_outcome_action_taken(outcome) -> bool:
    """Read the canonical sell outcome while tolerating isolated legacy mocks."""
    if outcome is None:
        return True
    return bool(getattr(outcome, "action_taken", False))


def _sell_outcome_is_full_exit(outcome, requested_percentage: float) -> bool:
    return float(
        getattr(outcome, "effective_percentage", requested_percentage)
    ) >= 100.0


def _track_trade_cycle_example(summary: dict, msg: str):
    if len(summary["examples"]) < 5:
        summary["examples"].append(msg)


def _send_trade_cycle_summary(summary: dict):
    msg = telegram_prefix_sl + format_trade_cycle_summary(summary)
    print(msg)
    telegram.send_telegram_message(telegram_token, "", msg)


def _live_history_requirements(
    df_sell: pd.DataFrame,
    df_buy: pd.DataFrame,
    timeframe: str,
    settings,
) -> dict[tuple[str, str], int]:
    """Resolve the maximum per-market warm-up needed by this trade cycle."""
    requirements: dict[tuple[str, str], int] = {}
    row_sets = ((df_sell, True), (df_buy, False))
    for rows, prefer_position_params in row_sets:
        for _, pos_row in rows.iterrows():
            symbol = str(pos_row["Symbol"]).strip().upper()
            strategy_id = str(
                pos_row.get("Strategy_Id")
                or get_default_main_strategy_id(settings)
            )
            context = get_strategy_runtime_context(
                strategy_id,
                symbol,
                timeframe,
                pos_row=pos_row,
                prefer_position_params=prefer_position_params,
            )
            definition = context.get("definition")
            try:
                runtime_definition = database.get_strategy_definition(strategy_id)
            except Exception:
                runtime_definition = None
            if isinstance(runtime_definition, dict) and runtime_definition:
                definition = runtime_definition
            declarative = (
                isinstance(definition, dict)
                and definition.get("engine") == "bec_strategy_ast_v2"
            )
            if declarative:
                risk_settings = get_runtime_risk_settings(
                    settings, strategy_id, pos_row=pos_row
                )
                strategy_requirements = (
                    strategy_engine.required_closed_candles_by_timeframe(
                        definition,
                        context.get("parameters"),
                        timeframe,
                        risk_settings,
                    )
                )
            else:
                strategy_requirements = {timeframe: MIN_LIVE_CLOSED_CANDLES}
            for required_timeframe, candles in strategy_requirements.items():
                key = symbol, str(required_timeframe)
                requirements[key] = max(
                    requirements.get(key, 0),
                    int(candles),
                    MIN_LIVE_CLOSED_CANDLES,
                )
    return requirements


@dataclass
class _PreparedTradeCycle:
    timeframe: str
    df_sell: pd.DataFrame
    df_buy: pd.DataFrame
    requirements: dict[tuple[str, str], int]
    summary: dict


def prepare_trade_cycle(timeframe: str, settings) -> _PreparedTradeCycle:
    """Resolve one timeframe's candidates and current strategy requirements."""
    routine_log_mode = getattr(settings, "telegram_routine_trade_logs", "summary")
    df_sell = database.get_positions_by_bot_position(bot=timeframe, position=1)
    df_buy = database.get_positions_by_bot_position(bot=timeframe, position=0)
    if not database.is_trade_main_timeframe_enabled(timeframe):
        df_buy = df_buy.iloc[0:0].copy()
    else:
        df_buy = _filter_buy_candidates_for_active_strategies(df_buy, settings)
    return _PreparedTradeCycle(
        timeframe=timeframe,
        df_sell=df_sell,
        df_buy=df_buy,
        requirements=_live_history_requirements(
            df_sell,
            df_buy,
            timeframe,
            settings,
        ),
        summary=_new_trade_cycle_summary(timeframe, routine_log_mode),
    )


def format_trade_cycle_summary(summary: dict) -> str:
    lines = [
        "Cycle",
        f"Sell checked: {summary['sell_evaluated']} | actions: {summary['sell_actions']}",
        f"Buy checked: {summary['buy_evaluated']} | actions: {summary['buy_actions']}",
        f"No action: sell {summary['sell_no_action']}, buy {summary['buy_no_action']}",
        f"Skipped: {summary['missing_parameters'] + summary['empty_data']} | warnings: {summary['warnings']}",
    ]
    if summary["take_profit_actions"] > 0:
        lines.append(f"Take profits: {summary['take_profit_actions']}")
    if summary["warnings"] > 0:
        lines.append("Warning detail: See Errors channel.")
    if summary["examples"] and summary["routine_log_mode"] == "detailed":
        lines.append("Examples without action:")
        lines.extend(summary["examples"])
    return "\n".join(lines)


def trade(
    timeframe,
    run_mode,
    settings=None,
    send_summary=True,
    *,
    prepared_cycle: _PreparedTradeCycle | None = None,
    market_data: _TradeCycleMarketData | None = None,
    execute_sells: bool = True,
    execute_buys: bool = True,
    finalize_market_data: bool = True,
):
    del run_mode  # Kept for compatibility with older callers; exchange order functions enforce settings.run_mode.
    if settings is None:
        settings = config.load_settings(refresh=True)
    if prepared_cycle is None:
        # Preserve the standalone main.py behavior.  Batch execution performs
        # this cleanup once before preparing all due timeframes.
        database.delete_positions_not_top_rank()
        prepared_cycle = prepare_trade_cycle(timeframe, settings)
    cycle_summary = prepared_cycle.summary
    send_detailed_routine_logs = cycle_summary["routine_log_mode"] == "detailed"

    df_sell = (
        prepared_cycle.df_sell
        if execute_sells
        else prepared_cycle.df_sell.iloc[0:0].copy()
    )
    df_buy = (
        prepared_cycle.df_buy
        if execute_buys
        else prepared_cycle.df_buy.iloc[0:0].copy()
    )
    if market_data is None:
        market_data = _TradeCycleMarketData(prepared_cycle.requirements)

    # check open positions and SELL if conditions are fulfilled
    for _, pos_row in df_sell.iterrows():
        cycle_summary["sell_evaluated"] += 1
        symbol = str(pos_row["Symbol"])
        position_id = int(pos_row["Id"])
        strategy_id = str(pos_row.get("Strategy_Id") or get_default_main_strategy_id(settings))
        strategy_name = str(pos_row.get("Strategy_Name") or get_strategy_display_name(strategy_id))
        strategy_context = get_strategy_runtime_context(
            strategy_id,
            symbol,
            timeframe,
            pos_row=pos_row,
            prefer_position_params=True,
        )
        strategy_parameters = strategy_context["parameters"]
        order_first = strategy_context["order_first"]
        order_second = strategy_context["order_second"]

        # get latest price data
        df = market_data.load(symbol=symbol, timeframe=timeframe)

        # Check if df is empty
        if df.empty:
            cycle_summary["empty_data"] += 1
            continue  # Skip to the next iteration

        apply_strategy_technicals(
            df,
            strategy_id,
            order_first,
            order_second,
            symbol=symbol,
            timeframe=timeframe,
            parameters=strategy_parameters,
            data_loader=market_data.load,
        )

        # last row
        lastrow = df.iloc[-1]

        # Current price
        current_price = lastrow.Close

        # Current PnL
        current_pnl = get_current_pnl(symbol, current_price, timeframe, position_id=position_id)

        buy_price_raw = pos_row.get("Buy_Price", 0)
        highest_raw = pos_row.get("Highest_Price_Since_Entry", 0)
        trail_stop_raw = pos_row.get("Trail_Stop_ATR", 0)

        buy_price = 0.0 if pd.isna(buy_price_raw) else float(buy_price_raw)
        existing_highest = 0.0 if pd.isna(highest_raw) else float(highest_raw)
        existing_trail_stop = 0.0 if pd.isna(trail_stop_raw) else float(trail_stop_raw)

        risk_settings = get_runtime_risk_settings(settings, strategy_id, pos_row=pos_row)
        atr_trailing_enabled = bool(risk_settings["atr_trailing_enabled"])
        atr_period = int(risk_settings["atr_period"])
        atr_multiplier = float(risk_settings["atr_multiplier"])
        atr_activation_pnl = float(risk_settings["atr_activation_pnl"])
        atr_value = calculate_atr(df, period=atr_period)

        base_highest = existing_highest if existing_highest > 0 else buy_price
        highest_price_since_entry = max(base_highest, float(current_price))

        hard_stop_price = (
            float(buy_price) * (1 - (float(risk_settings["stop_loss"]) / 100))
            if float(risk_settings["stop_loss"]) > 0 and float(buy_price) > 0
            else 0.0
        )

        trailing_started = existing_trail_stop > 0
        can_enable_trailing = (
            atr_trailing_enabled
            and atr_multiplier > 0
            and atr_value > 0
            and (trailing_started or current_pnl >= atr_activation_pnl)
        )

        trail_stop_atr = existing_trail_stop if can_enable_trailing else 0.0
        if can_enable_trailing:
            calc_trail_stop = highest_price_since_entry - (atr_multiplier * atr_value)
            calc_trail_stop = max(0.0, float(calc_trail_stop))
            trail_stop_atr = max(float(trail_stop_atr), float(calc_trail_stop))

        active_stop_price = hard_stop_price
        if trail_stop_atr > 0:
            active_stop_price = max(float(active_stop_price), float(trail_stop_atr))

        database.update_position_risk(
            bot=timeframe,
            symbol=symbol,
            highest_price_since_entry=highest_price_since_entry,
            trail_stop_atr=trail_stop_atr,
            position_id=position_id,
        )

        # if using stop loss (hard SL and/or ATR trailing stop)
        sell_stop_loss = False
        stop_loss_reason = f"Stop loss {risk_settings['stop_loss']}%"
        if active_stop_price > 0:
            sell_stop_loss = float(current_price) <= float(active_stop_price)
            if sell_stop_loss and trail_stop_atr > 0 and abs(active_stop_price - trail_stop_atr) < 1e-12:
                stop_loss_reason = (
                    f"ATR Trailing Stop - ATR({atr_period}) x {atr_multiplier:.2f} "
                    f"| Trigger: {active_stop_price:.8f}"
                )

        take_profit_enabled = bool(risk_settings["take_profit_enabled"])
        executed_tp_levels = database.get_position_executed_take_profit_levels(position_id=position_id)
        tp_steps = []
        if take_profit_enabled:
            for tp in normalize_take_profit_levels(risk_settings.get("take_profits", [])):
                tp_num = int(tp.get("level", 0) or 0)
                tp_pnl = float(tp.get("pnl_pct", 0.0) or 0.0)
                tp_amount = float(tp.get("amount_pct", 0.0) or 0.0)
                if tp_num <= 0 or tp_pnl <= 0 or tp_amount <= 0 or tp_num in executed_tp_levels:
                    continue
                tp_steps.append((tp_num, current_pnl >= tp_pnl, tp_pnl, tp_amount))

        # check sell condition for the strategy
        sell_condition = get_strategy_sell_condition(
            strategy_id,
            symbol,
            timeframe,
            df,
            lastrow,
            parameters=strategy_parameters,
            pos_row=pos_row,
            data_loader=market_data.load,
        )

        # set current PnL
        current_price = lastrow.Close
        database.update_position_pnl(
            bot=timeframe, symbol=symbol, curr_price=current_price, position_id=position_id
        )

        # Execute exactly one sell action by priority
        if (
            sell_stop_loss
            or sell_condition
            or any(tp_flag for _tp_num, tp_flag, _tp_pnl, _tp_amount in tp_steps)
        ):

            # stop loss
            if sell_stop_loss:
                outcome = binance.create_sell_order(
                    symbol=symbol,
                    bot=timeframe,
                    strategy_id=strategy_id,
                    strategy_name=strategy_name,
                    position_id=position_id,
                    reason=stop_loss_reason,
                )
                if _sell_outcome_action_taken(outcome):
                    cycle_summary["sell_actions"] += 1
                else:
                    cycle_summary["sell_no_action"] += 1
                continue

            # sell_codition
            if sell_condition:
                outcome = binance.create_sell_order(
                    symbol=symbol,
                    bot=timeframe,
                    strategy_id=strategy_id,
                    strategy_name=strategy_name,
                    position_id=position_id,
                )
                if _sell_outcome_action_taken(outcome):
                    cycle_summary["sell_actions"] += 1
                else:
                    cycle_summary["sell_no_action"] += 1
                continue

            # Take-Profits in cascade.
            # Each 'percentage' is applied to the remaining open position.
            # If any TP sells 100% of the position, stop immediately (skip further TPs).
            tp_executed = False
            for tp_num, tp_flag, tp_pnl, tp_amount in tp_steps:
                if not tp_flag:
                    continue
                if tp_amount <= 0.0:
                    continue

                outcome = binance.create_sell_order(
                    symbol=symbol,
                    bot=timeframe,
                    strategy_id=strategy_id,
                    strategy_name=strategy_name,
                    position_id=position_id,
                    reason=f"Take-Profit Level {tp_num} - {tp_pnl}% PnL - {tp_amount}% Amount",
                    percentage=tp_amount,
                    take_profit_num=tp_num,
                )

                if not _sell_outcome_action_taken(outcome):
                    cycle_summary["sell_no_action"] += 1
                    continue
                tp_executed = True
                cycle_summary["sell_actions"] += 1
                cycle_summary["take_profit_actions"] += 1
                # If this level sold 100%, there's no remaining position; stop chaining TPs
                if _sell_outcome_is_full_exit(outcome, tp_amount):
                    break

            # If any TP executed, skip the rest of this loop iteration (avoid redundant actions)
            if tp_executed:
                continue

        else:
            setup = strategy_context["setup"]
            msg = (
                f"{symbol} - {setup} {strategy_name} - Sell condition not fulfilled"
            )
            msg = telegram_prefix_sl + msg
            print(msg)
            cycle_summary["sell_no_action"] += 1
            _track_trade_cycle_example(cycle_summary, msg)
            if send_detailed_routine_logs:
                telegram.send_telegram_message(telegram_token, "", msg)

    # check symbols not in positions and BUY if conditions are fulfilled
    for _, pos_row in df_buy.iterrows():
        cycle_summary["buy_evaluated"] += 1
        symbol = str(pos_row["Symbol"])
        position_id = int(pos_row["Id"])
        strategy_id = str(pos_row.get("Strategy_Id") or get_default_main_strategy_id(settings))
        strategy_name = str(pos_row.get("Strategy_Name") or get_strategy_display_name(strategy_id))

        if not _position_has_strategy_params_snapshot(pos_row):
            msg = f"{symbol} - {strategy_name} - Buy skipped: missing strategy parameters snapshot"
            msg = telegram_prefix_sl + msg
            print(msg)
            cycle_summary["buy_no_action"] += 1
            _track_trade_cycle_example(cycle_summary, msg)
            if send_detailed_routine_logs:
                telegram.send_telegram_message(telegram_token, "", msg)
            continue

        if not _has_approved_backtest(strategy_id, symbol, timeframe):
            msg = f"{symbol} - {strategy_name} - Buy skipped: missing approved backtest"
            msg = telegram_prefix_sl + msg
            print(msg)
            cycle_summary["buy_no_action"] += 1
            _track_trade_cycle_example(cycle_summary, msg)
            if send_detailed_routine_logs:
                telegram.send_telegram_message(telegram_token, "", msg)
            continue

        strategy_context = get_strategy_runtime_context(
            strategy_id,
            symbol,
            timeframe,
            pos_row=pos_row,
        )
        strategy_parameters = strategy_context["parameters"]
        order_first = strategy_context["order_first"]
        order_second = strategy_context["order_second"]

        df = market_data.load(symbol=symbol, timeframe=timeframe)

        # skip if no data
        if df.empty:
            cycle_summary["empty_data"] += 1
            continue

        apply_strategy_technicals(
            df,
            strategy_id,
            order_first,
            order_second,
            symbol=symbol,
            timeframe=timeframe,
            parameters=strategy_parameters,
            data_loader=market_data.load,
        )

        # last row
        lastrow = df.iloc[-1]

        buy_condition, buy_detail = get_strategy_buy_condition(
            strategy_id,
            symbol,
            timeframe,
            df,
            lastrow,
            parameters=strategy_parameters,
            data_loader=market_data.load,
        )

        if buy_condition:
            strategy_definition = database.get_strategy_definition(strategy_id)
            entry_state = {}
            if isinstance(strategy_definition, dict) and strategy_definition.get("engine") == "bec_strategy_ast_v2":
                entry_state = strategy_engine.capture_entry_state(
                    df,
                    strategy_definition,
                    strategy_parameters,
                    symbol=symbol,
                    base_timeframe=timeframe,
                    data_loader=market_data.load,
                )
            strategy_params_json = _build_live_strategy_snapshot(
                strategy_id,
                strategy_definition,
                strategy_parameters,
                entry_state,
            )
            cycle_summary["buy_actions"] += 1
            binance.create_buy_order(
                symbol=symbol,
                bot=timeframe,
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                position_id=position_id,
                strategy_params_json=strategy_params_json,
            )
        else:
            setup = strategy_context["setup"]
            detail = f" | {buy_detail}" if buy_detail else ""
            msg = f"{symbol} - {setup} {strategy_name} - Buy condition not fulfilled{detail}"
            msg = telegram_prefix_sl + msg
            print(msg)
            cycle_summary["buy_no_action"] += 1
            _track_trade_cycle_example(cycle_summary, msg)
            if send_detailed_routine_logs:
                telegram.send_telegram_message(telegram_token, "", msg)

    if finalize_market_data:
        market_data.send_failure_summary(timeframe)
        relevant_failures = set(prepared_cycle.requirements) & set(market_data.failures)
        cycle_summary["warnings"] += len(relevant_failures)
        if send_summary and not send_detailed_routine_logs:
            _send_trade_cycle_summary(cycle_summary)
    return cycle_summary


def positions_summary(timeframe, settings=None, send=True):
    if settings is None:
        settings = config.load_settings(refresh=True)
    msg = telegram_reporting.format_positions_summary(timeframe, settings=settings)
    if send:
        lmsg = telegram_prefix_sl + msg
        print(lmsg)
        telegram.send_telegram_message(telegram_token, "", lmsg)
    return msg


def send_daily_summary(settings=None):
    if settings is None:
        settings = config.load_settings(refresh=True)
    msg = telegram_reporting.format_daily_summary(settings=settings)
    print(msg)
    telegram.send_telegram_message(telegram_token, "", msg)


def send_trade_report(timeframe: str, cycle_summary: dict, settings, total_seconds: float):
    """Send the existing per-timeframe report format for any execution mode."""
    duration = " ".join(
        str(database.calc_duration(total_seconds, decimal_places=2)).split()
    )
    summary_msg = (
        "Report\n"
        f"Status: completed in {duration}\n\n"
        + format_trade_cycle_summary(cycle_summary)
        + "\n\n"
        + positions_summary(timeframe, settings=settings, send=False)
    )
    msg = telegram_prefix_sl + summary_msg
    print(msg)
    telegram.send_telegram_message(telegram_token, "", msg)


def run(timeframe, run_mode=None):
    del run_mode  # Kept for compatibility with existing callers.
    settings = config.load_settings(refresh=True)

    # calculate program run time
    start = timeit.default_timer()

    cycle_summary = trade(timeframe, settings.run_mode, settings=settings, send_summary=False)

    # exchange.create_balance_snapshot(telegram_prefix="")

    # calculate execution time
    total_seconds = timeit.default_timer() - start
    send_trade_report(timeframe, cycle_summary, settings, total_seconds)


def _send_fatal_notification(message: str, context: str) -> None:
    """Best-effort fatal alert that cannot hide or replace the primary failure."""
    try:
        telegram.send_telegram_message(
            telegram_token,
            telegram.EMOJI_WARNING,
            message,
        )
    except Exception as error:
        # Log only fixed context and the exception type. An unexpected sender
        # exception may contain a token-bearing Telegram URL in its message.
        logging.error(
            "Failed to send fatal Telegram notification (%s): %s",
            context,
            type(error).__name__,
        )


def cli_main() -> int:
    """Run the command-line entrypoint and return its process exit code."""
    time_frame = read_arguments()

    try:
        # Validate timeframe and prefixes first
        apply_arguments(time_frame)
    except ValueError as error:
        # Cron-friendly: log error and exit with code 2 (usage/config error)
        logging.error(str(error))
        _send_fatal_notification(
            f"[FATAL] {error}",
            "timeframe validation",
        )
        return 2

    try:
        run(timeframe=time_frame)
    except Exception as error:
        # Unexpected runtime error: keep stacktrace in logs and notify
        logging.exception("Unhandled exception during bot run")
        _send_fatal_notification(
            f"[FATAL] Unhandled exception: {error}",
            "bot run",
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(cli_main())
