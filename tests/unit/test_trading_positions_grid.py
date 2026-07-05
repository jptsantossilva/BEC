import json
import importlib
from types import SimpleNamespace

import pandas as pd
import pytest


trading = importlib.import_module("pages.trading")


def _selection_event(rows):
    return SimpleNamespace(selection=SimpleNamespace(rows=rows))


def _exchange_editor(quote="USDC", fee_pct=0.4):
    return pd.DataFrame(
        [
            {
                "Id": 2,
                "Code": "kraken",
                "Quote Asset": quote,
                "Enabled": True,
                "Spot Taker Fee %": fee_pct,
            }
        ]
    )


def test_exchange_quote_change_requires_loaded_public_markets(monkeypatch):
    monkeypatch.setattr(
        trading.database,
        "get_exchange_settings_table",
        lambda: pd.DataFrame([{"Code": "kraken", "Quote_Asset": "USDC"}]),
    )

    with pytest.raises(ValueError, match="Check kraken API health"):
        trading._save_exchange_editor(_exchange_editor(quote="EUR"), {})


def test_exchange_editor_persists_validated_fee_and_quote(monkeypatch):
    saved = []
    monkeypatch.setattr(
        trading.database,
        "get_exchange_settings_table",
        lambda: pd.DataFrame([{"Code": "kraken", "Quote_Asset": "USDC"}]),
    )
    monkeypatch.setattr(
        trading.database, "update_exchange_settings", lambda rows: saved.extend(rows)
    )

    trading._save_exchange_editor(
        _exchange_editor(quote="EUR", fee_pct=0.4), {"kraken": ["EUR", "USDC"]}
    )

    assert saved == [
        {"Id": 2, "Enabled": True, "Quote_Asset": "EUR", "Taker_Fee": 0.004}
    ]


def test_positions_display_grid_removes_internal_columns_without_mutating_source():
    source = pd.DataFrame(
        [
            {
                "Id": 1,
                "Bot": "1d",
                "Symbol": "ETHUSDT",
                "Strategy_Id": "ema_cross",
                "Strategy_Name": "EMA Cross",
                "Strategy_Params_JSON": '{"ema_fast":12,"ema_slow":34}',
                "TP1": 0,
                "TP2": 1,
                "TP3": 0,
                "TP4": 0,
                "Take_Profits_JSON": '[{"level":2}]',
                "PnL_Perc": "5.00",
                "PnL_Value": "10.00000000",
                "Take Profits": ["TP2 Triggered"],
                "RPQ%": "75.0",
                "Qty": 1.5,
                "Buy_Price": "1000.00000000",
                "Position_Value": "1500.00000000",
                "Date": "2026-05-15 10:00:00",
                "Duration": " 5h ",
                "Trail_Stop_ATR": 900.0,
                "Highest_Price_Since_Entry": 1200.0,
                "Strategy": "EMA Cross",
                "Signal_Setup": "EMA 12/34",
            }
        ]
    )

    display = trading._prepare_positions_display_grid(source, show_trail_stop_atr=False)

    assert list(display.columns) == trading.POSITIONS_DISPLAY_COLUMNS
    for internal_column in [
        "Id",
        "Bot",
        "Strategy_Id",
        "Strategy_Name",
        "Strategy_Params_JSON",
        "TP1",
        "TP2",
        "TP3",
        "TP4",
        "Take_Profits_JSON",
        "Highest_Price_Since_Entry",
        "Trail_Stop_ATR",
    ]:
        assert internal_column not in display.columns
        assert internal_column in source.columns


def test_positions_display_grid_includes_trail_stop_atr_only_when_enabled():
    source = pd.DataFrame(
        [
            {
                "Symbol": "ETHUSDT",
                "PnL_Perc": "5.00",
                "PnL_Value": "10.00000000",
                "Trail_Stop_ATR": 900.0,
            }
        ]
    )

    hidden = trading._prepare_positions_display_grid(source, show_trail_stop_atr=False)
    visible = trading._prepare_positions_display_grid(source, show_trail_stop_atr=True)

    assert "Trail_Stop_ATR" not in hidden.columns
    assert "Trail_Stop_ATR" in visible.columns


def test_selected_position_is_resolved_from_current_positions_snapshot():
    positions = pd.DataFrame([{"Id": 10}, {"Id": 20}])

    selected = trading._resolve_selected_position(_selection_event([1]), positions)

    assert selected["Id"] == 20


def test_stale_selection_is_ignored_after_last_position_is_deleted():
    positions = pd.DataFrame([{"Id": 10}])
    stale_event = _selection_event([0])

    assert trading._resolve_selected_position(stale_event, positions)["Id"] == 10
    assert trading._resolve_selected_position(stale_event, positions.iloc[0:0]) is None


def test_out_of_range_position_selection_is_ignored():
    positions = pd.DataFrame([{"Id": 10}])

    assert trading._resolve_selected_position(_selection_event([1]), positions) is None
    assert trading._resolve_selected_position(_selection_event([-1]), positions) is None


def test_take_profit_display_marks_positions_without_configured_levels():
    source = pd.DataFrame(
        [
            {
                "Strategy_Id": "",
                "Strategy_Params_JSON": "{}",
                "Take_Profits_JSON": "[]",
                "PnL_Value": "0.00000000",
            }
        ]
    )

    display, options, colors = trading._add_take_profit_display_column(source)

    assert display["Take Profits"].iloc[0] == [trading.TAKE_PROFIT_NOT_DEFINED_LABEL]
    assert options == [trading.TAKE_PROFIT_NOT_DEFINED_LABEL]
    assert colors == ["#F2F2F2"]
    assert trading._take_profit_format(trading.TAKE_PROFIT_NOT_DEFINED_LABEL) == "No TP"


def test_position_signal_setup_uses_builtin_strategy_snapshot_params():
    row = pd.Series(
        {
            "Strategy_Id": "ema_cross_with_market_phases",
            "Strategy_Params_JSON": json.dumps({"ema_fast": 12, "ema_slow": 34}),
        }
    )

    assert trading._format_position_signal_setup(row) == "ema_fast=12 | ema_slow=34"


def test_position_signal_setup_requires_strategy_params_snapshot():
    row = pd.Series(
        {
            "Strategy_Id": "ema_cross",
            "Strategy_Params_JSON": "",
        }
    )

    assert trading._format_position_signal_setup(row) == ""


def _realized_trades_sample():
    return pd.DataFrame(
        [
            {
                "Bot": "1d",
                "Strategy_Id": "alpha",
                "Strategy_Name": "Alpha",
                "Sell_Date": "2026-01-05 10:00:00",
                "PnL_Perc": 10.0,
                "PnL_Value": 10.0,
                "Sell_Position_Value": 100.0,
                "Exit_Reason": "Take-Profit 1",
                "Stop_Type": "tp",
            },
            {
                "Bot": "4h",
                "Strategy_Id": "alpha",
                "Strategy_Name": "Alpha",
                "Sell_Date": "2026-01-07 10:00:00",
                "PnL_Perc": -5.0,
                "PnL_Value": -10.0,
                "Sell_Position_Value": 200.0,
                "Exit_Reason": "Hard stop",
                "Stop_Type": "hard_sl",
            },
            {
                "Bot": "1h",
                "Strategy_Id": "beta",
                "Strategy_Name": "Beta",
                "Sell_Date": "2026-02-02 10:00:00",
                "PnL_Perc": 4.0,
                "PnL_Value": 4.0,
                "Sell_Position_Value": 100.0,
                "Exit_Reason": "Strategy exit",
                "Stop_Type": "strategy",
            },
            {
                "Bot": "1d",
                "Strategy_Id": "",
                "Strategy_Name": "",
                "Sell_Date": "2026-02-04 10:00:00",
                "PnL_Perc": 2.0,
                "PnL_Value": 1.0,
                "Sell_Position_Value": 50.0,
                "Exit_Reason": "",
                "Stop_Type": "",
            },
        ]
    )


def test_realized_strategy_summary_groups_named_and_missing_strategies():
    trades = trading._normalize_realized_strategy_columns(_realized_trades_sample())

    summary = trading._build_realized_strategy_summary(trades)

    alpha = summary[summary["Strategy"] == "Alpha"].iloc[0]
    missing = summary[summary["Strategy"] == trading.MISSING_STRATEGY_LABEL].iloc[0]
    assert round(float(alpha["PnL_Perc"]), 4) == 0.0
    assert float(alpha["PnL_Value"]) == 0.0
    assert int(alpha["Positions"]) == 2
    assert float(alpha["Win_Rate"]) == 50.0
    assert float(missing["PnL_Perc"]) == 2.0
    assert int(missing["Positions"]) == 1


def test_realized_strategy_filter_supports_all_specific_and_missing():
    trades = trading._normalize_realized_strategy_columns(_realized_trades_sample())

    all_trades = trading._filter_realized_trades_by_strategy(
        trades, trading.ALL_STRATEGIES_FILTER
    )
    alpha_trades = trading._filter_realized_trades_by_strategy(trades, "alpha")
    missing_trades = trading._filter_realized_trades_by_strategy(
        trades, trading.MISSING_STRATEGY_FILTER
    )

    assert len(all_trades) == 4
    assert set(alpha_trades["Strategy"]) == {"Alpha"}
    assert len(alpha_trades) == 2
    assert len(missing_trades) == 1
    assert missing_trades.iloc[0]["Strategy"] == trading.MISSING_STRATEGY_LABEL


def test_realized_strategy_filter_options_include_only_period_strategies():
    trades = trading._normalize_realized_strategy_columns(_realized_trades_sample())
    trades = trades[trades["Strategy_Id"].isin(["alpha", ""])]

    options, labels = trading.get_realized_strategy_filter_options(trades)

    assert options == [
        trading.ALL_STRATEGIES_FILTER,
        "alpha",
        trading.MISSING_STRATEGY_FILTER,
    ]
    assert labels[trading.ALL_STRATEGIES_FILTER] == "All strategies"
    assert labels["alpha"] == "Alpha"
    assert labels[trading.MISSING_STRATEGY_FILTER] == trading.MISSING_STRATEGY_LABEL
    assert "beta" not in options


def test_realized_all_time_period_loads_without_year_filter(monkeypatch):
    calls = []
    sample = _realized_trades_sample()

    def fake_get_orders_by_side_year_month(side, year, month):
        calls.append((side, year, month))
        return sample

    monkeypatch.setattr(
        trading.database,
        "get_orders_by_side_year_month",
        fake_get_orders_by_side_year_month,
    )

    trades = trading._get_realized_trades_for_period(trading.ALL_TIME_FILTER, "13")

    assert calls == [("SELL", trading.ALL_TIME_FILTER, "13")]
    assert len(trades) == len(sample)


def test_monthly_realized_returns_respects_strategy_filter(monkeypatch):
    trading.num_decimals = 2
    trades = trading._normalize_realized_strategy_columns(_realized_trades_sample())
    monkeypatch.setattr(
        trading,
        "_get_realized_trades_for_year",
        lambda year, strategy_filter=trading.ALL_STRATEGIES_FILTER: (
            trading._filter_realized_trades_by_strategy(trades, strategy_filter)
        ),
    )

    alpha_months = trading.calculate_monthly_realized_returns(["2026"], "alpha")
    all_months = trading.calculate_monthly_realized_returns(
        ["2026"], trading.ALL_STRATEGIES_FILTER
    )

    alpha_jan = alpha_months[alpha_months["Month"] == "Jan"].iloc[0]
    all_feb = all_months[all_months["Month"] == "Feb"].iloc[0]
    assert round(float(alpha_jan["PnL_Perc"]), 4) == 0.0
    assert int(alpha_jan["Positions"]) == 2
    assert round(float(all_feb["PnL_Perc"]), 4) == 3.3333
    assert int(all_feb["Positions"]) == 2


def test_live_vs_backtest_summary_joins_on_strategy_timeframe_and_symbol(monkeypatch):
    trades = trading._normalize_realized_strategy_columns(_realized_trades_sample())
    backtests = pd.DataFrame(
        [
            {
                "Strategy_Id": "beta",
                "Symbol": "BETAUSDC",
                "Time_Frame": "1h",
                "Return_Perc": 12.5,
                "Win_Rate_Perc": 60.0,
                "Trades": 20,
                "Quality_Grade": "B",
                "Quality_Score": 72.0,
                "Trading_Approved": 1,
            }
        ]
    )
    trades.loc[trades["Strategy_Id"] == "beta", "Symbol"] = "BETAUSDC"
    monkeypatch.setattr(trading.database, "get_all_backtesting_results", lambda: backtests)

    summary = trading._build_live_vs_backtest_summary(trades)

    beta = summary[summary["Strategy"] == "Beta"].iloc[0]
    assert float(beta["Live_PnL_Perc"]) == 4.0
    assert float(beta["Backtest_Return_Perc"]) == 12.5
    assert beta["Quality_Grade"] == "B"
    assert int(beta["Trading_Approved"]) == 1
