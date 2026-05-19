import json
import importlib

import pandas as pd


trading = importlib.import_module("pages.trading")


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
