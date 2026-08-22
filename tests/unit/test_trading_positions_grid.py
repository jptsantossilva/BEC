import json
import importlib
from contextlib import nullcontext
from types import SimpleNamespace

import pandas as pd
import pytest

from bec.exchanges.base import MarketInfo, SpotMarketAvailability

trading = importlib.import_module("pages.trading")


def _selection_event(rows):
    return SimpleNamespace(selection=SimpleNamespace(rows=rows))


def _exchange_editor(
    quote="USDC",
    fee_pct=0.4,
    *,
    buy_enabled=False,
    sell_enabled=False,
    partial_sell_policy="accumulate",
    sizing_buffer_pct=1.0,
):
    return pd.DataFrame(
        [
            {
                "Id": 2,
                "Code": "kraken",
                "Quote Asset": quote,
                "Enabled": True,
                "Spot Taker Fee %": fee_pct,
                "Buy Enabled": buy_enabled,
                "Sell Enabled": sell_enabled,
                "Partial Sell Policy": partial_sell_policy,
                "Sizing Buffer %": sizing_buffer_pct,
            }
        ]
    )


def _okx_exchange_editor(*, enabled=True, quote="USDC", adapter_id="myokx"):
    return pd.DataFrame(
        [
            {
                "Id": 3,
                "Code": "okx",
                "Quote Asset": quote,
                "Adapter Variant": adapter_id,
                "Enabled": enabled,
            }
        ]
    )


def _persisted_okx(*, quote="USDC", adapter_id="myokx"):
    return pd.DataFrame(
        [
            {
                "Id": 3,
                "Code": "okx",
                "Quote_Asset": quote,
                "Adapter_Id": adapter_id,
            }
        ]
    )


def test_manual_demo_submission_feedback_handles_exchange_rejection():
    assert trading._manual_demo_submission_feedback(None, "submitted") == (
        "error",
        "The exchange rejected the order before submission. No replacement was "
        "submitted; review the operational log.",
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
        {
            "Id": 2,
            "Enabled": True,
            "Quote_Asset": "EUR",
            "Taker_Fee": 0.004,
            "Buy_Enabled": False,
            "Sell_Enabled": False,
            "Partial_Sell_Policy": "accumulate",
            "Sizing_Buffer_Pct": 1.0,
        }
    ]


def test_exchange_editor_persists_explicit_live_gates(monkeypatch):
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
        _exchange_editor(
            buy_enabled=True,
            sell_enabled=True,
            partial_sell_policy="sell_all",
            sizing_buffer_pct=1.5,
        ),
        {"kraken": ["USDC"]},
    )

    assert saved[0]["Buy_Enabled"] is True
    assert saved[0]["Sell_Enabled"] is True
    assert saved[0]["Partial_Sell_Policy"] == "sell_all"
    assert saved[0]["Sizing_Buffer_Pct"] == pytest.approx(1.5)


def test_okx_save_automatically_loads_and_syncs_public_markets(monkeypatch):
    calls = []
    market = MarketInfo("BTC/USDC", "BTC-USDC", "BTC", "USDC", True)
    adapter = SimpleNamespace(
        load_markets=lambda *, force=False: calls.append(("load", force))
        or {market.symbol: market}
    )
    monkeypatch.setattr(
        trading.database,
        "exchange_quote_has_validated_markets",
        lambda exchange_id, quote: False,
    )
    monkeypatch.setattr(
        trading,
        "get_adapter_for_code",
        lambda code, *, adapter_id: calls.append((code, adapter_id)) or adapter,
    )
    monkeypatch.setattr(
        trading.database,
        "sync_exchange_market_catalog",
        lambda exchange_id, markets: calls.append(
            ("sync", exchange_id, list(markets))
        ),
    )

    quotes = trading._prepare_okx_public_markets_for_save(
        _okx_exchange_editor(), _persisted_okx(), {}
    )

    assert quotes == {"okx": ["USDC"]}
    assert calls == [
        ("okx", "myokx"),
        ("load", True),
        ("sync", 3, ["BTC/USDC"]),
    ]


def test_okx_save_reuses_an_already_validated_public_catalog(monkeypatch):
    monkeypatch.setattr(
        trading.database,
        "exchange_quote_has_validated_markets",
        lambda exchange_id, quote: (exchange_id, quote) == (3, "USDC"),
    )
    monkeypatch.setattr(
        trading,
        "get_adapter_for_code",
        lambda *args, **kwargs: pytest.fail("validated markets should not reload"),
    )

    quotes = trading._prepare_okx_public_markets_for_save(
        _okx_exchange_editor(), _persisted_okx(), {}
    )

    assert quotes == {"okx": ["USDC"]}


def test_okx_save_reports_public_market_load_failure_without_raw_error(monkeypatch):
    monkeypatch.setattr(
        trading.database,
        "exchange_quote_has_validated_markets",
        lambda exchange_id, quote: False,
    )
    monkeypatch.setattr(
        trading,
        "get_adapter_for_code",
        lambda *args, **kwargs: SimpleNamespace(
            load_markets=lambda **kwargs: (_ for _ in ()).throw(
                RuntimeError("sensitive upstream detail")
            )
        ),
    )

    with pytest.raises(ValueError, match="Could not load OKX public spot markets") as exc:
        trading._prepare_okx_public_markets_for_save(
            _okx_exchange_editor(), _persisted_okx(), {}
        )

    assert "sensitive upstream detail" not in str(exc.value)


def test_standard_exchange_rows_hide_the_internal_okx_validation_identity():
    exchanges = pd.DataFrame(
        [
            {"Id": 1, "Code": "binance"},
            {"Id": 2, "Code": "kraken"},
            {"Id": 3, "Code": "okx"},
            {"Id": 4, "Code": "okx_demo"},
        ]
    )

    visible = trading._standard_exchange_rows(exchanges)

    assert visible["Code"].tolist() == ["binance", "kraken", "okx"]


def test_okx_read_only_buttons_use_location_scoped_streamlit_keys(monkeypatch):
    keys = []
    exchanges = pd.DataFrame(
        [{"Code": "okx", "Name": "OKX"}]
    )
    monkeypatch.setattr(trading.st, "caption", lambda message: None)
    monkeypatch.setattr(
        trading.st,
        "button",
        lambda label, **kwargs: keys.append(kwargs["key"]) or False,
    )

    trading._render_okx_read_only_checks(
        exchanges, key_scope="okx_production_validation"
    )
    trading._render_okx_read_only_checks(
        exchanges, key_scope="diagnostics"
    )

    assert keys == [
        "okx_production_validation_okx_readonly_api",
        "diagnostics_okx_readonly_api",
    ]
    assert len(keys) == len(set(keys))


def test_okx_validation_environment_inherits_production_identity_and_starts_disarmed():
    production = pd.Series(
        {
            "Id": 3,
            "Code": "okx",
            "Quote_Asset": "USDT",
            "Adapter_Id": "okx",
        }
    )
    demo = pd.Series(
        {
            "Id": 4,
            "Code": "okx_demo",
            "Taker_Fee": float("nan"),
            "Buy_Enabled": True,
            "Sell_Enabled": True,
            "Partial_Sell_Policy": "accumulate",
            "Sizing_Buffer_Pct": float("nan"),
        }
    )

    record = trading._okx_validation_environment_record(
        demo, production, enabled=True
    )

    assert record == {
        "Id": 4,
        "Enabled": True,
        "Quote_Asset": "USDT",
        "Taker_Fee": None,
        "Adapter_Id": "okx",
        "Buy_Enabled": False,
        "Sell_Enabled": False,
        "Partial_Sell_Policy": "accumulate",
        "Sizing_Buffer_Pct": 5.0,
    }


def test_okx_arming_confirmation_is_requested_only_for_new_production_side():
    persisted = pd.DataFrame(
        [
            {
                "Code": "okx",
                "Buy_Enabled": False,
                "Sell_Enabled": False,
            },
            {
                "Code": "kraken",
                "Buy_Enabled": False,
                "Sell_Enabled": False,
            },
        ]
    )
    unchanged = pd.DataFrame(
        [
            {"Code": "okx", "Buy Enabled": False, "Sell Enabled": False},
            {"Code": "kraken", "Buy Enabled": True, "Sell Enabled": True},
        ]
    )
    arming_okx = unchanged.copy()
    arming_okx.loc[arming_okx["Code"] == "okx", "Buy Enabled"] = True

    assert (
        trading._okx_production_arming_requested(unchanged, persisted) is False
    )
    assert (
        trading._okx_production_arming_requested(arming_okx, persisted) is True
    )


def test_okx_disarming_does_not_require_production_confirmation():
    persisted = pd.DataFrame(
        [{"Code": "okx", "Buy_Enabled": True, "Sell_Enabled": True}]
    )
    edited = pd.DataFrame(
        [{"Code": "okx", "Buy Enabled": False, "Sell Enabled": False}]
    )

    assert trading._okx_production_arming_requested(edited, persisted) is False


def test_exchange_summary_safe_state_has_no_alerts(monkeypatch):
    captions = []
    errors = []
    warnings = []
    monkeypatch.setattr(trading.config, "read_setting", lambda key: "test")
    monkeypatch.setattr(
        trading.database, "get_job_schedule_enabled", lambda name: True
    )
    monkeypatch.setattr(trading.st, "caption", captions.append)
    monkeypatch.setattr(trading.st, "error", errors.append)
    monkeypatch.setattr(trading.st, "warning", warnings.append)

    trading._render_active_exchange_summary(
        {
            "name": "Kraken",
            "execution_environment": "production",
            "quote_asset": "USDC",
            "buy_enabled": True,
            "sell_enabled": False,
        },
        SimpleNamespace(uses_gated_live_execution=True),
        [],
    )

    assert len(captions) == 1
    assert "Run mode TEST" in captions[0]
    assert errors == []
    assert warnings == []


def test_exchange_summary_live_state_has_one_clear_error(monkeypatch):
    errors = []
    warnings = []
    monkeypatch.setattr(trading.config, "read_setting", lambda key: "live")
    monkeypatch.setattr(
        trading.database, "get_job_schedule_enabled", lambda name: True
    )
    monkeypatch.setattr(trading.st, "caption", lambda message: None)
    monkeypatch.setattr(trading.st, "error", errors.append)
    monkeypatch.setattr(trading.st, "warning", warnings.append)

    trading._render_active_exchange_summary(
        {
            "name": "Kraken",
            "execution_environment": "production",
            "quote_asset": "USDC",
            "buy_enabled": True,
            "sell_enabled": True,
        },
        SimpleNamespace(uses_gated_live_execution=True),
        [],
    )

    assert errors == [
        "Live run mode is enabled and Kraken order sides are armed. "
        "Real orders are possible."
    ]
    assert warnings == []


def test_okx_public_health_check_syncs_catalog_without_touching_demo(monkeypatch):
    synced = []
    exchanges = pd.DataFrame(
        [
            {"Id": 3, "Code": "okx", "Adapter_Id": "myokx"},
            {"Id": 4, "Code": "okx_demo", "Adapter_Id": "myokx"},
        ]
    )
    market = MarketInfo("BTC/USDC", "BTC-USDC", "BTC", "USDC", True)
    adapter = SimpleNamespace(
        health_check=lambda: SimpleNamespace(available=True, message="ok"),
        load_markets=lambda: {market.symbol: market},
    )
    monkeypatch.setattr(trading, "get_registered_exchange_codes", lambda: ("okx",))
    monkeypatch.setattr(trading, "get_adapter_for_code", lambda *args, **kwargs: adapter)
    monkeypatch.setattr(
        trading.database,
        "sync_exchange_market_catalog",
        lambda exchange_id, markets: synced.append((exchange_id, markets)),
    )

    statuses, quotes = trading._check_exchange_public_apis(exchanges)

    assert statuses == {
        "okx": "Available",
        "okx_demo": "Configuration only; public adapter is not released",
    }
    assert quotes == {"okx": ["USDC"], "okx_demo": []}
    assert synced == [(3, {"BTC/USDC": market})]


def test_spot_market_diagnostics_grid_summarizes_supported_quote_assets():
    markets = [
        SpotMarketAvailability(
            "BTC/USDC", "BTC", "USDC", True, "online"
        ),
        SpotMarketAvailability(
            "AI16Z/USDC", "AI16Z", "USDC", False, "cancel_only"
        ),
        SpotMarketAvailability(
            "EURR/USDC", "EURR", "USDC", False, "post_only"
        ),
        SpotMarketAvailability(
            "USDR/USDC", "USDR", "USDC", False, "post_only"
        ),
        SpotMarketAvailability(
            "ETH/USDT", "ETH", "USDT", True, "online"
        ),
        SpotMarketAvailability(
            "ETH/EUR", "ETH", "EUR", True, "online"
        ),
    ]

    grid = trading._spot_market_diagnostics_grid(markets)

    assert grid.to_dict("records") == [
        {
            "Trade against": "USDC",
            "Spot markets": 4,
            "Active/online used by BEC": 1,
            "Not normally tradable": "1 cancel_only + 2 post_only",
        },
        {
            "Trade against": "USDT",
            "Spot markets": 1,
            "Active/online used by BEC": 1,
            "Not normally tradable": "None",
        },
        {
            "Trade against": "BTC",
            "Spot markets": 0,
            "Active/online used by BEC": 0,
            "Not normally tradable": "None",
        },
    ]


def test_active_spot_market_pairs_grid_lists_only_supported_active_pairs():
    markets = [
        SpotMarketAvailability("ETH/USDT", "ETH", "USDT", True, "online"),
        SpotMarketAvailability("BTC/USDC", "BTC", "USDC", True, "online"),
        SpotMarketAvailability("AI16Z/USDC", "AI16Z", "USDC", False, "cancel_only"),
        SpotMarketAvailability("ETH/EUR", "ETH", "EUR", True, "online"),
    ]

    grid = trading._active_spot_market_pairs_grid(markets)

    assert grid.to_dict("records") == [
        {
            "Trading pair": "BTC/USDC",
            "Base asset": "BTC",
            "Quote asset": "USDC",
            "Status": "online",
        },
        {
            "Trading pair": "ETH/USDT",
            "Base asset": "ETH",
            "Quote asset": "USDT",
            "Status": "online",
        },
    ]


def test_spot_market_diagnostics_loads_selected_public_adapter(monkeypatch):
    calls = []
    market = SpotMarketAvailability(
        "BTC/USDC", "BTC", "USDC", True, "online"
    )
    adapter = SimpleNamespace(
        load_spot_market_availability=lambda force=False: (
            calls.append(force) or (market,)
        )
    )
    monkeypatch.setattr(
        trading,
        "get_adapter_for_code",
        lambda code, adapter_id="": (
            calls.append((code, adapter_id)) or adapter
        ),
    )

    result = trading._load_spot_market_diagnostics(
        {"code": "kraken", "adapter_id": "kraken"}
    )

    assert calls == [("kraken", "kraken"), True]
    assert result["summary"].iloc[0].to_dict() == {
        "Trade against": "USDC",
        "Spot markets": 1,
        "Active/online used by BEC": 1,
        "Not normally tradable": "None",
    }
    assert result["active_pairs"].to_dict("records") == [
        {
            "Trading pair": "BTC/USDC",
            "Base asset": "BTC",
            "Quote asset": "USDC",
            "Status": "online",
        }
    ]


def test_spot_market_diagnostics_button_renders_loaded_grid(monkeypatch):
    rendered = []
    successes = []
    grid = pd.DataFrame(
        [
            {
                "Trade against": "USDC",
                "Spot markets": 47,
                "Active/online used by BEC": 44,
                "Not normally tradable": "1 cancel_only + 2 post_only",
            }
        ]
    )
    monkeypatch.setattr(trading.st, "session_state", {})
    monkeypatch.setattr(trading.st, "caption", lambda message: None)
    monkeypatch.setattr(
        trading.st,
        "button",
        lambda *args, **kwargs: kwargs["on_click"](*kwargs["args"]),
    )
    monkeypatch.setattr(
        trading.st,
        "spinner",
        lambda *args, **kwargs: nullcontext(),
    )
    monkeypatch.setattr(
        trading.st,
        "dataframe",
        lambda frame, **kwargs: rendered.append((frame, kwargs)),
    )
    monkeypatch.setattr(
        trading.st,
        "success",
        lambda message: successes.append(message),
    )
    monkeypatch.setattr(trading.st, "toggle", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        trading,
        "_load_spot_market_diagnostics",
        lambda active_exchange: {
            "summary": grid,
            "active_pairs": pd.DataFrame(
                [{"Trading pair": "BTC/USDC"}]
            ),
        },
    )

    trading._render_spot_market_diagnostics(
        {"code": "kraken", "name": "Kraken"}
    )

    assert rendered == [(grid, {"hide_index": True, "width": "content"})]
    assert successes == [
        "Loaded 47 direct Kraken spot markets supported by BEC "
        "(1 active/online)."
    ]


def test_spot_market_diagnostics_button_identity_survives_exchange_switch(
    monkeypatch,
):
    buttons = []
    monkeypatch.setattr(trading.st, "session_state", {})
    monkeypatch.setattr(trading.st, "caption", lambda message: None)
    monkeypatch.setattr(
        trading.st,
        "button",
        lambda *args, **kwargs: buttons.append(kwargs),
    )

    trading._render_spot_market_diagnostics(
        {"code": "kraken", "name": "Kraken"}
    )
    trading._render_spot_market_diagnostics(
        {"code": "okx", "name": "OKX"}
    )

    assert [button["key"] for button in buttons] == [
        "check_active_spot_markets",
        "check_active_spot_markets",
    ]
    assert [button["args"] for button in buttons] == [("kraken",), ("okx",)]


def test_spot_market_diagnostics_error_feedback_survives_rerun(monkeypatch):
    errors = []
    session_state = {
        "spot_market_diagnostics_okx": pd.DataFrame([{"stale": True}]),
    }
    monkeypatch.setattr(trading.st, "session_state", session_state)
    monkeypatch.setattr(trading.st, "caption", lambda message: None)
    monkeypatch.setattr(
        trading.st,
        "button",
        lambda *args, **kwargs: kwargs["on_click"](*kwargs["args"]),
    )
    monkeypatch.setattr(
        trading.st,
        "spinner",
        lambda *args, **kwargs: nullcontext(),
    )
    monkeypatch.setattr(trading.st, "error", lambda message: errors.append(message))
    monkeypatch.setattr(
        trading,
        "_load_spot_market_diagnostics",
        lambda active_exchange: (_ for _ in ()).throw(RuntimeError("private detail")),
    )

    trading._render_spot_market_diagnostics({"code": "okx", "name": "OKX"})
    monkeypatch.setattr(trading.st, "button", lambda *args, **kwargs: False)
    trading._render_spot_market_diagnostics({"code": "okx", "name": "OKX"})

    assert "spot_market_diagnostics_okx" not in session_state
    assert session_state["spot_market_diagnostics_feedback_okx"] == (
        "error",
        "OKX spot market diagnostics are unavailable. Review the operational "
        "log and try again.",
    )
    assert errors == [
        "OKX spot market diagnostics are unavailable. Review the operational "
        "log and try again.",
        "OKX spot market diagnostics are unavailable. Review the operational "
        "log and try again.",
    ]


def test_spot_market_diagnostics_request_survives_interrupted_run(monkeypatch):
    class SimulatedRerun(BaseException):
        pass

    session_state = {}
    should_interrupt = True
    summary = pd.DataFrame(
        [
            {
                "Trade against": "USDC",
                "Spot markets": 1,
                "Active/online used by BEC": 1,
                "Not normally tradable": "None",
            }
        ]
    )

    def button(*args, **kwargs):
        nonlocal should_interrupt
        if should_interrupt:
            kwargs["on_click"](*kwargs["args"])

    def load(active_exchange):
        nonlocal should_interrupt
        if should_interrupt:
            should_interrupt = False
            raise SimulatedRerun()
        return {"summary": summary, "active_pairs": pd.DataFrame()}

    monkeypatch.setattr(trading.st, "session_state", session_state)
    monkeypatch.setattr(trading.st, "caption", lambda message: None)
    monkeypatch.setattr(trading.st, "button", button)
    monkeypatch.setattr(
        trading.st,
        "spinner",
        lambda *args, **kwargs: nullcontext(),
    )
    monkeypatch.setattr(trading.st, "success", lambda message: None)
    monkeypatch.setattr(trading.st, "dataframe", lambda *args, **kwargs: None)
    monkeypatch.setattr(trading, "_load_spot_market_diagnostics", load)

    with pytest.raises(SimulatedRerun):
        trading._render_spot_market_diagnostics(
            {"code": "okx", "name": "OKX"}
        )

    assert session_state[trading.SPOT_MARKET_DIAGNOSTICS_REQUEST_KEY] == "okx"

    trading._render_spot_market_diagnostics({"code": "okx", "name": "OKX"})

    assert trading.SPOT_MARKET_DIAGNOSTICS_REQUEST_KEY not in session_state
    assert session_state["spot_market_diagnostics_okx"]["summary"].equals(summary)


def test_spot_market_diagnostics_pair_list_is_rendered_only_when_requested(
    monkeypatch,
):
    summary = pd.DataFrame([{"Trade against": "USDC", "Spot markets": 1}])
    active_pairs = pd.DataFrame(
        [
            {
                "Trading pair": "BTC/USDC",
                "Base asset": "BTC",
                "Quote asset": "USDC",
                "Status": "online",
            }
        ]
    )
    rendered = []
    monkeypatch.setattr(
        trading.st,
        "session_state",
        {
            "spot_market_diagnostics_okx": {
                "summary": summary,
                "active_pairs": active_pairs,
            }
        },
    )
    monkeypatch.setattr(trading.st, "caption", lambda message: None)
    monkeypatch.setattr(trading.st, "button", lambda *args, **kwargs: False)
    monkeypatch.setattr(trading.st, "toggle", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        trading.st,
        "dataframe",
        lambda frame, **kwargs: rendered.append((frame, kwargs)),
    )

    trading._render_spot_market_diagnostics(
        {"code": "okx", "name": "OKX"}
    )

    assert rendered == [
        (summary, {"hide_index": True, "width": "content"}),
        (
            active_pairs,
            {"hide_index": True, "width": "content", "height": 400},
        ),
    ]


def test_active_exchange_selector_replaces_stale_session_choice(monkeypatch):
    session_state = {
        trading.ACTIVE_EXCHANGE_SELECTOR_KEY: 1,
    }
    monkeypatch.setattr(trading.st, "session_state", session_state)

    trading._sync_active_exchange_selector(2)

    assert session_state == {
        trading.ACTIVE_EXCHANGE_SELECTOR_KEY: 2,
        trading.ACTIVE_EXCHANGE_SELECTOR_CONTEXT_KEY: 2,
    }


def test_active_exchange_selector_preserves_pending_user_choice(monkeypatch):
    session_state = {
        trading.ACTIVE_EXCHANGE_SELECTOR_KEY: 1,
        trading.ACTIVE_EXCHANGE_SELECTOR_CONTEXT_KEY: 2,
    }
    monkeypatch.setattr(trading.st, "session_state", session_state)

    trading._sync_active_exchange_selector(2)

    assert session_state == {
        trading.ACTIVE_EXCHANGE_SELECTOR_KEY: 1,
        trading.ACTIVE_EXCHANGE_SELECTOR_CONTEXT_KEY: 2,
    }


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


def test_realized_rolling_periods_accept_utc_sell_dates(monkeypatch):
    recent = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=1)
    trades = pd.DataFrame(
        [
            {
                "Bot": "manual_demo",
                "Sell_Date": recent.isoformat(),
                "PnL_Perc": 0.0,
                "PnL_Value": 0.0,
                "Sell_Position_Value": 0.0,
            }
        ]
    )
    monkeypatch.setattr(
        trading,
        "_get_realized_trades_for_period",
        lambda year, month: trades,
    )

    periods = trading.calculate_realized_rolling_periods()

    assert int(periods.loc[periods["Period"] == "24h", "Positions"].iloc[0]) == 1


@pytest.mark.parametrize("value", [None, "", float("nan"), "not-a-number"])
def test_pnl_color_skips_missing_or_non_numeric_values(value):
    assert trading.set_pnl_color(value) == ""


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
