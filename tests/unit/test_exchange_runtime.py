import sqlite3
from datetime import datetime, timezone
from decimal import Decimal

import pytest

import bec.utils.database as database
from bec.exchanges import service
from bec.exchanges.base import MarketInfo, OrderFill, OrderResult, OrderStatus
from bec.db.backtesting_schema import apply_exchange_backtesting_schema
from bec.db.backtesting_context_schema import apply_backtesting_context_schema
from bec.db.exchange_schema import apply_exchange_aware_schema
from bec.db.live_execution_schema import apply_live_execution_schema
from bec.db.order_fills_schema import apply_durable_order_fills_schema
from bec.db.okx_configuration_schema import apply_okx_configuration_schema


def _runtime_database(tmp_path):
    connection = sqlite3.connect(tmp_path / "runtime.db", check_same_thread=False)
    connection.executescript(
        f"""
        {database.create_orders_table}
        {database.sql_create_positions_table}
        {database.sql_create_backtesting_results_table}
        {database.sql_create_backtesting_trades_table}
        {database.sql_create_balances_table}
        {database.sql_create_symbols_by_market_phase_table}
        {database.sql_create_locked_values_table}
        {database.sql_create_job_schedules_table}
        {database.sql_create_backtesting_jobs_table}
        {database.sql_create_monte_carlo_jobs_table}
        {database.sql_create_monte_carlo_results_table}
        {database.sql_create_strategies_table}
        {database.sql_create_backtesting_settings_table}
        """
    )
    columns = ", ".join(database.DEFAULT_BACKTESTING_SETTINGS)
    placeholders = ", ".join("?" for _ in database.DEFAULT_BACKTESTING_SETTINGS)
    connection.execute(
        f"INSERT INTO Backtesting_Settings ({columns}) VALUES ({placeholders})",
        tuple(database.DEFAULT_BACKTESTING_SETTINGS.values()),
    )
    connection.commit()
    connection.execute("BEGIN IMMEDIATE")
    apply_exchange_aware_schema(connection, upgraded_install=False)
    connection.execute(
        """
        INSERT INTO Exchanges (Id, Code, Name, Enabled, Is_Default, Trading_Mode)
        VALUES (2, 'kraken', 'Kraken', 1, 0, 'spot')
        """
    )
    connection.execute("INSERT INTO Strategies (Id, Name) VALUES ('ema', 'EMA')")
    apply_exchange_backtesting_schema(connection)
    apply_backtesting_context_schema(connection)
    apply_live_execution_schema(connection)
    apply_durable_order_fills_schema(connection)
    apply_okx_configuration_schema(connection)
    connection.execute("UPDATE Exchanges SET Enabled=1 WHERE Code IN ('binance', 'kraken')")
    for name in database.EXCHANGE_DEPENDENT_JOBS:
        connection.execute(
            """
            INSERT INTO Job_Schedules (name, script, cadence, enabled)
            VALUES (?, 'main.py', '1h', 0)
            """,
            (name,),
        )
    connection.commit()
    return connection


def _use_connection(connection):
    original = getattr(database._thread_local, "conn", None)
    database._thread_local.conn = connection
    return original


def _restore_connection(original):
    if original is None:
        try:
            delattr(database._thread_local, "conn")
        except AttributeError:
            pass
    else:
        database._thread_local.conn = original


def test_new_install_activation_enables_jobs_and_candidates_do_not_block(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        connection.execute(
            "INSERT INTO Positions (Symbol, Position, Exchange_Id) VALUES ('BTCUSDC', 0, 1)"
        )
        selected = database.set_active_exchange(1)

        assert selected["code"] == "binance"
        enabled = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM Job_Schedules WHERE enabled=1"
            )
        }
        assert enabled == {
            *database.LIVE_TRADING_JOBS,
            *database.PUBLIC_ANALYSIS_JOBS,
            *database.SIGNAL_SCHEDULE_JOBS,
        }
    finally:
        connection.close()
        _restore_connection(original)


def test_new_install_kraken_activation_enables_public_analysis_only(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        selected = database.set_active_exchange(2)

        assert selected["code"] == "kraken"
        enabled = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM Job_Schedules WHERE enabled=1"
            )
        }
        assert enabled == set(database.PUBLIC_ANALYSIS_JOBS)
    finally:
        connection.close()
        _restore_connection(original)


def test_switching_to_kraken_disables_live_trading_jobs(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        database.set_active_exchange(2)

        placeholders = ",".join("?" for _ in database.LIVE_TRADING_JOBS)
        enabled_live_jobs = connection.execute(
            f"SELECT COUNT(*) FROM Job_Schedules "
            f"WHERE enabled=1 AND name IN ({placeholders})",
            database.LIVE_TRADING_JOBS,
        ).fetchone()[0]
        assert enabled_live_jobs == 0
    finally:
        connection.close()
        _restore_connection(original)


def test_live_trading_job_cannot_be_enabled_for_public_only_kraken(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(2)

        with pytest.raises(ValueError, match="explicitly enabled"):
            database.set_job_schedule_enabled("main_1h", True)

        assert database.get_job_schedule_enabled("main_1h") is False
    finally:
        connection.close()
        _restore_connection(original)


def test_kraken_live_flags_explicitly_unlock_schedules_and_reconciliation(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(2)
        database.update_exchange_settings(
            [
                {
                    "Id": 2,
                    "Enabled": True,
                    "Quote_Asset": "USDC",
                    "Taker_Fee": 0.004,
                    "Buy_Enabled": True,
                    "Sell_Enabled": False,
                    "Partial_Sell_Policy": "accumulate",
                    "Sizing_Buffer_Pct": 1.0,
                }
            ]
        )

        database.set_job_schedule_enabled("main_1h", True)

        assert database.get_active_exchange()["buy_enabled"] == 1
        assert database.get_job_schedule_enabled("main_1h") is True
        assert database.get_job_schedule_enabled(database.RECONCILIATION_JOB) is True
    finally:
        connection.close()
        _restore_connection(original)


def test_kraken_backtesting_queues_require_explicit_fee(tmp_path, monkeypatch):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(2)

        connection.execute(
            "DELETE FROM Exchange_Backtesting_Settings WHERE Exchange_Id=2"
        )
        connection.commit()

        with pytest.raises(RuntimeError, match="explicit backtesting fee"):
            database.enqueue_backtesting_jobs(
                [{"strategy_id": "ema", "symbol": "BTC/EUR", "timeframe": "1h"}]
            )
        database.set_exchange_backtesting_fee(0.0026)
        monkeypatch.setattr(
            database,
            "_exchange_symbol_metadata",
            lambda symbol: (2, "BTC/EUR", "XXBTZEUR", "BTC", "EUR"),
        )

        queued = database.enqueue_backtesting_jobs(
            [{"strategy_id": "ema", "symbol": "BTC/EUR", "timeframe": "1h"}]
        )["queued"][0]

        assert queued["exchange_code"] == "kraken"
        assert queued["commission_value"] == pytest.approx(0.0026)
        assert queued["work_fingerprint"]
    finally:
        connection.close()
        _restore_connection(original)


def test_backtesting_fingerprint_and_fee_context_are_exchange_specific(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        binance_settings = database.get_backtesting_settings(
            require_explicit_fee=True
        )
        binance_fingerprint = database.build_backtesting_work_fingerprint(
            "ema", False, binance_settings
        )

        database.set_active_exchange(2)
        database.set_exchange_backtesting_fee(0.0026)
        kraken_settings = database.get_backtesting_settings(
            require_explicit_fee=True
        )
        kraken_fingerprint = database.build_backtesting_work_fingerprint(
            "ema", False, kraken_settings
        )

        assert binance_fingerprint != kraken_fingerprint
        assert database.backtesting_result_matches_context(
            {
                "Backtest_Work_Fingerprint": kraken_fingerprint,
                "Backtest_Commission_Value": 0.0026,
            },
            work_fingerprint=kraken_fingerprint,
            commission_value=0.0026,
        )
        assert not database.backtesting_result_matches_context(
            {
                "Backtest_Work_Fingerprint": binance_fingerprint,
                "Backtest_Commission_Value": binance_settings["Commission_Value"],
            },
            work_fingerprint=kraken_fingerprint,
            commission_value=0.0026,
        )
    finally:
        connection.close()
        _restore_connection(original)


def test_kraken_persistence_uses_canonical_and_native_symbols(tmp_path, monkeypatch):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)

    class PublicKrakenAdapter:
        @staticmethod
        def normalize_symbol(symbol):
            assert symbol == "XBT/EUR"
            return "BTC/EUR"

        @staticmethod
        def load_markets():
            return {
                "BTC/EUR": MarketInfo(
                    "BTC/EUR", "XXBTZEUR", "BTC", "EUR", True
                )
            }

    try:
        database.set_active_exchange(2)
        monkeypatch.setattr(
            "bec.exchanges.registry.get_default_adapter",
            lambda: PublicKrakenAdapter(),
        )

        assert database._exchange_symbol_metadata("XBT/EUR") == (
            2,
            "BTC/EUR",
            "XXBTZEUR",
            "BTC",
            "EUR",
        )
    finally:
        connection.close()
        _restore_connection(original)


@pytest.mark.parametrize("status", ["pending", "open", "partially_filled", "unknown"])
def test_exchange_switch_is_blocked_by_unsettled_orders(tmp_path, status):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.execute(
            "INSERT INTO Orders (Symbol, Order_Status, Exchange_Id) VALUES ('BTCUSDC', ?, 1)",
            (status,),
        )
        connection.commit()

        with pytest.raises(ValueError, match="unsettled orders"):
            database.set_active_exchange(2)
    finally:
        connection.close()
        _restore_connection(original)


def test_exchange_switch_is_blocked_by_open_position(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.execute(
            "INSERT INTO Positions (Symbol, Position, Exchange_Id) VALUES ('BTCUSDC', 1, 1)"
        )
        connection.commit()

        with pytest.raises(ValueError, match="open_positions=1"):
            database.set_active_exchange(2)
    finally:
        connection.close()
        _restore_connection(original)


def test_runtime_queries_are_filtered_by_active_exchange(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.executemany(
            "INSERT INTO Positions (Bot, Symbol, Position, Exchange_Id) VALUES ('1h', ?, 0, ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.executemany(
            "INSERT INTO Orders (Symbol, Side, Order_Status, Exchange_Id) VALUES (?, 'BUY', 'filled', ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.commit()

        assert database.get_all_positions_by_bot("1h")["Symbol"].tolist() == [
            "BTCUSDC"
        ]
        assert database.get_all_orders()["Symbol"].tolist() == ["BTCUSDC"]

        database.set_active_exchange(2)
        assert database.get_all_positions_by_bot("1h")["Symbol"].tolist() == [
            "ETHEUR"
        ]
        assert database.get_all_orders()["Symbol"].tolist() == ["ETHEUR"]
    finally:
        connection.close()
        _restore_connection(original)


def test_rankings_balances_backtests_and_queues_are_exchange_isolated(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.execute(database.sql_create_strategies_table)
        connection.execute(
            "INSERT OR IGNORE INTO Strategies (Id, Name) VALUES ('ema', 'EMA')"
        )
        connection.executemany(
            "INSERT INTO Symbols_By_Market_Phase "
            "(Symbol, Rank, Exchange_Id) VALUES (?, 1, ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.executemany(
            "INSERT INTO Backtesting_Results "
            "(Symbol, Time_Frame, Strategy_Id, Exchange_Id) "
            "VALUES (?, '1h', 'ema', ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.executemany(
            "INSERT INTO Balances "
            "(Date, Asset, Balance_USD, Exchange_Id) "
            "VALUES ('2026-07-04', ?, 100, ?)",
            [("USDC", 1), ("EUR", 2)],
        )
        connection.executemany(
            "INSERT INTO Backtesting_Jobs "
            "(batch_id, strategy_id, symbol, timeframe, created_at, Exchange_Id) "
            "VALUES ('batch', 'ema', ?, '1h', '2026-07-04', ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.commit()

        assert database.get_all_symbols_by_market_phase()["Symbol"].tolist() == [
            "BTCUSDC"
        ]
        assert database.get_all_backtesting_results()["Symbol"].tolist() == [
            "BTCUSDC"
        ]
        assert database.get_asset_balances_all_time()["Asset"].tolist() == ["USDC"]
        assert int(database.get_backtesting_job_counts()["count"].sum()) == 1

        database.set_active_exchange(2)
        assert database.get_all_symbols_by_market_phase()["Symbol"].tolist() == [
            "ETHEUR"
        ]
        assert database.get_all_backtesting_results()["Symbol"].tolist() == [
            "ETHEUR"
        ]
        assert database.get_asset_balances_all_time()["Asset"].tolist() == ["EUR"]
        assert int(database.get_backtesting_job_counts()["count"].sum()) == 1
    finally:
        connection.close()
        _restore_connection(original)


def test_candidate_cleanup_does_not_use_other_exchange_rankings(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.execute(
            "INSERT INTO Positions (Symbol, Position, Exchange_Id) "
            "VALUES ('ONLYOTHER', 0, 1)"
        )
        connection.execute(
            "INSERT INTO Symbols_By_Market_Phase (Symbol, Exchange_Id) "
            "VALUES ('ONLYOTHER', 2)"
        )
        connection.commit()

        database.delete_positions_not_top_rank()

        assert connection.execute(
            "SELECT COUNT(*) FROM Positions "
            "WHERE Symbol='ONLYOTHER' AND Exchange_Id=1"
        ).fetchone()[0] == 0
    finally:
        connection.close()
        _restore_connection(original)


def test_runtime_writes_persist_active_exchange_metadata(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        database.add_order_buy("order-1", "2026-07-03", "1h", "BTCUSDC", 10, 2)
        database.insert_position("1h", "BTCUSDC")
        database.add_backtesting_results(
            "1h", "BTCUSDC", 1, 1, "2026-01-01", "2026-07-01", -1,
            10, 50, 2, -1, 0.5, "1d", "1h", 1.2, 0.1, 1.0, 0.2, "ema",
        )
        database.add_backtesting_trade(
            "BTCUSDC", "1h", "ema", 1, 2, 10, 11, 1, 10,
            "2026-01-01", "2026-01-02", "1d",
        )
        database.enqueue_backtesting_jobs(
            [{"strategy_id": "ema", "symbol": "BTCUSDC", "timeframe": "1h"}]
        )

        for table in (
            "Orders", "Positions", "Backtesting_Results", "Backtesting_Trades",
            "Backtesting_Jobs",
        ):
            assert connection.execute(
                f'SELECT Exchange_Id, Symbol_Normalized FROM "{table}" LIMIT 1'
            ).fetchone() == (1, "BTC/USDC")
    finally:
        connection.close()
        _restore_connection(original)


def test_exchange_settings_update_fee_quote_and_allow_no_active_exchange(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(2)

        database.update_exchange_settings(
            [
                {
                    "Id": 2,
                    "Enabled": False,
                    "Quote_Asset": "EUR",
                    "Taker_Fee": 0.0035,
                }
            ]
        )

        assert database.get_active_exchange(required=False) is None
        assert connection.execute(
            "SELECT Enabled, Is_Default, Quote_Asset FROM Exchanges WHERE Id=2"
        ).fetchone() == (0, 0, "EUR")
        assert connection.execute(
            "SELECT Commission_Value FROM Exchange_Backtesting_Settings "
            "WHERE Exchange_Id=2"
        ).fetchone()[0] == pytest.approx(0.0035)
        assert connection.execute(
            "SELECT COUNT(*) FROM Job_Schedules WHERE enabled=1"
        ).fetchone()[0] == 0
    finally:
        connection.close()
        _restore_connection(original)


@pytest.mark.parametrize("fee", [float("nan"), float("inf"), -0.001])
def test_exchange_settings_reject_invalid_fees(tmp_path, fee):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        with pytest.raises(ValueError, match="finite non-negative"):
            database.update_exchange_settings(
                [
                    {
                        "Id": 2,
                        "Enabled": True,
                        "Quote_Asset": "USDC",
                        "Taker_Fee": fee,
                    }
                ]
            )
    finally:
        connection.close()
        _restore_connection(original)


def test_okx_identities_remain_configuration_only_and_lock_variant_after_activity(
    tmp_path,
):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        rows = connection.execute(
            """
            SELECT Id, Code, Adapter_Id, Execution_Environment, Enabled, Is_Default,
                   Quote_Asset, Buy_Enabled, Sell_Enabled
            FROM Exchanges WHERE Code IN ('okx', 'okx_demo') ORDER BY Id
            """
        ).fetchall()
        assert rows == [
            (3, "okx", "myokx", "production", 0, 0, "USDC", 0, 0),
            (4, "okx_demo", "myokx", "demo", 0, 0, "USDC", 0, 0),
        ]
        assert connection.execute(
            "SELECT COUNT(*) FROM Exchange_Backtesting_Settings WHERE Exchange_Id IN (3, 4)"
        ).fetchone()[0] == 0

        with pytest.raises(ValueError, match="Load OKX public markets"):
            database.update_exchange_settings(
                [{"Id": 3, "Enabled": True, "Quote_Asset": "USDC", "Taker_Fee": None}]
            )
        with pytest.raises(ValueError, match="Load OKX public markets"):
            database.update_exchange_settings(
                [{"Id": 3, "Enabled": False, "Quote_Asset": "EUR", "Taker_Fee": None}]
            )

        database.update_exchange_settings(
            [{
                "Id": 3,
                "Enabled": False,
                "Quote_Asset": "USDC",
                "Taker_Fee": None,
                "Adapter_Id": "okx",
            }]
        )
        assert connection.execute(
            "SELECT Adapter_Id FROM Exchanges WHERE Id=3"
        ).fetchone() == ("okx",)
        assert connection.execute(
            "SELECT COUNT(*) FROM Exchange_Backtesting_Settings WHERE Exchange_Id=3"
        ).fetchone()[0] == 0

        connection.execute(
            """
            INSERT INTO Exchange_Symbols
                (Exchange_Id, Symbol_Normalized, Exchange_Symbol, Is_Tradable)
            VALUES (3, 'BTC/USDC', 'BTC-USDC', 0)
            """
        )
        connection.commit()
        with pytest.raises(ValueError, match="cannot change after exchange data"):
            database.update_exchange_settings(
                [{
                    "Id": 3,
                    "Enabled": False,
                    "Quote_Asset": "USDC",
                    "Taker_Fee": None,
                    "Adapter_Id": "myokx",
                }]
            )

        connection.execute("UPDATE Exchanges SET Enabled=1 WHERE Id=3")
        connection.commit()
        with pytest.raises(ValueError, match="Load OKX public markets"):
            database.set_active_exchange(3)
    finally:
        connection.close()
        _restore_connection(original)


def test_okx_public_catalog_validates_quote_enables_public_selection_and_marks_missing(
    tmp_path,
    monkeypatch,
):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        markets = {
            "BTC/USDC": MarketInfo(
                "BTC/USDC", "BTC-USDC", "BTC", "USDC", True,
                amount_step=Decimal("0.0001"), price_step=Decimal("0.1"),
                min_amount=Decimal("0.0002"), max_amount=Decimal("10"),
                min_cost=Decimal("5"), max_cost=Decimal("100000"),
                market_type="spot", spot=True, contract=False,
                raw={"instId": "BTC-USDC"},
            ),
            "DOGE/USDC": MarketInfo(
                "DOGE/USDC", "DOGE-USDC", "DOGE", "USDC", True,
                amount_step=Decimal("1"), price_step=Decimal("0.00001"),
                min_amount=Decimal("1"), min_cost=Decimal("1"),
                market_type="spot", spot=True, contract=False,
                raw={"instId": "DOGE-USDC"},
            ),
        }
        assert database.sync_exchange_market_catalog(3, markets) == {"upserted": 2}
        assert database.exchange_quote_has_validated_markets(3, "USDC") is True
        assert connection.execute(
            """
            SELECT Symbol_Normalized, Exchange_Symbol, Amount_Step, Min_Cost,
                   Is_Spot, Is_Contract, Availability_Status
            FROM Exchange_Symbols WHERE Exchange_Id=3 AND Exchange_Symbol='BTC-USDC'
            """
        ).fetchone() == ("BTC/USDC", "BTC-USDC", 0.0001, 5.0, 1, 0, "available")

        database.update_exchange_settings(
            [{"Id": 3, "Enabled": True, "Quote_Asset": "USDC", "Taker_Fee": None}]
        )
        selected = database.set_active_exchange(3)
        assert selected["code"] == "okx"
        assert connection.execute(
            "SELECT COUNT(*) FROM Exchange_Backtesting_Settings WHERE Exchange_Id=3"
        ).fetchone()[0] == 0
        with pytest.raises(RuntimeError, match="explicit backtesting fee"):
            database.require_backtesting_execution_available()
        database.set_exchange_backtesting_fee(0.001, exchange_id=3)
        assert database.require_backtesting_execution_available()["code"] == "okx"
        monkeypatch.setattr(
            database,
            "_exchange_symbol_metadata",
            lambda symbol: (3, "BTC/USDC", "BTC-USDC", "BTC", "USDC"),
        )
        database.add_backtesting_results(
            timeframe="1h",
            symbol="BTC/USDC",
            return_perc=1,
            buy_hold_return_perc=1,
            backtest_start_date="2026-01-01",
            backtest_end_date="2026-01-02",
            max_drawdown_perc=1,
            trades=1,
            win_rate_perc=100,
            best_trade_perc=1,
            worst_trade_perc=1,
            avg_trade_perc=1,
            max_trade_duration="1h",
            avg_trade_duration="1h",
            profit_factor=1,
            expectancy_perc=1,
            sqn=1,
            kelly_criterion=1,
            strategy_Id="ema",
            backtest_work_fingerprint="okx-work",
            backtest_commission_value=0.001,
        )
        assert connection.execute(
            """
            SELECT Backtest_Adapter_Id, Backtest_Quote_Asset,
                   Backtest_Execution_Environment
            FROM Backtesting_Results WHERE Exchange_Id=3
            """
        ).fetchone() == ("myokx", "USDC", "production")
        queued = database.enqueue_backtesting_jobs(
            [{"strategy_id": "ema", "symbol": "BTC/USDC", "timeframe": "1h"}]
        )["queued"][0]
        assert queued["exchange_adapter_id"] == "myokx"
        assert queued["exchange_quote_asset"] == "USDC"
        claimed = database.claim_next_backtesting_job()
        assert claimed["exchange_code"] == "okx"
        assert claimed["exchange_adapter_id"] == "myokx"
        assert claimed["exchange_quote_asset"] == "USDC"
        monkeypatch.setattr(
            database,
            "_exchange_symbol_metadata",
            lambda symbol: (_ for _ in ()).throw(ValueError("Unknown OKX spot symbol")),
        )
        unavailable = database.enqueue_backtesting_jobs(
            [{"strategy_id": "ema", "symbol": "NOT/USDC", "timeframe": "1h"}]
        )
        assert unavailable["queued"] == []
        assert unavailable["skipped"][0]["reason"] == "unavailable_pair"
        original_fingerprint = queued["work_fingerprint"]
        connection.execute("UPDATE Exchanges SET Adapter_Id='okx' WHERE Id=3")
        connection.commit()
        assert database.build_backtesting_work_fingerprint("ema", False) != original_fingerprint
        assert not database.backtesting_result_matches_context(
            {
                "Backtest_Work_Fingerprint": original_fingerprint,
                "Backtest_Commission_Value": 0.001,
                "Backtest_Adapter_Id": "myokx",
                "Backtest_Quote_Asset": "USDC",
                "Backtest_Execution_Environment": "production",
            },
            work_fingerprint=original_fingerprint,
            commission_value=0.001,
        )
        enabled_jobs = {
            row[0]
            for row in connection.execute("SELECT name FROM Job_Schedules WHERE enabled=1")
        }
        assert enabled_jobs == set(database.PUBLIC_ANALYSIS_JOBS)

        database.sync_exchange_market_catalog(3, {"BTC/USDC": markets["BTC/USDC"]})
        assert connection.execute(
            "SELECT Is_Tradable, Availability_Status FROM Exchange_Symbols "
            "WHERE Exchange_Id=3 AND Exchange_Symbol='DOGE-USDC'"
        ).fetchone() == (0, "missing")
    finally:
        connection.close()
        _restore_connection(original)


def _kraken_order_result(
    status,
    executed,
    fee="0",
    exchange_order_id="K-1",
    *,
    side="buy",
    fee_asset="BTC",
):
    return OrderResult(
        exchange_order_id=exchange_order_id,
        symbol="BTC/USDC",
        exchange_symbol="XBTUSDC",
        side=side,
        status=status,
        requested_quantity=Decimal("1"),
        executed_quantity=Decimal(str(executed)),
        average_price=Decimal("100"),
        fills=(
            OrderFill(
                price=Decimal("100"),
                quantity=Decimal(str(executed)),
                fee_asset=fee_asset,
                fee_amount=Decimal(str(fee)),
            ),
        ),
        client_order_id="bec-test",
        timestamp=datetime(2026, 7, 5, tzinfo=timezone.utc),
        raw={"id": exchange_order_id, "status": status.value},
    )


def test_order_intent_is_persisted_before_submission_and_never_resubmitted(
    tmp_path, monkeypatch
):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(2)
        monkeypatch.setattr(
            database,
            "_exchange_symbol_metadata",
            lambda symbol: (2, "BTC/USDC", "XBTUSDC", "BTC", "USDC"),
        )
        position_id = connection.execute(
            "INSERT INTO Positions (Symbol, Position, Exchange_Id) VALUES ('BTC/USDC',0,2)"
        ).lastrowid
        connection.commit()

        intent = database.create_order_intent(
            side="BUY",
            symbol="BTC/USDC",
            bot="1h",
            position_id=position_id,
            requested_quote_qty=100,
            client_order_id="bec-test",
        )

        assert intent["Intent_State"] == "created"
        assert intent["Order_Status"] == "pending"
        database.mark_order_intent_submitting(intent["Id"])
        with pytest.raises(RuntimeError, match="already been submitted"):
            database.mark_order_intent_submitting(intent["Id"])
        with pytest.raises(RuntimeError, match="unsettled order intent"):
            database.create_order_intent(
                side="BUY",
                symbol="BTC/USDC",
                bot="1h",
                position_id=position_id,
                requested_quote_qty=100,
            )
    finally:
        connection.close()
        _restore_connection(original)


def test_invalid_client_order_id_is_rejected_before_intent_persistence(tmp_path, monkeypatch):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(2)
        monkeypatch.setattr(
            database,
            "_exchange_symbol_metadata",
            lambda symbol: (2, "BTC/USDC", "XBTUSDC", "BTC", "USDC"),
        )
        monkeypatch.setattr(
            service,
            "validate_client_order_id",
            lambda value: (_ for _ in ()).throw(ValueError("invalid client id")),
        )

        with pytest.raises(ValueError, match="invalid client id"):
            database.create_order_intent(
                side="BUY",
                symbol="BTC/USDC",
                bot="1h",
                requested_quote_qty=100,
            )

        assert connection.execute("SELECT COUNT(*) FROM Orders").fetchone()[0] == 0
    finally:
        connection.close()
        _restore_connection(original)


def test_partial_buy_reconciliation_applies_only_incremental_net_quantity(
    tmp_path, monkeypatch
):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(2)
        monkeypatch.setattr(
            database,
            "_exchange_symbol_metadata",
            lambda symbol: (2, "BTC/USDC", "XBTUSDC", "BTC", "USDC"),
        )
        position_id = connection.execute(
            "INSERT INTO Positions (Symbol, Position, Exchange_Id) VALUES ('BTC/USDC',0,2)"
        ).lastrowid
        connection.commit()
        intent = database.create_order_intent(
            side="BUY",
            symbol="BTC/USDC",
            bot="1h",
            position_id=position_id,
            requested_quote_qty=100,
            client_order_id="bec-test",
        )
        database.mark_order_intent_submitting(intent["Id"])

        database.apply_order_result(
            intent["Id"],
            _kraken_order_result(OrderStatus.PARTIALLY_FILLED, "0.5", "0.01"),
        )
        database.apply_order_result(
            intent["Id"], _kraken_order_result(OrderStatus.FILLED, "1", "0.02")
        )
        repeated = database.apply_order_result(
            intent["Id"], _kraken_order_result(OrderStatus.FILLED, "1", "0.02")
        )

        assert repeated["delta_executed_qty"] == 0
        assert connection.execute(
            "SELECT Position, Qty, Buy_Order_Id FROM Positions WHERE Id=?",
            (position_id,),
        ).fetchone() == (1, pytest.approx(0.98), "K-1")
        assert connection.execute(
            "SELECT Order_Status, Intent_State, Executed_Qty, Fee_Asset, Fee_Amount, Net_Qty "
            "FROM Orders WHERE Id=?",
            (intent["Id"],),
        ).fetchone() == (
            "filled",
            "settled",
            pytest.approx(1.0),
            "BTC",
            pytest.approx(0.02),
            pytest.approx(0.98),
        )
    finally:
        connection.close()
        _restore_connection(original)


def test_durable_fills_support_fee_correction_and_multiple_assets(tmp_path, monkeypatch):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(2)
        monkeypatch.setattr(
            database,
            "_exchange_symbol_metadata",
            lambda symbol: (2, "BTC/USDC", "XBTUSDC", "BTC", "USDC"),
        )
        position_id = connection.execute(
            "INSERT INTO Positions (Symbol, Position, Exchange_Id) VALUES ('BTC/USDC',0,2)"
        ).lastrowid
        connection.commit()
        intent = database.create_order_intent(
            side="BUY",
            symbol="BTC/USDC",
            bot="1h",
            position_id=position_id,
            requested_quote_qty=100,
            client_order_id="bec-durable-fills",
        )
        database.mark_order_intent_submitting(intent["Id"])

        first = _kraken_order_result(OrderStatus.PARTIALLY_FILLED, "1", "0")
        first = OrderResult(
            **{
                **first.__dict__,
                "fills": (
                    OrderFill(
                        price=Decimal("100"), quantity=Decimal("1"),
                        fee_asset="BTC", fee_amount=Decimal("0"), trade_id="trade-1",
                    ),
                ),
            }
        )
        database.apply_order_result(intent["Id"], first)

        corrected = OrderResult(
            **{
                **first.__dict__,
                "status": OrderStatus.FILLED,
                "fills": (
                    OrderFill(
                        price=Decimal("100"), quantity=Decimal("1"),
                        fee_asset="BTC", fee_amount=Decimal("0.01"), trade_id="trade-1",
                    ),
                    OrderFill(
                        price=Decimal("100"), quantity=Decimal("1"),
                        fee_asset="USDC", fee_amount=Decimal("1"), trade_id="fee-quote",
                    ),
                ),
            }
        )
        updated = database.apply_order_result(intent["Id"], corrected)
        repeated = database.apply_order_result(intent["Id"], corrected)

        assert updated["delta_executed_qty"] == 0
        assert repeated["delta_executed_qty"] == 0
        assert connection.execute(
            "SELECT Qty FROM Positions WHERE Id=?", (position_id,)
        ).fetchone()[0] == pytest.approx(0.99)
        assert connection.execute(
            "SELECT COUNT(*) FROM Order_Fills WHERE Order_Id=?", (intent["Id"],)
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT Fee_Asset, Fee_Amount, Executed_Cost, Fees_JSON FROM Orders WHERE Id=?",
            (intent["Id"],),
        ).fetchone() == ("", 0.0, pytest.approx(100.0), '{"BTC":0.01,"USDC":1.0}')
    finally:
        connection.close()
        _restore_connection(original)


def test_partial_sell_reconciliation_decrements_position_once_and_keeps_remainder(
    tmp_path, monkeypatch
):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(2)
        monkeypatch.setattr(
            database,
            "_exchange_symbol_metadata",
            lambda symbol: (2, "BTC/USDC", "XBTUSDC", "BTC", "USDC"),
        )
        connection.execute(
            """
            INSERT INTO Orders
                (Exchange_Id, Exchange_Order_Id, Symbol, Side, Price, Qty,
                 Order_Status, Executed_Qty, Average_Price, Fee_Amount, Net_Qty)
            VALUES (2, 'BUY-1', 'BTC/USDC', 'BUY', 80, 1, 'filled', 1, 80, 0, 1)
            """
        )
        position_id = connection.execute(
            """
            INSERT INTO Positions
                (Symbol, Position, Exchange_Id, Qty, Buy_Price, Buy_Order_Id, Bot)
            VALUES ('BTC/USDC',1,2,1,80,'BUY-1','1h')
            """
        ).lastrowid
        connection.commit()
        intent = database.create_order_intent(
            side="SELL",
            symbol="BTC/USDC",
            bot="1h",
            position_id=position_id,
            requested_qty=0.5,
            sell_percentage=50,
            client_order_id="bec-sell",
        )
        database.mark_order_intent_submitting(intent["Id"])
        result = _kraken_order_result(
            OrderStatus.FILLED,
            "0.5",
            "1",
            "SELL-1",
            side="sell",
            fee_asset="USDC",
        )

        first = database.apply_order_result(intent["Id"], result)
        repeated = database.apply_order_result(intent["Id"], result)

        assert first["closed_position"] is False
        assert repeated["delta_executed_qty"] == 0
        assert connection.execute(
            "SELECT Position, Qty FROM Positions WHERE Id=?", (position_id,)
        ).fetchone() == (1, pytest.approx(0.5))
        assert connection.execute(
            "SELECT Buy_Order_Id, PnL_Value, Fee_Asset, Fee_Amount FROM Orders WHERE Id=?",
            (intent["Id"],),
        ).fetchone() == ("1", pytest.approx(9.0), "USDC", pytest.approx(1.0))

        canceled_intent = database.create_order_intent(
            side="SELL",
            symbol="BTC/USDC",
            bot="1h",
            position_id=position_id,
            requested_qty=0.5,
            sell_percentage=100,
            client_order_id="bec-canceled",
        )
        database.mark_order_intent_submitting(canceled_intent["Id"])
        canceled = database.apply_order_result(
            canceled_intent["Id"],
            _kraken_order_result(
                OrderStatus.CANCELED,
                "0.1",
                "0",
                "SELL-CANCELED",
                side="sell",
                fee_asset="USDC",
            ),
        )
        assert canceled["closed_position"] is False
        assert connection.execute(
            "SELECT Position, Qty FROM Positions WHERE Id=?", (position_id,)
        ).fetchone() == (1, pytest.approx(0.4))

        closing_intent = database.create_order_intent(
            side="SELL",
            symbol="BTC/USDC",
            bot="1h",
            position_id=position_id,
            requested_qty=0.4,
            sell_percentage=100,
            client_order_id="bec-close",
        )
        database.mark_order_intent_submitting(closing_intent["Id"])
        closed = database.apply_order_result(
            closing_intent["Id"],
            _kraken_order_result(
                OrderStatus.FILLED,
                "0.4",
                "0",
                "SELL-2",
                side="sell",
                fee_asset="USDC",
            ),
        )

        assert closed["closed_position"] is True
        assert connection.execute(
            "SELECT Position, Qty, Buy_Order_Id FROM Positions WHERE Id=?",
            (position_id,),
        ).fetchone() == (0, 0.0, None)
    finally:
        connection.close()
        _restore_connection(original)
