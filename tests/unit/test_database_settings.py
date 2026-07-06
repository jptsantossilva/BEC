import sqlite3

import pytest

import bec.utils.config as config
import bec.utils.database as database
from bec.db.exchange_schema import apply_exchange_aware_schema, create_exchange_metadata
from bec.strategy_builder.templates import get_builtin_template


def test_app_settings_load_without_legacy_take_profit_enabled():
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(":memory:", check_same_thread=False)
    test_conn.execute(database.sql_create_settings_table)
    test_conn.execute(database.sql_create_strategies_table)
    database._thread_local.conn = test_conn
    config._invalidate_settings_cache()

    try:
        settings = config.load_settings(refresh=True)

        assert settings.trade_against == "USDC"
        assert not hasattr(settings, "take_profit_enabled")
        assert database.setting_exists("take_profit_enabled") is False
    finally:
        config._invalidate_settings_cache()
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_backtesting_settings_include_monte_carlo_candle_perturbation(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)

    database._thread_local.conn = test_conn
    try:
        database.ensure_backtesting_settings()
        settings = database.get_backtesting_settings()
        assert settings["Monte_Carlo_Candle_Perturb_Min_Pct"] == 0.1
        assert settings["Monte_Carlo_Candle_Perturb_Max_Pct"] == 0.5
        assert settings["Backtest_Use_Full_History"] is False
        assert settings["Backtest_Lookback_1d_Years"] == 4
        assert settings["Backtest_Lookback_4h_Months"] == 18
        assert settings["Backtest_Lookback_1h_Months"] == 12
        assert settings["Backtest_Warmup_Candles"] == 200

        database.update_backtesting_settings(
            commission_value=settings["Commission_Value"],
            cash_value=settings["Cash_Value"],
            maximize=settings["Maximize"],
            buy_hold_start_mode=settings["Buy_Hold_Start_Mode"],
            optimization_max_combinations=settings["Optimization_Max_Combinations"],
            candidate_backtest_refresh_days=settings[
                "Candidate_Backtest_Refresh_Days"
            ],
            backtest_use_full_history=True,
            backtest_lookback_1d_years=5,
            backtest_lookback_4h_months=24,
            backtest_lookback_1h_months=18,
            backtest_warmup_candles=400,
            strategy_quality_return_weight=settings[
                "Strategy_Quality_Return_Weight"
            ],
            strategy_quality_risk_weight=settings["Strategy_Quality_Risk_Weight"],
            strategy_quality_risk_adjusted_weight=settings[
                "Strategy_Quality_Risk_Adjusted_Weight"
            ],
            strategy_quality_trade_quality_weight=settings[
                "Strategy_Quality_Trade_Quality_Weight"
            ],
            strategy_quality_robustness_weight=settings[
                "Strategy_Quality_Robustness_Weight"
            ],
            monte_carlo_candle_perturb_min_pct=0.2,
            monte_carlo_candle_perturb_max_pct=0.8,
        )

        updated = database.get_backtesting_settings()
        assert updated["Monte_Carlo_Candle_Perturb_Min_Pct"] == 0.2
        assert updated["Monte_Carlo_Candle_Perturb_Max_Pct"] == 0.8
        assert updated["Backtest_Use_Full_History"] is True
        assert updated["Backtest_Lookback_1d_Years"] == 5
        assert updated["Backtest_Lookback_4h_Months"] == 24
        assert updated["Backtest_Lookback_1h_Months"] == 18
        assert updated["Backtest_Warmup_Candles"] == 400
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_new_install_disables_exchange_dependent_jobs(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_job_schedules_table)
    create_exchange_metadata(test_conn, upgraded_install=False)
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database.ensure_job_schedules()
        disabled = {
            row[0]
            for row in test_conn.execute(
                "SELECT name FROM Job_Schedules WHERE enabled=0"
            ).fetchall()
        }
        assert disabled == {
            "main_1h",
            "main_4h",
            "main_1d",
            "symbol_by_market_phase_1d",
            "super_rsi_15m",
            "kraken_reconcile_15m",
        }
        assert test_conn.execute(
            "SELECT COUNT(*) FROM Exchanges WHERE Enabled=1 OR Is_Default=1"
        ).fetchone()[0] == 0
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_upgraded_install_keeps_binance_active_and_jobs_enabled(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_job_schedules_table)
    create_exchange_metadata(test_conn, upgraded_install=True)
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database.ensure_job_schedules()

        assert database.get_active_exchange(required=True)["code"] == "binance"
        expected_enabled = tuple(
            name
            for name in database.EXCHANGE_DEPENDENT_JOBS
            if name != database.RECONCILIATION_JOB
        )
        placeholders = ",".join("?" for _ in expected_enabled)
        enabled = test_conn.execute(
            f"SELECT COUNT(*) FROM Job_Schedules "
            f"WHERE enabled=1 AND name IN ({placeholders})",
            expected_enabled,
        ).fetchone()[0]
        assert enabled == len(expected_enabled)
        assert database.get_job_schedule_enabled(database.RECONCILIATION_JOB) is False
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_builtin_strategy_template_seed_updates_existing_builtin_rows(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_strategies_table)
    test_conn.execute(
        """
        INSERT INTO Strategies (Id, Name, Type, Status, Definition_JSON)
        VALUES ('ema_cross', 'EMA Cross', 'builtin', 'approved', NULL)
        """
    )
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database.seed_builtin_strategy_templates(test_conn)
        row = test_conn.execute(
            "SELECT Type, Status, Definition_JSON FROM Strategies WHERE Id = 'ema_cross'"
        ).fetchone()
        assert row[0] == "custom"
        assert row[1] == "approved"
        assert '"engine":"bec_strategy_ast_v2"' in row[2]
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_builtin_strategy_seed_creates_weekly_btc_strategies(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_strategies_table)
    for statement in database.sql_strategies_add_default_strategies.split(";"):
        if statement.strip():
            test_conn.execute(statement)
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database.seed_builtin_strategy_templates(test_conn)
        rows = test_conn.execute(
            """
            SELECT Id, Type, Status, Backtest_Optimize, Main_Strategy, BTC_Strategy, Definition_JSON
            FROM Strategies
            WHERE Id IN ('bullmarketsupportband', 'wema20')
            ORDER BY Id
            """
        ).fetchall()

        assert [row[0] for row in rows] == ["bullmarketsupportband", "wema20"]
        for row in rows:
            assert row[1] == "custom"
            assert row[2] == "approved"
            assert row[3] == 0
            assert row[4] == 0
            assert row[5] == 1
            assert '"timeframe":"1w"' in row[6]
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_dual_momentum_simple_seed_is_available_for_main_only(tmp_path):
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_strategies_table)
    for statement in database.sql_strategies_add_default_strategies.split(";"):
        if statement.strip():
            test_conn.execute(statement)

    database.seed_builtin_strategy_templates(test_conn)

    row = test_conn.execute(
        """
        SELECT Type, Status, Backtest_Optimize, Main_Strategy, BTC_Strategy,
               Definition_JSON
        FROM Strategies
        WHERE Id = 'dual_momentum_simple'
        """
    ).fetchone()
    assert row[:5] == ("custom", "approved", 1, 1, 0)
    assert '"engine":"bec_strategy_ast_v2"' in row[5]
    assert '"rules":[]' in row[5]
    test_conn.close()


def test_approved_strategy_reuses_working_copy_draft_version(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_strategies_table)
    database._thread_local.conn = test_conn

    source_id = "approved_strategy"
    first_definition = get_builtin_template("dual_momentum_simple")
    second_definition = get_builtin_template("dual_momentum_simple")
    second_definition["description"] = "Updated working copy"

    try:
        database.upsert_custom_strategy(
            strategy_id=source_id,
            name="Approved Strategy",
            definition=first_definition,
            status="approved",
        )

        first_draft_id = database.get_or_create_strategy_draft_version(
            source_id,
            first_definition,
        )
        second_draft_id = database.get_or_create_strategy_draft_version(
            source_id,
            second_definition,
        )

        assert second_draft_id == first_draft_id
        rows = test_conn.execute(
            """
            SELECT Id, Status, Parent_Strategy_Id, Version, Definition_JSON
            FROM Strategies
            WHERE Parent_Strategy_Id = ? AND Version = 2
            """,
            (source_id,),
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == first_draft_id
        assert rows[0][1:4] == ("draft", source_id, 2)
        assert "Updated working copy" in rows[0][4]

        database.approve_strategy_for_live(first_draft_id)
        third_draft_id = database.get_or_create_strategy_draft_version(
            first_draft_id,
            second_definition,
        )
        third_draft = database.get_strategy_by_id(third_draft_id).iloc[0]
        assert third_draft_id == f"{source_id}_v3"
        assert third_draft["Name"] == "Approved Strategy v3"
        assert third_draft["Parent_Strategy_Id"] == source_id
        assert third_draft["Version"] == 3
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_symbols_by_market_phase_migration_adds_roc_columns(tmp_path):
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(
        """
        CREATE TABLE Symbols_By_Market_Phase (
            Id INTEGER PRIMARY KEY,
            Symbol TEXT,
            Rank INTEGER
        )
        """
    )
    test_conn.execute(
        """
        CREATE TABLE Symbols_By_Market_Phase_Historical (
            Id INTEGER PRIMARY KEY,
            Symbol TEXT,
            Rank INTEGER,
            Date_Inserted TEXT
        )
        """
    )

    database._ensure_symbols_by_market_phase_columns(test_conn)

    for table_name in (
        "Symbols_By_Market_Phase",
        "Symbols_By_Market_Phase_Historical",
    ):
        columns = {
            row[1] for row in test_conn.execute(f"PRAGMA table_info({table_name})")
        }
        assert {"ROC_30", "ROC_60"}.issubset(columns)

    test_conn.close()


def test_symbols_by_market_phase_persists_roc_columns_to_history(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_symbols_by_market_phase_table)
    test_conn.execute(database.sql_create_symbols_by_market_phase_historical_table)
    test_conn.execute("BEGIN IMMEDIATE")
    apply_exchange_aware_schema(test_conn, upgraded_install=True)
    test_conn.commit()
    database._thread_local.conn = test_conn

    try:
        database.insert_symbols_by_market_phase(
            "TESTUSDC",
            10.0,
            9.0,
            8.0,
            "bullish",
            11.0,
            25.0,
            0.30,
            0.60,
            1,
        )
        database.insert_symbols_by_market_phase_historical("2026-06-01")

        assert test_conn.execute(
            "SELECT ROC_30, ROC_60 FROM Symbols_By_Market_Phase"
        ).fetchone() == (0.30, 0.60)
        assert test_conn.execute(
            "SELECT ROC_30, ROC_60 FROM Symbols_By_Market_Phase_Historical"
        ).fetchone() == (0.30, 0.60)
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_builtin_position_snapshot_migration_preserves_executed_take_profits(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_settings_table)
    test_conn.executemany(
        "INSERT INTO Settings (name, value) VALUES (?, ?)",
        [
            ("stop_loss", "7.5"),
            ("atr_trailing_enabled", "false"),
            ("take_profit_enabled", "true"),
            ("take_profits_json", '[{"level":1,"pnl_pct":8,"amount_pct":25},{"level":2,"pnl_pct":12,"amount_pct":50}]'),
        ],
    )
    test_conn.execute(database.sql_create_strategies_table)
    for statement in database.sql_strategies_add_default_strategies.split(";"):
        if statement.strip():
            test_conn.execute(statement)
    database.seed_builtin_strategy_templates(test_conn)
    test_conn.execute(database.sql_create_positions_table)
    test_conn.execute(
        """
        INSERT INTO Positions (
            Bot, Symbol, Position, Strategy_Id, Strategy_Name,
            Strategy_Params_JSON, Take_Profits_JSON
        )
        VALUES ('1h', 'TESTUSDC', 1, 'ema_cross', 'EMA Cross',
                '{"ema_fast":30,"ema_slow":90}', '[{"level":1}]')
        """
    )
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database._ensure_positions_columns(test_conn)
        strategy_params_json, take_profits_json = test_conn.execute(
            """
            SELECT Strategy_Params_JSON, Take_Profits_JSON
            FROM Positions
            WHERE Symbol = 'TESTUSDC'
            """
        ).fetchone()
        params = database.parse_strategy_params(strategy_params_json)
        assert params["engine"] == "bec_strategy_ast_v2"
        assert params["parameters"]["ema_fast"] == 30
        assert params["parameters"]["ema_slow"] == 90
        assert params["definition"]["engine"] == "bec_strategy_ast_v2"
        assert params["risk"] == database.get_strategy_risk("ema_cross")
        assert take_profits_json == '[{"level":1}]'
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_positions_migration_moves_legacy_take_profit_flags_to_json(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_settings_table)
    test_conn.execute(database.sql_create_strategies_table)
    test_conn.execute(database.sql_create_positions_table)
    for level in range(1, 5):
        test_conn.execute(
            f"ALTER TABLE Positions ADD COLUMN Take_Profit_{level} INTEGER NOT NULL DEFAULT 0"
        )
    test_conn.execute(
        """
        INSERT INTO Positions (
            Bot, Symbol, Position, Strategy_Id, Strategy_Name,
            Take_Profits_JSON, Take_Profit_1, Take_Profit_2,
            Take_Profit_3, Take_Profit_4
        )
        VALUES ('1h', 'LEGACYUSDC', 1, 'ema_cross', 'EMA Cross',
                '[]', 1, 0, 1, 0)
        """
    )
    test_conn.execute(
        """
        INSERT INTO Positions (
            Bot, Symbol, Position, Strategy_Id, Strategy_Name,
            Take_Profits_JSON, Take_Profit_1
        )
        VALUES ('1h', 'JSONUSDC', 1, 'ema_cross', 'EMA Cross',
                '[{"level":2}]', 1)
        """
    )
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database._ensure_positions_columns(test_conn)
        rows = dict(
            test_conn.execute(
                "SELECT Symbol, Take_Profits_JSON FROM Positions"
            ).fetchall()
        )
        assert rows["LEGACYUSDC"] == "[1,3]"
        assert rows["JSONUSDC"] == '[{"level":2}]'

        columns = {
            row[1] for row in test_conn.execute("PRAGMA table_info(Positions)")
        }
        assert "Take_Profit_1" not in columns
        assert "Take_Profit_2" not in columns
        assert "Take_Profit_3" not in columns
        assert "Take_Profit_4" not in columns
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_delete_inactive_position_candidates_preserves_only_active_pending_rows(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_positions_table)
    test_conn.execute(database.sql_create_locked_values_table)
    test_conn.execute("BEGIN IMMEDIATE")
    apply_exchange_aware_schema(test_conn, upgraded_install=True)
    test_conn.commit()
    test_conn.executemany(
        """
        INSERT INTO Positions (Bot, Symbol, Position, Strategy_Id, Strategy_Name)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("1h", "ACTIVEUSDC", 0, "hma_rsi_linreg_copy", "HMA RSI LINREG Copy"),
            ("1h", "OLDUSDC", 0, "hma_rsi_linreg", "HMA RSI LINREG"),
            ("1h", "BLANKUSDC", 0, "", ""),
            ("1h", "OPENUSDC", 1, "hma_rsi_linreg", "HMA RSI LINREG"),
        ],
    )
    old_position_id = test_conn.execute(
        "SELECT Id FROM Positions WHERE Symbol = 'OLDUSDC'"
    ).fetchone()[0]
    test_conn.execute(
        """
        INSERT INTO Locked_Values
            (Position_Id, Buy_Order_Id, Locked_Amount, Released)
        VALUES (?, 'released-order', 12.5, 1)
        """,
        (old_position_id,),
    )
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database.delete_inactive_position_candidates(["hma_rsi_linreg_copy"])
        rows = test_conn.execute(
            "SELECT Symbol, Position, Strategy_Id FROM Positions ORDER BY Symbol"
        ).fetchall()
        assert rows == [
            ("ACTIVEUSDC", 0, "hma_rsi_linreg_copy"),
            ("OPENUSDC", 1, "hma_rsi_linreg"),
        ]
        assert (
            test_conn.execute("SELECT COUNT(*) FROM Locked_Values").fetchone()[0]
            == 0
        )
        assert test_conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_position_cleanup_refuses_to_orphan_active_locked_values(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_positions_table)
    test_conn.execute(database.sql_create_locked_values_table)
    test_conn.execute("BEGIN IMMEDIATE")
    apply_exchange_aware_schema(test_conn, upgraded_install=True)
    test_conn.commit()
    position_id = test_conn.execute(
        """
        INSERT INTO Positions (Bot, Symbol, Position, Strategy_Id)
        VALUES ('1h', 'LOCKEDUSDC', 0, 'inactive')
        RETURNING Id
        """
    ).fetchone()[0]
    test_conn.execute(
        """
        INSERT INTO Locked_Values
            (Position_Id, Buy_Order_Id, Locked_Amount, Released)
        VALUES (?, 'active-order', 25, 0)
        """,
        (position_id,),
    )
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        with pytest.raises(sqlite3.IntegrityError, match="active locked value"):
            database.delete_inactive_position_candidates([])

        assert test_conn.execute("SELECT COUNT(*) FROM Positions").fetchone()[0] == 1
        assert (
            test_conn.execute("SELECT COUNT(*) FROM Locked_Values").fetchone()[0]
            == 1
        )
        assert test_conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn
