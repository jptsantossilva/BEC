import streamlit as st
import time
import pandas as pd

import utils.database as database
import utils.icons as icons

MAXIMIZE_OPTIONS = {
    "SQN": "Van Tharp's [System Quality Number](https://www.google.com/search?q=System+Quality+Number).",
    "Equity Final [$]": "Maximizes final equity (more aggressive).",
    "Calmar Ratio": "Favors return with drawdown control.",
    "Sharpe Ratio": "Favors return per unit of volatility.",
    "Sortino Ratio": "Like Sharpe but penalizes downside more.",
}

QUALITY_GRADE_VALUES = {"A", "B", "C", "D", "F"}

BUY_HOLD_START_MODE_OPTIONS = {
    "indicator_warmup": "After indicator warm-up",
    "full_period": "From first data candle",
}

QUALITY_GRADE_SCALE = [
    {"Grade": "A", "Quality Score": ">= 85", "Interpretation": "Excellent"},
    {"Grade": "B", "Quality Score": "70 - 84.99", "Interpretation": "Strong"},
    {"Grade": "C", "Quality Score": "55 - 69.99", "Interpretation": "Acceptable / moderate"},
    {"Grade": "D", "Quality Score": "40 - 54.99", "Interpretation": "Weak"},
    {"Grade": "F", "Quality Score": "< 40", "Interpretation": "Rejectable"},
]


def approval_rules_section():
    st.subheader("Approval Rules")
    st.caption(
        "These rules decide if a symbol is approved or rejected for trading after backtesting. "
        "Enable rules and set minimum/maximum values per timeframe. Timeframe overrides global rules."
    )

    rule_defs = database.get_Approval_Rule_Definitions()
    rules_df = database.get_Backtest_Approval_Rules()

    if rule_defs.empty:
        st.info("No rule definitions found.")
        return

    rule_names = rule_defs["rule_name"].tolist()
    desc_map = dict(zip(rule_defs["rule_name"], rule_defs["description"].fillna("")))

    if rules_df.empty:
        rules_df = rules_df.reindex(columns=["rule_name", "description", "rule_value", "timeframe", "enabled"]).dropna(how="all")

    rules_df = rules_df.copy()
    if "rule_name" in rules_df.columns:
        rules_df["description"] = rules_df["rule_name"].map(desc_map).fillna(rules_df.get("description", ""))
    rules_df["timeframe"] = rules_df["timeframe"].fillna("global")
    rules_df["enabled"] = rules_df["enabled"].fillna(0).astype(bool)

    timeframe_options = ["global", "1h", "4h", "1d"]

    editable_rules = rules_df[["rule_name", "description", "rule_value", "timeframe", "enabled"]].copy()

    for column in ["rule_name", "description", "rule_value", "timeframe", "enabled"]:
        if column not in editable_rules.columns:
            editable_rules[column] = None

    editable_rules["description"] = editable_rules["rule_name"].map(desc_map).fillna("")
    editable_rules["timeframe"] = editable_rules["timeframe"].fillna("global")
    editable_rules["enabled"] = editable_rules["enabled"].fillna(1).astype(bool)
    editable_rules["rule_value"] = editable_rules["rule_value"].apply(
        lambda value: "" if pd.isna(value) else str(value)
    )

    edited_rules = st.data_editor(
        editable_rules,
        num_rows="dynamic",
        width="content",
        column_config={
            "rule_name": st.column_config.SelectboxColumn("Rule", options=rule_names),
            "description": st.column_config.TextColumn("Description"),
            "rule_value": st.column_config.TextColumn("Value"),
            "timeframe": st.column_config.SelectboxColumn("Timeframe", options=timeframe_options),
            "enabled": st.column_config.CheckboxColumn("Enabled"),
        },
        disabled=["description"],
        key="backtest_rules_editor",
    )

    rules_buttons_container = st.container(horizontal=True)
    after_rules_buttons_container = st.container()
    with rules_buttons_container:

        if st.button("Save Rules", icon=icons.ICON_SAVE):
            normalized = edited_rules.copy()
            normalized["timeframe"] = normalized["timeframe"].fillna("global")
            normalized["enabled"] = normalized["enabled"].fillna(True).astype(bool)
            missing_required = (
                normalized["rule_name"].isna()
                | (normalized["rule_name"].astype(str).str.strip() == "")
                | normalized["rule_value"].isna()
                | (normalized["rule_value"].astype(str).str.strip() == "")
            )
            if missing_required.any():
                st.error("Rule and Value are required for each row.")
                st.stop()

            grade_mask = normalized["rule_name"] == "Quality_Grade_Min"
            if grade_mask.any():
                normalized.loc[grade_mask, "rule_value"] = (
                    normalized.loc[grade_mask, "rule_value"]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                )
                invalid_grade = ~normalized.loc[grade_mask, "rule_value"].isin(
                    QUALITY_GRADE_VALUES
                )
                if invalid_grade.any():
                    st.error("Quality_Grade_Min must be one of A, B, C, D, or F.")
                    st.stop()

            numeric_mask = ~grade_mask
            if numeric_mask.any():
                numeric_values = pd.to_numeric(
                    normalized.loc[numeric_mask, "rule_value"],
                    errors="coerce",
                )
                if numeric_values.isna().any():
                    st.error("Value must be numeric for all rules except Quality_Grade_Min.")
                    st.stop()
                normalized.loc[numeric_mask, "rule_value"] = numeric_values

            if (normalized["rule_name"] == "Require_Drawdown_Limit_When_Underperform_BuyHold").any():
                rd_mask = normalized["rule_name"] == "Require_Drawdown_Limit_When_Underperform_BuyHold"
                normalized.loc[rd_mask, "rule_value"] = (
                    normalized.loc[rd_mask, "rule_value"]
                    .astype(float)
                    .apply(lambda v: 1 if v >= 0.5 else 0)
                )
            duplicates = normalized.duplicated(subset=["rule_name", "timeframe"]).any()
            if duplicates:
                st.error("Rule and Timeframe must be unique. Please remove duplicates before saving.")
                st.stop()

            existing = rules_df.copy()
            existing["timeframe"] = existing["timeframe"].fillna("global")
            existing_keys = set(existing.apply(lambda r: (r["rule_name"], r["timeframe"]), axis=1))
            new_keys = set(normalized.apply(lambda r: (r["rule_name"], r["timeframe"]), axis=1))
            deleted_keys = existing_keys - new_keys
            editor_state = st.session_state.get("backtest_rules_editor")
            if isinstance(editor_state, dict):
                deleted_rows = editor_state.get("deleted_rows", [])
                if deleted_rows:
                    valid_rows = [idx for idx in deleted_rows if 0 <= idx < len(editable_rules)]
                    if not valid_rows:
                        valid_rows = []
                    deleted_df = editable_rules.iloc[valid_rows].copy() if valid_rows else editable_rules.iloc[0:0].copy()
                    deleted_df["timeframe"] = deleted_df["timeframe"].fillna("global")
                    deleted_keys |= set(deleted_df.apply(lambda r: (r["rule_name"], r["timeframe"]), axis=1))

            for row in normalized.itertuples(index=False):
                if not row.rule_name or row.rule_name not in desc_map:
                    continue
                if row.rule_value is None:
                    continue
                timeframe = None if row.timeframe == "global" else row.timeframe
                database.upsert_backtest_approval_rule(
                    rule_name=row.rule_name,
                    rule_value=row.rule_value,
                    timeframe=timeframe,
                    enabled=bool(row.enabled),
                )

            for rule_name, timeframe in deleted_keys:
                db_timeframe = None if timeframe == "global" else timeframe
                database.delete_backtest_approval_rule(rule_name=rule_name, timeframe=db_timeframe)

            after_rules_buttons_container.success("Approval rules updated.")
            st.session_state.pop("backtest_rules_editor", None)
            time.sleep(1.0)
            
            st.rerun(scope="fragment")

        if st.button("Discard Changes", icon=icons.ICON_CANCEL):
            st.session_state.pop("backtest_rules_editor", None)
            st.rerun(scope="fragment")

        if st.button("Restore Default Rules", icon=icons.ICON_REFRESH):
            database.reset_backtest_approval_rules_to_defaults()
            after_rules_buttons_container.success("Approval rules restored to defaults.")
            st.session_state.pop("backtest_rules_editor", None)
            time.sleep(1.0)
            st.rerun(scope="fragment")

@st.fragment
def render_backtest_settings():
    st.markdown("## Backtest Settings")
    st.caption("Configure backtesting settings. These settings will be used when running strategy backtests.")

    settings = database.get_backtesting_settings()

    execution_tab, market_phase_tab, quality_score_tab = st.tabs(
        ["Execution", "Market Phase", "Quality Score"]
    )

    with execution_tab:
        st.subheader("Execution")
        st.caption("Core assumptions used by every backtest run.")

        with st.container(horizontal=True):
            cash_value = st.number_input(
                "Initial cash to start with (USD)",
                min_value=0.0,
                step=100.0,
                value=float(settings["Cash_Value"]),
                format="%.2f",
                width=220,
            )

            commission_percent = st.number_input(
                "Exchange commission rate (%)",
                min_value=0.0,
                max_value=10.0,
                step=0.1,
                value=float(settings["Commission_Value"]) * 100.0,
                format="%.2f",
                width=220,
            )

        st.subheader("Optimization")
        st.caption("Metric used when strategy parameters are optimized.")

        maximize = st.radio(
            label="Optimize strategy parameter to an optimal combination",
            options=list(MAXIMIZE_OPTIONS.keys()),
            index=list(MAXIMIZE_OPTIONS.keys()).index(settings["Maximize"]) if settings["Maximize"] in MAXIMIZE_OPTIONS else 0,
            captions=[MAXIMIZE_OPTIONS[k] for k in MAXIMIZE_OPTIONS],
        )

        st.subheader("Buy & Hold Benchmark")
        st.caption("Choose where the Buy & Hold comparison starts.")

        current_buy_hold_start_mode = settings.get("Buy_Hold_Start_Mode", "indicator_warmup")
        buy_hold_start_mode_options = list(BUY_HOLD_START_MODE_OPTIONS.keys())
        buy_hold_start_mode = st.radio(
            "Buy & Hold start",
            options=buy_hold_start_mode_options,
            index=(
                buy_hold_start_mode_options.index(current_buy_hold_start_mode)
                if current_buy_hold_start_mode in buy_hold_start_mode_options
                else 0
            ),
            format_func=lambda value: BUY_HOLD_START_MODE_OPTIONS[value],
            captions=[
                "Use the native backtesting.py benchmark start, after indicators have enough data.",
                "Override Buy & Hold to start at the first candle in the dataset.",
            ],
            horizontal=True,
        )

        execution_settings_actions = st.container()
        st.divider()
        approval_rules_section()

    with market_phase_tab:
        st.subheader("Market Phase Filters")
        st.caption("Configure the SMA regime filters used by intraday and daily backtests.")

        use_intraday_current_timeframe_market_phase_filter = st.checkbox(
            "Include current timeframe market phase filter for 1h and 4h",
            value=bool(int(settings.get("Use_Intraday_Current_Timeframe_Market_Phase_Filter", 1))),
            help=(
                "When enabled, intraday backtests that support market phases require both the 1h/4h "
                "market phase and the 1d market phase. When disabled, 1h/4h backtests keep only the "
                "1d market phase filter."
            ),
        )

        st.subheader("SMA Periods By Timeframe")
        st.caption("For 1h/4h backtests, the current timeframe uses its own row and the higher-timeframe filter uses 1D.")

        with st.container(horizontal=True):
            market_phase_1h_sma_fast = st.number_input(
                "1H SMA fast",
                min_value=1,
                step=1,
                value=int(settings.get("Market_Phase_1h_SMA_Fast", 50)),
                width=150,
            )
            market_phase_1h_sma_slow = st.number_input(
                "1H SMA slow",
                min_value=1,
                step=1,
                value=int(settings.get("Market_Phase_1h_SMA_Slow", 200)),
                width=150,
            )

        with st.container(horizontal=True):
            market_phase_4h_sma_fast = st.number_input(
                "4H SMA fast",
                min_value=1,
                step=1,
                value=int(settings.get("Market_Phase_4h_SMA_Fast", 50)),
                width=150,
            )
            market_phase_4h_sma_slow = st.number_input(
                "4H SMA slow",
                min_value=1,
                step=1,
                value=int(settings.get("Market_Phase_4h_SMA_Slow", 200)),
                width=150,
            )

        with st.container(horizontal=True):
            market_phase_1d_sma_fast = st.number_input(
                "1D SMA fast",
                min_value=1,
                step=1,
                value=int(settings.get("Market_Phase_1d_SMA_Fast", 50)),
                width=150,
            )
            market_phase_1d_sma_slow = st.number_input(
                "1D SMA slow",
                min_value=1,
                step=1,
                value=int(settings.get("Market_Phase_1d_SMA_Slow", 200)),
                width=150,
            )

        market_phase_settings_actions = st.container()

    with quality_score_tab:
        st.subheader("Strategy Quality Score")
        st.caption("Weights used to combine return, risk, trade quality and robustness into a 0-100 score.")

        with st.container(horizontal=True):
            strategy_quality_return_weight = st.number_input(
                "Return %",
                min_value=0.0,
                max_value=100.0,
                step=1.0,
                value=float(settings.get("Strategy_Quality_Return_Weight", 20.0)),
                width=130,
            )
            strategy_quality_risk_weight = st.number_input(
                "Risk %",
                min_value=0.0,
                max_value=100.0,
                step=1.0,
                value=float(settings.get("Strategy_Quality_Risk_Weight", 25.0)),
                width=130,
            )
            strategy_quality_risk_adjusted_weight = st.number_input(
                "Risk-adjusted %",
                min_value=0.0,
                max_value=100.0,
                step=1.0,
                value=float(settings.get("Strategy_Quality_Risk_Adjusted_Weight", 20.0)),
                width=160,
            )
            strategy_quality_trade_quality_weight = st.number_input(
                "Trade quality %",
                min_value=0.0,
                max_value=100.0,
                step=1.0,
                value=float(settings.get("Strategy_Quality_Trade_Quality_Weight", 20.0)),
                width=160,
            )
            strategy_quality_robustness_weight = st.number_input(
                "Robustness %",
                min_value=0.0,
                max_value=100.0,
                step=1.0,
                value=float(settings.get("Strategy_Quality_Robustness_Weight", 15.0)),
                width=150,
            )

    strategy_quality_weight_total = (
        strategy_quality_return_weight
        + strategy_quality_risk_weight
        + strategy_quality_risk_adjusted_weight
        + strategy_quality_trade_quality_weight
        + strategy_quality_robustness_weight
    )
    with quality_score_tab:
        if abs(strategy_quality_weight_total - 100.0) <= 0.0001:
            st.caption(f"Total weight: {strategy_quality_weight_total:g}%")
        else:
            st.markdown(
                f"<span style='color:#dc2626;font-size:0.875rem;'>"
                f"Total weight: {strategy_quality_weight_total:g}% - must equal 100% to save."
                f"</span>",
                unsafe_allow_html=True,
            )
        quality_score_settings_actions = st.container()

        st.subheader("Grade Scale")
        st.caption(
            "Use this scale when choosing approval rules. A default "
            "Quality_Grade_Min of C keeps Grade A, B and C backtests."
        )
        st.dataframe(
            QUALITY_GRADE_SCALE,
            hide_index=True,
            width="content",
            column_config={
                "Grade": st.column_config.TextColumn("Grade", width="small"),
                "Quality Score": st.column_config.TextColumn("Quality Score"),
                "Interpretation": st.column_config.TextColumn("Interpretation"),
            },
        )

    def save_backtesting_settings():
        if market_phase_1h_sma_fast >= market_phase_1h_sma_slow:
            st.error("1H SMA fast must be lower than 1H SMA slow.")
            st.stop()
        if market_phase_4h_sma_fast >= market_phase_4h_sma_slow:
            st.error("4H SMA fast must be lower than 4H SMA slow.")
            st.stop()
        if market_phase_1d_sma_fast >= market_phase_1d_sma_slow:
            st.error("1D SMA fast must be lower than 1D SMA slow.")
            st.stop()
        if abs(strategy_quality_weight_total - 100.0) > 0.0001:
            st.error("Strategy Quality Score weights must add up to 100%.")
            st.stop()

        commission_value = commission_percent / 100.0
        database.update_backtesting_settings(
            commission_value=commission_value,
            cash_value=cash_value,
            maximize=maximize,
            buy_hold_start_mode=buy_hold_start_mode,
            use_intraday_current_timeframe_market_phase_filter=use_intraday_current_timeframe_market_phase_filter,
            market_phase_1h_sma_fast=market_phase_1h_sma_fast,
            market_phase_1h_sma_slow=market_phase_1h_sma_slow,
            market_phase_4h_sma_fast=market_phase_4h_sma_fast,
            market_phase_4h_sma_slow=market_phase_4h_sma_slow,
            market_phase_1d_sma_fast=market_phase_1d_sma_fast,
            market_phase_1d_sma_slow=market_phase_1d_sma_slow,
            strategy_quality_return_weight=strategy_quality_return_weight,
            strategy_quality_risk_weight=strategy_quality_risk_weight,
            strategy_quality_risk_adjusted_weight=strategy_quality_risk_adjusted_weight,
            strategy_quality_trade_quality_weight=strategy_quality_trade_quality_weight,
            strategy_quality_robustness_weight=strategy_quality_robustness_weight,
        )
        st.success("Backtesting settings updated.")
        time.sleep(1.0)
        st.rerun(scope="fragment")

    with execution_settings_actions:
        if st.button("Save Settings", icon=icons.ICON_SAVE, key="save_backtesting_settings_execution"):
            save_backtesting_settings()

    with market_phase_settings_actions:
        if st.button("Save Settings", icon=icons.ICON_SAVE, key="save_backtesting_settings_market_phase"):
            save_backtesting_settings()

    with quality_score_settings_actions:
        if st.button("Save Settings", icon=icons.ICON_SAVE, key="save_backtesting_settings_quality_score"):
            save_backtesting_settings()

    st.space()


def main():
    render_backtest_settings()


if __name__ == "__main__":
    main()
