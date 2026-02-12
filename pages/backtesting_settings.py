import pandas as pd
import streamlit as st

import utils.database as database
import utils.icons as icons

# st.header("Backtest Settings")
st.markdown("## Backtest Settings")
st.caption("Configure backtesting settings. These settings will be used when running strategy backtests.")

st.space()



settings = database.get_backtesting_settings()

with st.container(horizontal=True):
    cash_value = st.number_input(
        "Initial cash to start with (USD)",
        min_value=0.0,
        step=100.0,
        value=float(settings["Cash_Value"]),
        format="%.2f",
        width=200,
    )

    commission_percent = st.number_input(
        "Exchange commission rate (%)",
        min_value=0.0,
        max_value=10.0,
        step=0.1,
        value=float(settings["Commission_Value"]) * 100.0,
        format="%.2f",
        width=200,
    )

st.space()

maximize_options = {
    "SQN": "Van Tharp's [System Quality Number](https://www.google.com/search?q=System+Quality+Number).",
    "Equity Final [$]": "Maximizes final equity (more aggressive).",
    "Calmar Ratio": "Favors return with drawdown control.",
    "Sharpe Ratio": "Favors return per unit of volatility.",
    "Sortino Ratio": "Like Sharpe but penalizes downside more.",
}

maximize = st.radio(
    label="Optimize strategy parameter to an optimal combination",
    options=list(maximize_options.keys()),
    index=list(maximize_options.keys()).index(settings["Maximize"]) if settings["Maximize"] in maximize_options else 0,
    captions=[maximize_options[k] for k in maximize_options],
)

if st.button("Save", icon=icons.ICON_SAVE):
    commission_value = commission_percent / 100.0
    database.update_backtesting_settings(
        commission_value=commission_value,
        cash_value=cash_value,
        maximize=maximize,
    )
    st.success("Backtesting settings updated.")

st.space()


@st.fragment
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

    edited_rules = st.data_editor(
        editable_rules,
        num_rows="dynamic",
        width="content",
        column_config={
            "rule_name": st.column_config.SelectboxColumn("Rule", options=rule_names),
            "description": st.column_config.TextColumn("Description"),
            "rule_value": st.column_config.NumberColumn("Value"),
            "timeframe": st.column_config.SelectboxColumn("Timeframe", options=timeframe_options),
            "enabled": st.column_config.CheckboxColumn("Enabled"),
        },
        disabled=["description"],
        key="backtest_rules_editor",
    )


    with st.container(horizontal=True):

        if st.button("Save Rules", icon=icons.ICON_SAVE):
            normalized = edited_rules.copy()
            normalized["timeframe"] = normalized["timeframe"].fillna("global")
            normalized["enabled"] = normalized["enabled"].fillna(True).astype(bool)
            missing_required = normalized["rule_name"].isna() | (normalized["rule_name"].astype(str).str.strip() == "") | normalized["rule_value"].isna()
            if missing_required.any():
                st.error("Rule and Value are required for each row.")
                st.stop()
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

            st.success("Approval rules updated.")
            st.session_state.pop("backtest_rules_editor", None)
            st.rerun(scope="fragment")

        if st.button("Discard Changes", icon=icons.ICON_CANCEL):
            st.session_state.pop("backtest_rules_editor", None)
            st.rerun(scope="fragment")


approval_rules_section()
