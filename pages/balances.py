import streamlit as st
import altair as alt

import utils.database as database


def get_chart_daily_balance(asset):
    if asset not in ["USD", "BTC"]:
        return

    expander_total_balance = st.expander(label=f"Daily Balance Snapshot - {asset}", expanded=True)
    with expander_total_balance:
        period_selected_balances = st.radio(
            label="Choose Period",
            options=("Last 7 days", "Last 30 days", "Last 90 days", "YTD", "All Time"),
            index=1,
            horizontal=True,
            label_visibility="collapsed",
            key=f"period_selected_balances_{asset}",
        )

        if period_selected_balances == "Last 7 days":
            n_days = 7
            source = database.get_total_balance_last_n_days(n_days, asset=asset)
        elif period_selected_balances == "Last 30 days":
            n_days = 30
            source = database.get_total_balance_last_n_days(n_days, asset=asset)
        elif period_selected_balances == "Last 90 days":
            n_days = 90
            source = database.get_total_balance_last_n_days(n_days, asset=asset)
        elif period_selected_balances == "YTD":
            source = database.get_total_balance_ytd(asset=asset)
        elif period_selected_balances == "All Time":
            source = database.get_total_balance_all_time(asset=asset)

        if source.empty:
            st.warning("No data on Balances yet!")
            current_total_balance = 0
        else:
            if asset == "USD":
                current_total_balance = source.Total_Balance_USD.iloc[-1]
            elif asset == "BTC":
                current_total_balance = source.Total_Balance_BTC.iloc[-1]

        col1, col2 = st.columns([10, 1])
        with col1:
            st.caption(f"Last Daily Balance: {current_total_balance}")

        if source.empty:
            return

        hover = alt.selection_point(
            fields=["Date"],
            nearest=True,
            on="mouseover",
            empty=False,
        )
        if asset == "USD":
            lines = (
                alt.Chart(source)
                .mark_line()
                .encode(
                    x="Date",
                    y=alt.Y(
                        f"Total_Balance_{asset}",
                        title=f"Balance_{asset}",
                        scale=alt.Scale(domain=[source.Total_Balance_USD.min(), source.Total_Balance_USD.max()]),
                    ),
                )
            )
        else:
            lines = (
                alt.Chart(source)
                .mark_line()
                .encode(
                    x="Date",
                    y=alt.Y(
                        f"Total_Balance_{asset}",
                        title=f"Balance_{asset}",
                        scale=alt.Scale(domain=[source.Total_Balance_BTC.min(), source.Total_Balance_BTC.max()]),
                    ),
                )
            )

        points = lines.transform_filter(hover).mark_circle(size=70)
        tooltips = (
            alt.Chart(source)
            .mark_rule()
            .encode(
                x="Date",
                y=f"Total_Balance_{asset}",
                opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
                tooltip=[
                    alt.Tooltip("Date", title="Date"),
                    alt.Tooltip(f"Total_Balance_{asset}", title=f"Balance_{asset}"),
                ],
            )
            .add_params(hover)
        )
        chart = (lines + points + tooltips).interactive()
        st.altair_chart(chart)


def get_chart_daily_asset_balances():
    expander_asset_balances = st.expander(label="Daily Asset Balances", expanded=True)
    with expander_asset_balances:
        period_selected_asset = st.radio(
            label="Choose Period",
            options=("Last 7 days", "Last 30 days", "Last 90 days", "YTD", "All Time"),
            index=1,
            horizontal=True,
            label_visibility="collapsed",
            key="period_selected_asset",
        )

        if period_selected_asset == "Last 7 days":
            n_days = 7
            source = database.get_asset_balances_last_n_days(n_days)
        elif period_selected_asset == "Last 30 days":
            n_days = 30
            source = database.get_asset_balances_last_n_days(n_days)
        elif period_selected_asset == "Last 90 days":
            n_days = 90
            source = database.get_asset_balances_last_n_days(n_days)
        elif period_selected_asset == "YTD":
            source = database.get_asset_balances_ytd()
        elif period_selected_asset == "All Time":
            source = database.get_asset_balances_all_time()

        if source.empty:
            st.warning("No data on Balances yet!")
            return

        hover = alt.selection_point(
            fields=["Date"],
            nearest=True,
            on="mouseover",
            empty=False,
        )

        lines = (
            alt.Chart(source)
            .mark_line()
            .encode(
                x="Date",
                y=alt.Y("Balance_USD", scale=alt.Scale(domain=[source.Balance_USD.min(), source.Balance_USD.max()])),
                color="Asset",
            )
        )

        points = lines.transform_filter(hover).mark_circle(size=70)
        tooltips = (
            alt.Chart(source)
            .mark_rule()
            .encode(
                x="Date",
                y="Balance_USD",
                opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
                tooltip=[
                    alt.Tooltip("Date", title="Date"),
                    alt.Tooltip("Asset", title="Asset"),
                    alt.Tooltip("Balance_USD", title="Balance_USD"),
                ],
            )
            .add_params(hover)
        )
        chart = (lines + points + tooltips).interactive()
        st.altair_chart(chart)


st.header("Balances")
get_chart_daily_balance(asset="USD")
get_chart_daily_balance(asset="BTC")
get_chart_daily_asset_balances()
