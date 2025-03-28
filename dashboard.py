import pandas as pd
import time
import os
import calendar
from datetime import datetime

import streamlit as st
from millify import millify
import streamlit_authenticator as stauth
import altair as alt

import utils.config as config
import utils.database as database
import exchanges.binance as binance
import utils.general as general

from symbol_by_market_phase import main as force_run_backtest
from my_backtesting import FOLDER_BACKTEST_RESULTS

import update 

st.set_page_config(
    page_title="Bot Dashboard App",
    page_icon="random",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'Get Help': 'https://github.com/jptsantossilva/BEC#readme',
        'Report a bug': "https://github.com/jptsantossilva/BEC/issues/new",
        'About': """# My name is BEC \n I am a Trading Bot and I'm trying to be an *extremely* cool app! 
        \n This is my dad's 🐦 Twitter: [@jptsantossilva](https://twitter.com/jptsantossilva).
        """
    }
)

# for testing purposes
# st.session_state

# Initialization
if "name" not in st.session_state:
    st.session_state.name = ""
if "username" not in st.session_state:
    st.session_state.username = ""
if "user_password" not in st.session_state:
    st.session_state.user_password = "None"
if "reset_form_open" not in st.session_state:
    st.session_state.reset_form_open = False
if "reset_password_submitted" not in st.session_state:
    st.session_state.reset_password_submitted = False
if "authentication_status" not in st.session_state:
    st.session_state.authentication_status = False

def get_chart_daily_balance(asset):
    if asset not in ["USD", "BTC"]:
        return

    expander_total_balance = st.expander(label=f"Daily Balance Snapshot - {asset}", expanded=False)
    with expander_total_balance:
        period_selected_balances = st.radio(
            label='Choose Period',
            options=('Last 7 days','Last 30 days', 'Last 90 days', 'YTD', 'All Time'),
            index=1,
            horizontal=True,
            label_visibility='collapsed',
            key=f'period_selected_balances_{asset}'
        )

        if period_selected_balances == 'Last 7 days':
            n_days = 7
            source = database.get_total_balance_last_n_days(connection, n_days, asset=asset)
        elif period_selected_balances == 'Last 30 days':
            n_days = 30
            source = database.get_total_balance_last_n_days(connection, n_days, asset=asset)
        elif period_selected_balances == 'Last 90 days':
            n_days = 90
            source = database.get_total_balance_last_n_days(connection, n_days, asset=asset)
        elif period_selected_balances == 'YTD':
            source = database.get_total_balance_ytd(connection, asset=asset)
        elif period_selected_balances == 'All Time':
            source = database.get_total_balance_all_time(connection, asset=asset)

        if source.empty:
            st.warning('No data on Balances yet! Click Refresh.')
            current_total_balance = 0
        else:
            if asset == "USD":
                current_total_balance = source.Total_Balance_USD.iloc[-1]
            elif asset == "BTC":
                current_total_balance = source.Total_Balance_BTC.iloc[-1]

        col1, col2 = st.columns([10, 1])
        with col1:
            st.caption(f'Last Daily Balance: {current_total_balance}')
        # with col2:
        #     refresh_balance = st.button("Refresh", key=f"refresh_balance_{asset}")

        # if refresh_balance:
        #     with st.spinner("Creating balance snapshot. It can take a few minutes..."):
        #         exchange.create_balance_snapshot(telegram_prefix="")
        #         # dasboard refresh
        #         st.rerun()

        # exit if there is no data to display on chart
        if source.empty:
            return
        
        hover = alt.selection_single(
            fields=["Date"],
            nearest=True,
            on="mouseover",
            empty="none",
        )
        if asset == "USD":
            lines = (
                alt.Chart(source, 
                        #   title="Total Balance USD Last 30 Days"
                        )
                .mark_line()
                .encode(
                    x="Date",
                    y=alt.Y(f"Total_Balance_{asset}", title=f"Balance_{asset}",scale=alt.Scale(domain=[source.Total_Balance_USD.min(),source.Total_Balance_USD.max()])),
                    # color="Total_Balance_USD",
                )
            )
        elif asset == "BTC":
            lines = (
                alt.Chart(source, 
                        #   title="Total Balance USD Last 30 Days"
                        )
                .mark_line()
                .encode(
                    x="Date",
                    y=alt.Y(f"Total_Balance_{asset}", title=f"Balance_{asset}",scale=alt.Scale(domain=[source.Total_Balance_BTC.min(),source.Total_Balance_BTC.max()])),
                    # color="Total_Balance_USD",
                )
            )

        # Draw points on the line, and highlight based on selection
        points = lines.transform_filter(hover).mark_circle(size=70)

        # Draw a rule at the location of the selection
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
            .add_selection(hover)
        )
        chart = (lines + points + tooltips).interactive()
        st.altair_chart(chart, use_container_width=True)


def get_chart_daily_asset_balances():
    expander_asset_balances = st.expander(label="Daily Asset Balances", expanded=False)
    with expander_asset_balances:
        period_selected_asset = st.radio(
            label='Choose Period',
            options=('Last 7 days','Last 30 days', 'Last 90 days', 'YTD', 'All Time'),
            index=1,
            horizontal=True,
            label_visibility='collapsed',
            key='period_selected_asset')

        if period_selected_asset == 'Last 7 days':
            n_days = 7
            source = database.get_asset_balances_last_n_days(connection, n_days)
        elif period_selected_asset == 'Last 30 days':
            n_days = 30
            source = database.get_asset_balances_last_n_days(connection, n_days)
        elif period_selected_asset == 'Last 90 days':
            n_days = 90
            source = database.get_asset_balances_last_n_days(connection, n_days)
        elif period_selected_asset == 'YTD':
            source = database.get_asset_balances_ytd(connection)
        elif period_selected_asset == 'All Time':
            source = database.get_asset_balances_all_time(connection)

        if source.empty:
            st.warning('No data on Balances yet!')
            # exit - there is no data to display on chart
            return

        hover = alt.selection_single(
            fields=["Date"],
            nearest=True,
            on="mouseover",
            empty="none",
        )

        lines = (
            alt.Chart(source, 
                    #   title="Asset Balances Last 30 Days"
                      )
            .mark_line()
            .encode(
                x="Date",
                y=alt.Y("Balance_USD", scale=alt.Scale(domain=[source.Balance_USD.min(),source.Balance_USD.max()])),
                color="Asset",
            )
        )

        # Draw points on the line, and highlight based on selection
        points = lines.transform_filter(hover).mark_circle(size=70)

        # Draw a rule at the location of the selection
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
            .add_selection(hover)
        )
        # chart = (lines + points + tooltips).properties(height=800).interactive()
        chart = (lines + points + tooltips).interactive()
        st.altair_chart(chart, use_container_width=True)

def realized_pnl():
    with tab_rpnl:
        # get years
        years = get_years(bot_selected)

        # years empty list
        if len(years) == 0:
            st.warning('There are no closed positions yet! 🤞')

        col1, col2, col3 = st.columns(3)
        # years selectbox
        year = col1.selectbox(
            'Year',
            (years)
        )
        # get months
        # months_dict = get_orders_by_month(year, bot_selected)
        months_dict = get_orders_by_month(year)
        month_names = list(months_dict.values())

        # months selectbox
        month_selected_name = col2.selectbox(
            'Month',
            (month_names)
        )

        disable_full_year = month_selected_name == None
        if month_selected_name == None:
            month_number = 1
        else: # get month number from month name using months dictionary 
            month_number = list(months_dict.keys())[list(months_dict.values()).index(month_selected_name)]


        if col2.checkbox('Full Year', disabled=disable_full_year):
            month_number = 13

        result_closed_positions, trades_month_1d, trades_month_4h, trades_month_1h = calculate_realized_pnl(str(year), str(month_number))
        # print("\nPnL - Total")
        # print(result_closed_positions)

        st.header("Realized PnL - Total")
        result_closed_positions = result_closed_positions.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value'])
        st.dataframe(result_closed_positions)    
    
        # print("Realized PnL - Detail")
        # print(trades_month_1d)
        # print(trades_month_4h)
        # print(trades_month_1h)

        st.header(f"Realized PnL - Detail")
        
        st.subheader("Bot 1d")
        st.dataframe(
            trades_month_1d.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            column_config = {
                "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                "Exit_Reason": st.column_config.TextColumn(width="large")
                }
        )
        
        st.subheader("Bot 4h")
        st.dataframe(
            trades_month_4h.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            column_config = {
                "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                "Exit_Reason": st.column_config.TextColumn(width="large")
                }
        )
        
        st.subheader("Bot 1h")
        st.dataframe(
            trades_month_1h.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            column_config = {
                "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                "Exit_Reason": st.column_config.TextColumn(width="large")
                }
        )

        # print('\n----------------------------\n')

@st.dialog("Delete Position")
def delete_position(symbol, timeframe):
    st.warning("If you delete position the sell price will be set to 0.", icon="⚠️")
    st.write("As an example, this feature is useful for clearing a position when a symbol is delisted from the exchange and you have no way to sell it.")
    st.write(f"Are you sure you want to delete **{symbol}** from **{timeframe}** timeframe?")
    if st.button("Delete", key="delete_position"):
        binance.delete_position(
            symbol=symbol,
            bot=timeframe
        ) 
        st.rerun()
        
def unrealized_pnl():
    with tab_upnl:
        result_open_positions, positions_df_1d, positions_df_4h, positions_df_1h = calculate_unrealized_pnl()
        # print("\nUnrealized PnL - Total")
        # print('-------------------------------')
        # print(result_open_positions)

        if positions_df_1d.empty and positions_df_4h.empty and positions_df_1h.empty:
            st.warning('There are no open positions yet! 🤞') 

        st.header("Unrealized PnL - Total")

        result_open_positions = result_open_positions.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value'])
        st.dataframe(result_open_positions)

        st.header(f"Unrealized PnL - Detail")
        
        st.subheader("Bot 1d")

        col_config = {
            "Id": None,
            "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
            "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
            "TP1": st.column_config.CheckboxColumn(),
            "TP2": st.column_config.CheckboxColumn(),
            "TP3": st.column_config.CheckboxColumn(),
            "TP4": st.column_config.CheckboxColumn(),
            "RPQ%": st.column_config.NumberColumn(help="Remaining Position Qty %",
                                            #    format="%.2f",
                                                )
        }
        
        event_positions_1d = st.dataframe(
            positions_df_1d.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            key="positions_df_1d",
            column_config=col_config,
            hide_index=True,
            on_select="rerun",
            selection_mode=["single-row", "multi-column"],
        )

        # event_positions_1d.selection

        row_1d_selected = len(event_positions_1d.selection.rows) > 0

        # Check if there's a selection
        if row_1d_selected:
            selected_row_index = event_positions_1d.selection.rows[0]  # Get the index of the selected row
            selected_symbol = positions_df_1d.loc[selected_row_index, 'Symbol']
            selected_bot = positions_df_1d.loc[selected_row_index, 'Bot']  

            # Show the selected Name
            # st.write(f"Selected Symbol {selected_symbol} and bot {selected_bot}")

            if st.button("Delete", key="delete_1d"):
                delete_position(symbol=selected_symbol, timeframe=selected_bot)
        
        
        #########################

        st.subheader("Bot 4h")
        
        event_positions_4h = st.dataframe(
            positions_df_4h.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            key="positions_df_4h",
            column_config=col_config,
            hide_index=True,
            on_select="rerun",
            selection_mode=["single-row", "multi-column"],
        )

        # event_positions_1d.selection

        row_4h_selected = len(event_positions_4h.selection.rows) > 0

        # Check if there's a selection
        if row_4h_selected:
            selected_row_index = event_positions_4h.selection.rows[0]  # Get the index of the selected row
            selected_symbol = positions_df_4h.loc[selected_row_index, 'Symbol']
            selected_bot = positions_df_4h.loc[selected_row_index, 'Bot']  

            # Show the selected Name
            # st.write(f"Selected Symbol {selected_symbol} and bot {selected_bot}")

            if st.button("Delete", key="delete_4h"):
                delete_position(symbol=selected_symbol, timeframe=selected_bot)

        #########################
        
        st.subheader("Bot 1h")

        event_positions_1h = st.dataframe(
            positions_df_1h.style.applymap(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            key="positions_df_1h",
            column_config=col_config,
            hide_index=True,
            on_select="rerun",
            selection_mode=["single-row", "multi-column"],
        )

        # event_positions_1d.selection

        row_1h_selected = len(event_positions_1h.selection.rows) > 0

        # Check if there's a selection
        if row_1h_selected:
            selected_row_index = event_positions_1h.selection.rows[0]  # Get the index of the selected row
            selected_symbol = positions_df_1h.loc[selected_row_index, 'Symbol']
            selected_bot = positions_df_1h.loc[selected_row_index, 'Bot']  

            # Show the selected Name
            # st.write(f"Selected Symbol {selected_symbol} and bot {selected_bot}")

            if st.button("Delete", key="delete_1h"):
                delete_position(symbol=selected_symbol, timeframe=selected_bot)

        #----------------------
        # Force Close Position
        st.header("Force Selling")
        
        sell_expander = st.expander("Choose position to sell")
        with sell_expander:

            # bots
            bots = ["1d", "4h", "1h"]

            
            if "sell_bot" not in st.session_state:
                st.session_state.sell_bot = None
            # def sell_bot_change():

            sell_bot = st.selectbox(
                label="Bot",
                options=(bots),
                # label_visibility="collapsed",
                key="sell_bot",
                # on_change=sell_bot_change
            )
            
            if st.session_state.sell_bot == "1d":
                list_positions = positions_df_1d.Symbol.to_list()
            elif st.session_state.sell_bot == "4h":
                list_positions = positions_df_4h.Symbol.to_list()
            elif st.session_state.sell_bot == "1h":
                list_positions = positions_df_1h.Symbol.to_list()
            else:
                list_positions = []

            if "sell_symbol" not in st.session_state:
                st.session_state.sell_symbol = None

            sell_symbol = st.selectbox(
                label="Symbol",
                options=(list_positions),
                # label_visibility="collapsed",
                key="sell_symbol",
                disabled=len(list_positions) == 0
            )

            disable_sell_confirmation1 = sell_symbol == None    
                    
            # get balance
            if not disable_sell_confirmation1:
                sell_amount_perc = st.slider(
                    label='Amount', 
                    min_value=10, 
                    max_value=100, 
                    value=25, 
                    step=5, 
                    format="%d%%", 
                    disabled=disable_sell_confirmation1
                )
                
                # get current position balance
                df_pos = database.get_positions_by_bot_symbol_position(database.conn, bot=sell_bot, symbol=sell_symbol, position=1)
                if not df_pos.empty:
                    balance_qty = df_pos['Qty'].iloc[0]
                else:
                    balance_qty = 0
                # symbol_only, symbol_stable = general.separate_symbol_and_trade_against(sell_symbol)
                # balance_qty = exchange.get_symbol_balance(symbol=symbol_only, bot=sell_bot) 
                
                sell_amount = balance_qty*(sell_amount_perc/100)
                st.text_input(
                    label='Sell Amount / Position Balance', 
                    value=f'{sell_amount} / {balance_qty}', 
                    disabled=True
                )
    
                # sell_expander.write(disable_sell_confirmation1)
                sell_reason = f"Forced Selling of {sell_amount_perc}%"
                sell_reason_input = st.text_input("Reason")
                if sell_reason_input:
                    sell_reason = f"{sell_reason} - {sell_reason_input}"
                sell_confirmation1 = st.checkbox(f"I confirm the Sell of **{sell_amount_perc}%** of **{sell_symbol}** from **{sell_bot}** bot", disabled=disable_sell_confirmation1)

                # if button pressed then sell position
                if sell_confirmation1:
                    def sell_click():
                        result, msg = binance.create_sell_order(
                            symbol=sell_symbol,
                            bot=sell_bot,
                            reason=f"Forced Selling of {sell_amount_perc}%",
                            percentage=sell_amount_perc
                        ) 

                        if result:
                            sell_expander.success(f"SOLD **{sell_amount_perc}%** of {sell_symbol} from **{sell_bot}** bot!")
                        else:
                            sell_expander.error(msg)
                        # time.sleep(3)

                        st.session_state.sell_bot = None
                        
                        # dasboard refresh
                        # st.rerun()

                    sell_confirmation2 = sell_expander.button(label="SELL", on_click=sell_click)

def top_performers():
    with tab_top_perf:
        top_perf = config.get_setting("trade_top_performance")
        st.subheader(f"Top {top_perf} Performers")
        st.caption("The top performers are those in accumulation phase (Price > 50DSMA and Price > 200DSMA and 50DSMA < 200DSMA) and bullish phase (Price > 50DSMA and Price > 200DSMA and 50DSMA > 200DSMA) and then sorted by the price above the 200-day moving average (DSMA) in percentage terms. [Click here for more details](https://twitter.com/jptsantossilva/status/1539976855469428738?s=20).")
        df_mp = database.get_all_symbols_by_market_phase(connection)
        df_mp['Price'] = df_mp['Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['DSMA50'] = df_mp['DSMA50'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['DSMA200'] = df_mp['DSMA200'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['Perc_Above_DSMA50'] = df_mp['Perc_Above_DSMA50'].apply(lambda x:'{:.2f}'.format(x))
        df_mp['Perc_Above_DSMA200'] = df_mp['Perc_Above_DSMA200'].apply(lambda x:'{:.2f}'.format(x))
        st.dataframe(df_mp)

        filename = "Top_performers_"+trade_against+".txt"
        if os.path.exists(filename):
            with open(filename, "rb") as file:
                st.download_button(
                    label="Download as TradingView List",
                    data=file,
                    file_name=filename,
                    mime='text/csv',
                ) 

        st.subheader(f"Historical Top Performers")
        st.caption("Symbols that spend the most number of days in the bullish or accumulating phases")
        df_symbols_days_at_top = database.symbols_by_market_phase_Historical_get_symbols_days_at_top(connection)
        st.dataframe(df_symbols_days_at_top)

def signals():
    with tab_signals:
        st.subheader(f"Signals Log")
        st.caption("These signals are just informative. They do not automatically trigger buy and sell orders. You can use these to help you make decisions about when to force a manual exit from an unrealized position.")
        expander_signals = st.expander(label="Signals", expanded=False)
        with expander_signals:
            st.write("""**SUPER-RSI** - Triggered when all time-frames are below or above a defined level.
                    \n RSI(14) 1d / 4h / 1h / 30m / 15m <= 25
                    \n RSI(14) 1d / 4h / 1h / 30m / 15m >= 80""")
            # st.divider()  # Draws a horizontal line
        df_s = database.get_all_signals_log(connection, num_rows=100)
        st.dataframe(df_s)

@st.fragment
def blacklist():

    st.subheader("Blacklist")

    st.caption("""
        The blacklist allows you to exclude specific symbol tickers from trading.<br>
        When adding a ticker, enter only the base symbol (e.g., ETH, SOL, LTC) instead of the full trading pair (e.g., ETHUSDT, SOLUSDC, LTCBTC).""",
        unsafe_allow_html=True)

    df_blacklist = database.get_symbol_blacklist(connection)

    # Hide 'Id' but keep it for internal tracking
    df_blacklist_display = df_blacklist[["Symbol"]]  # Only show 'Symbol' column

    # Allow user to edit the blacklist without showing 'Id'
    edited_blacklist_display = st.data_editor(df_blacklist_display, num_rows="dynamic")

    # Detect deleted rows (Symbols that were in df_blacklist but are missing in edited_blacklist_display)
    deleted_symbols = df_blacklist[~df_blacklist["Symbol"].isin(edited_blacklist_display["Symbol"])]

    # Merge back to retain Ids and capture new symbols
    edited_blacklist = df_blacklist.merge(
        edited_blacklist_display, on="Symbol", how="right"
    )

    # Ensure Ids for existing rows remain the same
    edited_blacklist["Id"] = edited_blacklist["Id"].apply(lambda x: None if pd.isna(x) else int(x))
    
    # Save button
    if st.button("Save", key="save_blacklist"):
        # Update the database (Insert/Update)
        database.update_blacklist(connection, edited_blacklist)

            # Remove deleted rows
        if not deleted_symbols.empty:
            database.delete_from_blacklist(connection, deleted_symbols)

        st.rerun(scope="fragment")

def backtesting_results():
    with tab_backtesting_results:
        st.subheader("Backtesting Results")

        col_br1, col_br2, col_br3 = st.columns(3)

        with col_br1:
            # search by strategy
            search_strategy = st.multiselect(
                'Strategy',
                options=list(dict_strategies.keys()),
                format_func=format_func_strategies
            )
        # st.write('You selected:', search_strategy)

        df_bt_results = database.get_all_backtesting_results(connection)

        with col_br3:
            pass

        with col_br2:
            list_timeframe = ["1d", "4h", "1h"]
            search_timeframe = st.multiselect(label='Time-Frame',options=list_timeframe)

        
        
        # search by symbol
        # get distinct symbols
        list_symbols = df_bt_results['Symbol'].unique().tolist()
            
        col_br_symbol1, col_br_symbol2 = st.columns([0.66, 0.33])
        with col_br_symbol2:
            # add sapce above the checkbox to align with the symbol multiselect
            st.write('<div style="height: 35px;"></div>', unsafe_allow_html=True)

            default_symbols = None
            if st.checkbox("Use Top Performers"):
                df_top_perf = database.get_all_symbols_by_market_phase(connection)
                top_perf_symbol_list = df_top_perf["Symbol"].to_list()
                default_symbols = top_perf_symbol_list
                
        with col_br_symbol1:
            search_symbol = st.multiselect(label='Symbol', default=default_symbols, options=list_symbols)
            # st.write('You selected:', search_symbol)
            
        col_br4, col_br5, col_br6 = st.columns(3)

        today = datetime.now()
        four_years_ago = today.replace(year=today.year - 4)
        
        with col_br4:
            col_br41, col_br42 = st.columns(2)

            with col_br41:
                search_date_ini = st.date_input(
                    label="Start date",
                    value=four_years_ago,
                    min_value=four_years_ago,
                    max_value=today,
                    format="DD.MM.YYYY",
                )

            with col_br42:
                search_date_end = st.date_input(
                    label="End date",
                    value=today,
                    min_value=search_date_ini,
                    max_value=today,
                    format="DD.MM.YYYY",
                )

        search_return_pct = st.checkbox("Return Percentage > 0", value=True)

        # Convert 'Backtest_Start_Date' and 'Backtest_End_Date' columns to datetime objects
        df_bt_results['Backtest_Start_Date'] = pd.to_datetime(df_bt_results['Backtest_Start_Date'])
        df_bt_results['Backtest_End_Date'] = pd.to_datetime(df_bt_results['Backtest_End_Date'])

        # if (not search_strategy) and (not search_symbol):
        #     st.dataframe(df_bt_results)
        if search_strategy:
            df_bt_results = df_bt_results[df_bt_results['Strategy_Id'].isin(search_strategy)]
        if search_symbol:
            df_bt_results = df_bt_results[df_bt_results['Symbol'].isin(search_symbol)]
        if search_timeframe:
            df_bt_results = df_bt_results[df_bt_results['Time_Frame'].isin(search_timeframe)]
        if search_return_pct:
            df_bt_results = df_bt_results[df_bt_results['Return_Perc'] > 0]
        if search_date_ini and search_date_end:
            # Convert search_dates tuple to datetime objects
            start_date = datetime(search_date_ini.year, search_date_ini.month, search_date_ini.day)
            end_date = datetime(search_date_end.year, search_date_end.month, search_date_end.day)
            # Find overlapping periods
            df_bt_results = df_bt_results[
                (df_bt_results['Backtest_Start_Date'] <= end_date) & 
                (df_bt_results['Backtest_End_Date'] >= start_date)
            ]
        # df_bt_results = database.get_all_backtesting_results(database.conn)

        # add backtest link
        # Function to generate backtest link
        def generate_backtest_link(row, type):
            strategy_id = str(row['Strategy_Id'])
            time_frame = row['Time_Frame']
            symbol = row['Symbol']
            filename = f'{strategy_id} - {time_frame} - {symbol}.{type}'

            file_path = os.path.join(FOLDER_BACKTEST_RESULTS, filename)
            if os.path.exists(file_path):
                file_path = os.path.join("app", FOLDER_BACKTEST_RESULTS, filename)
                backtest_link = file_path
            else:
                backtest_link = ""
                
            return backtest_link

        # Apply the function to create the "Backtest_Link" column
        df_bt_results['Backtest_HTML'] = df_bt_results.apply(lambda row: generate_backtest_link(row, "html"), axis=1)
        df_bt_results['Backtest_CSV'] = df_bt_results.apply(lambda row: generate_backtest_link(row, "csv"), axis=1)

        st.dataframe(
            df_bt_results,
            column_config={
                "Strategy_Id": None,
                "Backtest_HTML": st.column_config.LinkColumn(
                    display_text="Open",
                    help="Backtesting results in HTML",
                    
                ),
                "Backtest_CSV": st.column_config.LinkColumn(
                    display_text="Open",
                    help="Backtesting results in CSV",
                    
                )
            }
        )
        
        st.subheader("Backtesting Trades")
        get_trades = st.button("Get Trades", key="get_trades")
        if get_trades:
            df_bt_trades = database.get_all_backtesting_trades(connection)

            # Convert 'Backtest_Start_Date' and 'Backtest_End_Date' columns to datetime objects
            df_bt_trades['EntryTime'] = pd.to_datetime(df_bt_trades['EntryTime'])
            df_bt_trades['ExitTime'] = pd.to_datetime(df_bt_trades['ExitTime'])

            if search_strategy:
                df_bt_trades = df_bt_trades[df_bt_trades['Strategy_Id'].isin(search_strategy)]
            if search_symbol:
                df_bt_trades = df_bt_trades[df_bt_trades['Symbol'].isin(search_symbol)]
            if search_timeframe:
                df_bt_trades = df_bt_trades[df_bt_trades['Time_Frame'].isin(search_timeframe)]
            # if search_return_pct:
            #     df_bt_trades = df_bt_trades[df_bt_trades['ReturnPct'] > 0]
            if search_date_ini and search_date_end:
                # Convert search_dates tuple to datetime objects
                start_date = datetime(search_date_ini.year, search_date_ini.month, search_date_ini.day)
                end_date = datetime(search_date_end.year, search_date_end.month, search_date_end.day)
                # Now perform the search
                # df_bt_trades = df_bt_trades[(df_bt_trades['EntryTime'] >= start_date) & (df_bt_trades['ExitTime'] <= end_date)]
                # Find overlapping periods
                df_bt_trades = df_bt_trades[
                    (df_bt_trades['EntryTime'] <= end_date) & 
                    (df_bt_trades['ExitTime'] >= start_date)
                ]

            st.dataframe(df_bt_trades)

            # Count the number of trades with ReturnPct below -20
            trades_below_minus20 = df_bt_trades[df_bt_trades['ReturnPct'] < -20].shape[0]

            # Count the number of trades with ReturnPct above 100
            trades_above_100 = df_bt_trades[df_bt_trades['ReturnPct'] > 100].shape[0]

            trades_minus20_minus10 = df_bt_trades[(df_bt_trades['ReturnPct'] >= -20) & (df_bt_trades['ReturnPct'] < -10)].shape[0]
            trades_minus10_0 = df_bt_trades[(df_bt_trades['ReturnPct'] >= -10) & (df_bt_trades['ReturnPct'] < 0)].shape[0]
            trades_0_10 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 0) & (df_bt_trades['ReturnPct'] < 10)].shape[0]
            trades_10_20 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 10) & (df_bt_trades['ReturnPct'] < 20)].shape[0]
            trades_20_30 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 20) & (df_bt_trades['ReturnPct'] < 30)].shape[0]
            trades_30_40 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 30) & (df_bt_trades['ReturnPct'] < 40)].shape[0]
            trades_40_50 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 40) & (df_bt_trades['ReturnPct'] < 50)].shape[0]
            trades_50_60 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 50) & (df_bt_trades['ReturnPct'] < 60)].shape[0]
            trades_60_70 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 60) & (df_bt_trades['ReturnPct'] < 70)].shape[0]
            trades_70_80 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 70) & (df_bt_trades['ReturnPct'] < 80)].shape[0]
            trades_80_90 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 80) & (df_bt_trades['ReturnPct'] < 90)].shape[0]
            trades_90_100 = df_bt_trades[(df_bt_trades['ReturnPct'] >= 90) & (df_bt_trades['ReturnPct'] < 100)].shape[0]

            # Trades in % terms
            trades_total = trades_below_minus20 + trades_minus20_minus10 + trades_minus10_0 + trades_0_10 + trades_10_20 + trades_20_30 + trades_30_40 + trades_40_50 + trades_50_60 + trades_60_70 + trades_70_80 + trades_80_90 + trades_90_100 + trades_above_100

            round_num = 2
            if trades_total != 0:
                trades_below_minus20_perc = round(trades_below_minus20/trades_total,round_num)*100
                trades_minus20_minus10_perc = round(trades_minus20_minus10/trades_total,round_num)*100
                trades_minus10_0_perc = round(trades_minus10_0/trades_total,round_num)*100
                trades_0_10_perc = round(trades_0_10/trades_total,round_num)*100
                trades_10_20_perc = round(trades_10_20/trades_total,round_num)*100
                trades_20_30_perc = round(trades_20_30/trades_total,round_num)*100
                trades_30_40_perc = round(trades_30_40/trades_total,round_num)*100
                trades_40_50_perc = round(trades_40_50/trades_total,round_num)*100
                trades_50_60_perc = round(trades_50_60/trades_total,round_num)*100
                trades_60_70_perc = round(trades_60_70/trades_total,round_num)*100
                trades_70_80_perc = round(trades_70_80/trades_total,round_num)*100
                trades_80_90_perc = round(trades_80_90/trades_total,round_num)*100
                trades_90_100_perc = round(trades_90_100/trades_total,round_num)*100
                trades_above_100_perc = round(trades_above_100/trades_total,round_num)*100
            else:
                trades_below_minus20_perc = 0
                trades_minus20_minus10_perc = 0
                trades_minus10_0_perc = 0
                trades_0_10_perc = 0
                trades_10_20_perc = 0
                trades_20_30_perc = 0
                trades_30_40_perc = 0
                trades_40_50_perc = 0
                trades_50_60_perc = 0
                trades_60_70_perc = 0
                trades_70_80_perc = 0
                trades_80_90_perc = 0
                trades_90_100_perc = 0
                trades_above_100_perc = 0
            
            trades_by_Return_perc = {
                'Category': ['< -20%',  '-20-10%','-10-0%','0-10%', '10-20%', '20-30%', '30-40%', '40-50%', '50-60%', '60-70%', '70-80%', '80-90%', '90-100%', '> 100%'],
                'Number of Trades': [trades_below_minus20] + [trades_minus20_minus10] + [trades_minus10_0] + [trades_0_10] + [trades_10_20] + [trades_20_30] + [trades_30_40] + [trades_40_50] + [trades_50_60] + [trades_60_70] + [trades_70_80] + [trades_80_90] + [trades_90_100] + [trades_above_100]
            }
            df_tbrp = pd.DataFrame(trades_by_Return_perc)
            df_tbrp["Perc of Trades"] = [
                trades_below_minus20_perc,
                trades_minus20_minus10_perc,
                trades_minus10_0_perc,
                trades_0_10_perc, 
                trades_10_20_perc, 
                trades_20_30_perc, 
                trades_30_40_perc, 
                trades_40_50_perc, 
                trades_50_60_perc, 
                trades_60_70_perc, 
                trades_70_80_perc, 
                trades_80_90_perc, 
                trades_90_100_perc, 
                trades_above_100_perc
                ]

            # Define the order of categories
            category_order = ['< -20%', '-20-10%', '-10-0%', '0-10%', '10-20%', '20-30%', '30-40%', '40-50%', '50-60%', '60-70%', '70-80%', '80-90%', '90-100%', '> 100%']

            # Plotting the cheese chart using Altair
            chart_tbrp = alt.Chart(df_tbrp).mark_bar().encode(
                x=alt.X("Category", title="Return %", scale=alt.Scale(domain=category_order)),
                # y=alt.Y("Number of Trades", title="Number of Trades"),
                y=alt.Y("Perc of Trades", title="Percentage of Trades"),
                color=alt.Color("Number of Trades")
            ).properties(
                title="Distribution of Trades"
            )

            st.altair_chart(chart_tbrp, use_container_width=True)

            # Display the results in a table
            st.dataframe(
                df_tbrp,
                hide_index=True,
                height=(len(df_tbrp) + 1) * 35 + 3)
            
def settings():

    col1_cfg, col2_cfg = tab_settings.columns(2)    

    with col2_cfg:
        container_main_strategy = st.container(border=True)
        with container_main_strategy:
            st.write("**Main Strategy**")

            if "main_strategy" not in st.session_state:
                st.session_state.main_strategy = config.get_setting("main_strategy")
            def main_strategy_change():
                config.set_setting("main_strategy", st.session_state.main_strategy)
            main_strategy = st.selectbox(
                label='Main Strategy',
                options=list(dict_strategies_main.keys()),
                key="main_strategy",
                on_change=main_strategy_change,
                format_func=format_func_strategies_main, 
                label_visibility="collapsed"
            )
        
        
        container_btc_strategy = st.container(border=True)
        with container_btc_strategy:
            st.write("**Bitcoin Strategy**")

            if "btc_strategy" not in st.session_state:
                st.session_state.btc_strategy = config.get_setting("btc_strategy")
            def btc_strategy_change():
                config.set_setting("btc_strategy", st.session_state.btc_strategy)
            btc_strategy = st.selectbox(
                label='BTC Strategy',
                options=list(dict_strategies_btc.keys()),
                key="btc_strategy",
                on_change=btc_strategy_change,
                format_func=format_func_strategies_btc, 
                label_visibility="collapsed"
            )

            if "trade_against_switch" not in st.session_state:
                st.session_state.trade_against_switch = config.get_setting("trade_against_switch")
            def trade_against_switch_change():
                config.set_setting("trade_against_switch", st.session_state.trade_against_switch)
            trade_against_switch = st.checkbox(
                label="Automatically switch between trading against Stablecoins (USDT, USDC) or Bitcoin (BTC)",
                key="trade_against_switch",
                on_change=trade_against_switch_change,
                help="""Considering the chosen Bitcoin strategy will decide whether it is a Bull or Bear market. If Bull then will convert stablecoin to BTC and trade against BTC. If Bear will convert BTC into stablecoin and trade against stablecoin."""
            )

            if "trade_against_switch_stablecoin" not in st.session_state:
                st.session_state.trade_against_switch_stablecoin = config.get_setting("trade_against_switch_stablecoin")
            def trade_against_switch_stablecoin_change():
                config.set_setting("trade_against_switch_stablecoin", st.session_state.trade_against_switch_stablecoin)
            trade_against_switch_stablecoin = st.selectbox(
                label='Stablecoin for auto-switching',
                options=['USDC','USDT'], 
                key="trade_against_switch_stablecoin",
                on_change=trade_against_switch_stablecoin_change,
            )
        
        # run_backtesting = st.button("Run Backtesting", help="Please be patient, as it could take a few hours to complete.")
        # if run_backtesting:
        #     with st.spinner('This task is taking a leisurely stroll through the digital landscape (+/- 1h). Why not do the same? Stretch those legs, grab a snack, or contemplate the meaning of life.'):
        #         trade_against = config.get_setting("trade_against") 
        #         force_run_backtest(time_frame="1d")

        container_bot_prefix = st.container(border=True)
        with container_bot_prefix:
            if "bot_prefix" not in st.session_state:
                st.session_state.bot_prefix = config.get_setting("bot_prefix")
            def bot_prefix_change():
                config.set_setting("bot_prefix", st.session_state.bot_prefix)
            bot_prefix = st.text_input(
                label="Telegram Messages Prefix",
                key="bot_prefix",
                on_change=bot_prefix_change,
                help="When there are multiple instances of BEC running, the prefix is useful to distinguish which BEC the telegram message belongs to."
            )

    with col1_cfg:
        container_bots = st.container(border=True)
        with container_bots:
            
            st.write("**Trading by Time Frame**")
            
            if "bot_1d" not in st.session_state:
                st.session_state.bot_1d = config.get_setting("bot_1d")
            def bot_1d_change():
                config.set_setting("bot_1d", st.session_state.bot_1d)
            bot_1d = st.toggle(
                label='Enable 1d',
                key="bot_1d",
                on_change=bot_1d_change,
                help="""
                    **:green[Enabled]**: Buy new positions and sell existing ones based on the daily timeframe.  
                    **:red[Disabled]**: Will not buy new positions but will continue to attempt to sell existing positions based on sell strategy conditions.
                """
            )
            
            if "bot_4h" not in st.session_state:
                st.session_state.bot_4h = config.get_setting("bot_4h")
            def bot_4h_change():
                config.set_setting("bot_4h", st.session_state.bot_4h)
            bot_4h = st.toggle(
                label='Enable 4h',
                key="bot_4h",
                on_change=bot_4h_change,
                help="""
                    **:green[Enabled]**: Buy new positions and sell existing ones based on the 4h timeframe.  
                    **:red[Disabled]**: Will not buy new positions but will continue to attempt to sell existing positions based on sell strategy conditions.
                """
            )
            
            if "bot_1h" not in st.session_state:
                st.session_state.bot_1h = config.get_setting("bot_1h")
            def bot_1h_change():
                config.set_setting("bot_1h", st.session_state.bot_1h)
            bot_1h = st.toggle(
                label='Enable 1h',
                key="bot_1h",
                on_change=bot_1h_change,
                help="""
                    **:green[Enabled]**: Buy new positions and sell existing ones based on the 1h timeframe.  
                    **:red[Disabled]**: Will not buy new positions but will continue to attempt to sell existing positions based on sell strategy conditions.
                """
            )
        
        # try:
        #     prev_stake_amount_type = config.get_setting("stake_amount_type")
        #     stake_amount_type = st.selectbox('Stake Amount Type', ['unlimited'], 
        #                                     help="""Stake_amount is the amount of stake the bot will use for each trade. 
        #                                         \nIf stake_amount = "unlimited" the increasing/decreasing of stakes will depend on the performance of the bot. Lower stakes if the bot is losing, higher stakes if the bot has a winning record since higher balances are available and will result in profit compounding.
        #                                         \nIf stake amount = static number, that is the amount per trade
        #                                     """)
        #     if stake_amount_type != prev_stake_amount_type:
        #         config.set_setting("stake_amount_type", stake_amount_type)
        # except KeyError:
        #     st.warning('Invalid or missing configuration: stake_amount_type')
        #     st.stop()
            
        container_others = st.container(border=True)
        with container_others:

            if "trade_against" not in st.session_state:
                st.session_state.trade_against = config.get_setting("trade_against")
            def trade_against_change():
                config.set_setting("trade_against", st.session_state.trade_against)
                min_position_size_change()
            trade_against = st.selectbox(
                label='Trade Against',
                options=['USDC','USDT', 'BTC'], 
                key="trade_against",
                on_change=trade_against_change,
                help="""Trade against USDC, USDT or BTC
                    """
            )
            
            if "max_number_of_open_positions" not in st.session_state:
                st.session_state.max_number_of_open_positions = config.get_setting("max_number_of_open_positions")
            def max_number_of_open_positions_change():
                config.set_setting("max_number_of_open_positions", st.session_state.max_number_of_open_positions)
            max_number_of_open_positions = st.number_input(
                label="Max Number of Open Positions",
                min_value=1,
                # max_value=50,
                step=1,
                key="max_number_of_open_positions",
                on_change=max_number_of_open_positions_change,
                help="""
                If tradable balance = 1000 and max_number_of_open_positions = 10, the stake_amount = 1000/10 = 100
                        """
            )
            
            # calc the position value for the next position
            ta = st.session_state.trade_against
            current_balance = binance.get_symbol_balance(ta)
            current_balance = round(current_balance, num_decimals)
            total_locked_values = database.get_total_locked_values(database.conn) 
            total_locked_values = round(total_locked_values, num_decimals)
            available_balance = current_balance - total_locked_values
            available_balance = round(available_balance, num_decimals)
            num_open_positions = database.get_num_open_positions(database.conn)
            max_number_of_open_positions = config.get_setting("max_number_of_open_positions")
            remaining_positions = max_number_of_open_positions-num_open_positions
            if remaining_positions <= 0:
                remaining_positions = 0
                position_size = 0
            else:
                position_size = available_balance/remaining_positions
                position_size = round(position_size, num_decimals)

            # st.caption(f"""
            #     Balance: {current_balance} {ta}
            #     <br>
            #     Total Locked Values: {total_locked_values} {ta}
            #     <br>
            #     Available Open Positions: {remaining_positions}
            #     <br>
            #     Next Trade Position Size = Balance - Total Locked Values / Available Open Positions = **{position_size} {ta}**
            #     """,
            #     help="Next Trade Position Size = (Balance - Total Locked Values) ÷ Available Open Positions",
            #     unsafe_allow_html=True)

            st.caption(f"""
                Balance: {current_balance} {ta}<br>
                Total Locked Values: {total_locked_values} {ta}<br>
                Available Open Positions: {remaining_positions}<br>
                Next Trade Position Size: **{position_size} {ta}**<br>
                """,
                help="Next Trade Position Size = (Balance - Total Locked Values) ÷ Available Open Positions",
                unsafe_allow_html=True)

            # st.latex(r"""
            # \text{Next Trade Position Size} = 
            # \frac{\text{Balance} - \text{Total Locked Values}}{\text{Available Open Positions}}
            # """)

            # st.caption(f"Next Trade Position Size: {position_size} {ta}", help="Next Trade Position Size = (Balance - Total Locked Values) ÷ Available Open Positions")
            
            col1_pos, col2_stop = st.columns(2)
            
            with col1_pos:
                # min_position_size 
                if "min_position_size" not in st.session_state:
                    st.session_state.min_position_size = float(config.get_setting("min_position_size"))
                MIN_POSITION_SIZE_USD = 20
                def min_position_size_change():
                    if st.session_state.trade_against in ["USDC", "USDT"]:
                        if int(st.session_state.min_position_size) < MIN_POSITION_SIZE_USD:
                            st.session_state.min_position_size = MIN_POSITION_SIZE_USD
                    elif st.session_state.trade_against == "BTC":
                        if float(st.session_state.min_position_size) >= MIN_POSITION_SIZE_USD:
                            st.session_state.min_position_size = 0.0001                    
                    config.set_setting("min_position_size", st.session_state.min_position_size)

                if trade_against in ["USDC", "USDT"]:
                    trade_min_val = MIN_POSITION_SIZE_USD
                    trade_step = 10
                    trade_format = None
                elif trade_against == "BTC":
                    trade_min_val = 0.0001
                    trade_step = 0.0001
                    trade_format = "%.4f"    

                min_position_size = st.number_input(
                    label='Minimum Position Size', 
                    min_value=float(trade_min_val), 
                    step=float(trade_step),
                    format=trade_format,
                    key="min_position_size",
                    on_change=min_position_size_change,
                    help="""If trade_against = USDT/USDC => min_position_size = 20
                        \nIf trade_against = BTC => min_position_size = 0.0001
                    """
                )      
                # ---  

            with col2_stop:
                if "stop_loss" not in st.session_state:
                    st.session_state.stop_loss = config.get_setting("stop_loss")
                def stop_loss_change():
                    config.set_setting("stop_loss", st.session_state.stop_loss)
                stop_loss = st.number_input(
                    label='Stop Loss %', 
                    min_value=0, 
                    step=1,
                    key="stop_loss",
                    on_change=stop_loss_change,
                    help="""Set stop loss to automatically sell if its price falls below a certain percentage.
                        \nExamples:
                        \n stop_loss = 0 => will not use stop loss.
                        \n stop_loss = 10 => 10%.   
                    """
                )

            if "trade_top_performance" not in st.session_state:
                st.session_state.trade_top_performance = config.get_setting("trade_top_performance")
            def trade_top_performance_change():
                config.set_setting("trade_top_performance", st.session_state.trade_top_performance)
            trade_top_performance = st.slider(
                label='Trade Top Performance Symbols', 
                min_value=0, 
                max_value=500, 
                step=5,
                key="trade_top_performance",
                on_change=trade_top_performance_change,
                help="""
                        Trade top X performance symbols                                              
                    """
            )
            
            if "tradable_balance_ratio" not in st.session_state:
                st.session_state.tradable_balance_ratio = config.get_setting("tradable_balance_ratio")*100
            
            # st.session_state.tradable_balance_ratio

            def tradable_balance_ratio_change():
                config.set_setting("tradable_balance_ratio", st.session_state.tradable_balance_ratio/100)

            tradable_balance_ratio = st.slider(
                label='Tradable Balance Ratio',
                min_value=0, 
                max_value=100,
                step=1, 
                format="%d%%",
                key="tradable_balance_ratio",
                on_change=tradable_balance_ratio_change, 
                help="""Tradable percentage of the balance
                    """
            )   
        
        
        st.write("**Take-Profit Levels**")
        container_tp = st.container(border=True)
        with container_tp:
            
            col1_tp1, col2_tp1 = st.columns(2)
            
            with col1_tp1:
                if "take_profit_1" not in st.session_state:
                    st.session_state.take_profit_1 = config.get_setting("take_profit_1")
                def take_profit_1_change():
                    config.set_setting("take_profit_1", st.session_state.take_profit_1)
                    
                take_profit_1 = st.number_input(
                    label="TP1 (%)", 
                    min_value=0, 
                    step=1,
                    key="take_profit_1",
                    on_change=take_profit_1_change,
                    help="The percentage increase in price at which the system will automatically trigger a sell order to secure profits."
                )                    
            with col2_tp1:
                if "take_profit_1_amount" not in st.session_state:
                    st.session_state.take_profit_1_amount = config.get_setting("take_profit_1_amount")
                def take_profit_1_amount_change():
                    config.set_setting("take_profit_1_amount", st.session_state.take_profit_1_amount)
                take_profit_1_amount = st.number_input(
                    label="TP1 Amount (%)", 
                    min_value=5, 
                    max_value=100,
                    step=5,
                    key="take_profit_1_amount",
                    on_change=take_profit_1_amount_change,
                    help="The percentage of the position quantity to be sold when take profits level 1 is achieved."
                )
        
            col1_tp2, col2_tp2 = st.columns(2)
            
            with col1_tp2:
                if "take_profit_2" not in st.session_state:
                    st.session_state.take_profit_2 = config.get_setting("take_profit_2")
                def take_profit_2_change():
                    config.set_setting("take_profit_2", st.session_state.take_profit_2)

                take_profit_2 = st.number_input(
                    label="TP2 (%)", 
                    min_value=0, 
                    step=1,
                    key="take_profit_2",
                    on_change=take_profit_2_change,
                    help="The percentage increase in price at which the system will automatically trigger a sell order to secure profits."
                )

            with col2_tp2:
                if "take_profit_2_amount" not in st.session_state:
                    st.session_state.take_profit_2_amount = config.get_setting("take_profit_2_amount")
                def take_profit_2_amount_change():
                    config.set_setting("take_profit_2_amount", st.session_state.take_profit_2_amount)

                take_profit_2_amount = st.number_input(
                    label="TP2 Amount (%)", 
                    min_value=5, 
                    max_value=100,
                    step=5,
                    key="take_profit_2_amount",
                    on_change=take_profit_2_amount_change,
                    help="The percentage of the position quantity to be sold when take profits level 2 is achieved."
                )
            
            col1_tp3, col2_tp3 = st.columns(2)
            
            with col1_tp3:
                if "take_profit_3" not in st.session_state:
                    st.session_state.take_profit_3 = config.get_setting("take_profit_3")
                def take_profit_3_change():
                    config.set_setting("take_profit_3", st.session_state.take_profit_3)

                take_profit_3 = st.number_input(
                    label="TP3 (%)", 
                    min_value=0, 
                    step=1,
                    key="take_profit_3",
                    on_change=take_profit_3_change,
                    help="The percentage increase in price at which the system will automatically trigger a sell order to secure profits."
                )

            with col2_tp3:
                if "take_profit_3_amount" not in st.session_state:
                    st.session_state.take_profit_3_amount = config.get_setting("take_profit_3_amount")
                def take_profit_3_amount_change():
                    config.set_setting("take_profit_3_amount", st.session_state.take_profit_3_amount)

                take_profit_3_amount = st.number_input(
                    label="TP3 Amount (%)", 
                    min_value=5, 
                    max_value=100,
                    step=5,
                    key="take_profit_3_amount",
                    on_change=take_profit_3_amount_change,
                    help="The percentage of the position quantity to be sold when take profits level 3 is achieved."
                )
            
            col1_tp4, col2_tp4 = st.columns(2)
            
            with col1_tp4:
                if "take_profit_4" not in st.session_state:
                    st.session_state.take_profit_4 = config.get_setting("take_profit_4")
                def take_profit_4_change():
                    config.set_setting("take_profit_4", st.session_state.take_profit_4)

                take_profit_4 = st.number_input(
                    label="TP4 (%)", 
                    min_value=0, 
                    step=1,
                    key="take_profit_4",
                    on_change=take_profit_4_change,
                    help="The percentage increase in price at which the system will automatically trigger a sell order to secure profits."
                )
                
            with col2_tp4:
                if "take_profit_4_amount" not in st.session_state:
                    st.session_state.take_profit_4_amount = config.get_setting("take_profit_4_amount")
                def take_profit_4_amount_change():
                    config.set_setting("take_profit_4_amount", st.session_state.take_profit_4_amount)

                take_profit_4_amount = st.number_input(
                    label="TP4 Amount (%)", 
                    min_value=5, 
                    max_value=100,
                    step=5,
                    key="take_profit_4_amount",
                    on_change=take_profit_4_amount_change,
                    help="The percentage of the position quantity to be sold when take profits level 4 is achieved."
                )
            
        st.write("**Locked Values**")
        container_lv = st.container(border=True)
        with container_lv:
            
            if "lock_values" not in st.session_state:
                st.session_state.lock_values = config.get_setting("lock_values")
            
            def lock_values_change():
                if not st.session_state.lock_values:
                    database.release_all_values(database.conn)
                config.set_setting("lock_values", st.session_state.lock_values)

            lock_values = st.checkbox(
                label='Lock Values from partial sales',
                key="lock_values",
                on_change=lock_values_change,
                help="""When **enabled**, means that any amount obtained from partially selling a position will be temporarily locked and cannot be used to purchase another position until the entire position is sold. 
                    \nWhen **disabled**, partial sales can be freely reinvested into new positions. It's important to note that this may increase the risk of larger position amounts, as funds from partial sales may be immediately reinvested without reservation.
                    \nNote that disabling this option will **release all** locked values.
                """
            )
            if st.session_state.lock_values:
                st.caption("Note that disabling this option will **release all** locked values.")
            
            expander_lv = st.expander(label=f"Current Locked Values", expanded=False)
            with expander_lv:
                df_clv = database.get_all_locked_values(database.conn)
                df_clv_selections = df_clv.copy()
                df_clv_selections.insert(0, "Select", value=False)

                col_config = { 
                    "Select": st.column_config.CheckboxColumn(label="Select", required=True, default=False),
                    "Id": None
                }
                # st.dataframe(df_clv_selections)
                edited_clv = st.data_editor(
                    df_clv_selections,
                    hide_index=True,
                    column_config=col_config,
                    disabled=["Bot", "Symbol", "Locked_Amount","Locked_At"],
                    )
                # Filter the dataframe using the select column
                selected_rows = edited_clv[edited_clv.Select]
                clv_list_selected_rows_id = selected_rows.Id.to_list()
                if st.button("Unlock Values",  disabled=selected_rows.empty):
                    for item_id in clv_list_selected_rows_id:
                        if item_id > 0: # ignore the TOTAL row
                            database.release_locked_value_by_id(database.conn, item_id)
                    st.rerun()

    
def check_app_version():
    last_date = general.extract_date_from_local_changelog()
    if last_date:
        app_version = last_date
    else:
        app_version = "App version not found"
    st.caption(f'**{bot_selected}** - {trade_against} - App Version {app_version}')

    github_last_date = general.extract_date_from_github_changelog()
    if github_last_date != last_date:
        st.warning("Update Available! A new version of the BEC is available. Click UPDATE to get the latest features and improvements. Check the [Change Log](https://github.com/jptsantossilva/BEC/blob/main/CHANGELOG.md) for more details.")
        update_version = st.button('UPDATE', key="update_version")
        if update_version:
            with st.spinner('🎉 Hold on tight! 🎉 Our elves are sprinkling magic dust on the app to make it even better.'):
                result = update.main() 
                st.code(result)

                restart_time = 10
                progress_text = f"App will restart in {restart_time} seconds."
                my_bar = st.progress(0, text=progress_text)

                for step in range(restart_time+1):
                    progress_percent = step * 10
                    if progress_percent != 0:
                        restart_time -= 1
                        progress_text = f"App will restart in {restart_time} seconds."
                    my_bar.progress(progress_percent, text=progress_text)
                    time.sleep(1)  

                st.rerun()

def show_main_page():
    
    global trade_against
    trade_against = config.get_setting("trade_against") 

    global num_decimals
    num_decimals = 8 if trade_against == "BTC" else 2
    # num_decimals = 2  

    # Get the current directory
    current_dir = os.getcwd()
    # Get the parent directory
    parent_dir = os.path.basename(current_dir)

    global bot_selected
    bot_selected = parent_dir  

    check_app_version()      

    get_chart_daily_balance(asset="USD")
    get_chart_daily_balance(asset="BTC")
    get_chart_daily_asset_balances()

    global tab_upnl, tab_rpnl, tab_top_perf, tab_signals, tab_blacklist, tab_backtesting_results, tab_settings
    tab_upnl, tab_rpnl, tab_signals, tab_top_perf, tab_blacklist, tab_backtesting_results, tab_settings = st.tabs(["Unrealized PnL", "Realized PnL", "Signals", "Top Performers", "Blacklist", "Backtesting Results", "Settings"])

    realized_pnl()
    unrealized_pnl()
    signals()
    top_performers()

    with tab_blacklist:
        blacklist()
    
    backtesting_results()
    settings()

def check_open_positions(bot: str):
    num = database.get_num_open_positions_by_bot(connection=connection, bot=bot)
    if num > 0:
        msg = f"There are {num} open position on Bot_{bot}. If you turn the bot OFF this positions will remain open. Make sure you close them."
        st.warning(msg) 

def show_form_reset_password():
    if st.session_state.authentication_status and st.session_state.reset_form_open:
        try:
            with st.form(key="reset_password"):
                st.subheader("Reset password")
                # password = st.text_input('Current password', type='password')
                new_password = st.text_input('New password', type='password', key="new_password")
                new_password_repeat = st.text_input('Repeat password', type='password', key="new_password_repeat")

                # st.form_submit_button(label='Reset Password', on_click=reset_password_submitted(True))
                submitted = st.form_submit_button(label='Reset Password')
                if submitted:
                    reset_password_submitted(True)

            if 'reset_password_submitted' in  st.session_state:
                if st.session_state.reset_password_submitted == True:
                    if len(new_password) > 0:
                        if new_password == new_password_repeat:
                            if 1 == 1: #password != new_password: 
                                new_password_hashed = stauth.Hasher([new_password]).generate()[0]
                                database.update_user_password(connection, username=st.session_state.username, password=new_password_hashed)
                                st.success('Password updated!')
                                time.sleep(3)
                                reset_password_submitted(False)
                                reset_form_open(False)
                                st.rerun()
                            else:
                                st.error('New and current passwords are the same')
                        else:
                            st.error('Passwords do not match')
                    else:
                        st.error('No new password provided')

        except Exception as e:
            st.error(e)

def create_user():
    try:
        if authenticator.register_user(form_name='Register user', preauthorization=True):
            st.success('User registered successfully')

        # authenticator.credentials
    except Exception as e:
        st.error(e)

def forgot_password():
    try:
        username_forgot_pw, email_forgot_password, random_password = authenticator.forgot_password('Forgot password')
        if username_forgot_pw:
            st.success('New password sent securely')
            # Random password to be transferred to user securely
        elif username_forgot_pw == False:
            st.error('Username not found')
    except Exception as e:
        st.error(e)

# Get years from orders
def get_years(bot):
    years = database.get_years_from_orders(connection)
    return years

# get months with orders within the year
def get_orders_by_month(year: str):

    months = database.get_months_from_orders_by_year(connection, year)

    month_dict = {}
    for month in months:
        month_name = calendar.month_name[month]
        month_dict[month] = month_name
    return month_dict
    
# Define a function to get the year and month from a datetime object
def get_year_month(date):
    return date.year, date.month

def calculate_realized_pnl(year: str, month: str):

    # print(f'Year = {year}')
    # if month == '13':
    #     print(f'Month = ALL')
    # else:
    #     print(f'Month = {month}')

    # print('\n Realized PnL')
    # print('---------------------')
    
    df_month_1d = database.get_orders_by_bot_side_year_month(connection, bot="1d", side="SELL", year=year, month=month)
    df_month_4h = database.get_orders_by_bot_side_year_month(connection, bot="4h", side="SELL", year=year, month=month)
    df_month_1h = database.get_orders_by_bot_side_year_month(connection, bot="1h", side="SELL", year=year, month=month)
    
    # set decimal precision 
    df_month_1d['Buy_Price'] = df_month_1d['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_1d['Sell_Price'] = df_month_1d['Sell_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_1d['Buy_Position_Value'] = df_month_1d['Buy_Position_Value'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_1d['Sell_Position_Value'] = df_month_1d['Sell_Position_Value'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    
    df_month_4h['Buy_Price'] = df_month_4h['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_4h['Sell_Price'] = df_month_4h['Sell_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_4h['Buy_Position_Value'] = df_month_4h['Buy_Position_Value'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_4h['Sell_Position_Value'] = df_month_4h['Sell_Position_Value'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    
    df_month_1h['Buy_Price'] = df_month_1h['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_1h['Sell_Price'] = df_month_1h['Sell_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_1h['Buy_Position_Value'] = df_month_1h['Buy_Position_Value'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    df_month_1h['Sell_Position_Value'] = df_month_1h['Sell_Position_Value'].apply(lambda x:f'{{:.{8}f}}'.format(x) if x is not None else 'None')
    
    # print('')              
    # print(df_month_1d)
    # print(df_month_4h)
    # print(df_month_1h)
    
    results_df = pd.DataFrame()
    # results_df = pd.DataFrame(columns=['bot','Year','Month','pnl_%','pnl_value','trades'])
    for timeframe, df_month in [('1d', df_month_1d), ('4h', df_month_4h), ('1h', df_month_1h)]:
        if df_month.empty:
            continue
        pnl_perc_sum = df_month.PnL_Perc.sum()
        pnl_value_sum = round(df_month.PnL_Value.sum(), num_decimals)
        trades = len(df_month) 
        df_new = pd.DataFrame({
                'Bot': [timeframe],
                'Year': [year],
                'Month': [month],
                'PnL_Perc': [pnl_perc_sum],
                'PnL_Value': [pnl_value_sum],
                'Trades': [trades]})
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # Calculate the sum of values in pnl 
    if not results_df.empty:
        sum_pnl_perc = round(results_df['PnL_Perc'].sum(), 2)
        sum_pnl_value = round(results_df['PnL_Value'].sum(), num_decimals)
        sum_trades = results_df['Trades'].sum()
    else:
        sum_pnl_perc = 0
        sum_pnl_value = 0
        sum_trades = 0

    # Add a new row at the end of the dataframe with the sum values
    if not results_df.empty:
        results_df.loc[len(results_df)] = ['TOTAL','', '', sum_pnl_perc, sum_pnl_value, sum_trades]
    else:
        df_data = [['TOTAL','', '', sum_pnl_perc, sum_pnl_value, sum_trades]]
        results_df = pd.DataFrame(df_data, columns=['Bot', 'Year', 'Month','PnL_Perc','PnL_Value','Trades'])

    # format the pnl_perc and pnl_value decimal places
    # format the pnl_value decimal places depending on trade against
    results_df['PnL_Perc'] = results_df['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    results_df['PnL_Value'] = results_df['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))

    df_month_1d['PnL_Perc'] = df_month_1d['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_month_1d['PnL_Value'] = df_month_1d['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    df_month_4h['PnL_Perc'] = df_month_4h['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_month_4h['PnL_Value'] = df_month_4h['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    df_month_1h['PnL_Perc'] = df_month_1h['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_month_1h['PnL_Value'] = df_month_1h['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))

    return results_df, df_month_1d, df_month_4h, df_month_1h

def calculate_unrealized_pnl():
    
    # print('\nUnrealized PnL')
    # print('---------------------')

    # results_df = pd.DataFrame(columns=['bot','pnl_%','pnl_value','positions'])

    df_positions_1d = database.get_unrealized_pnl_by_bot(connection, bot="1d")
    df_positions_4h = database.get_unrealized_pnl_by_bot(connection, bot="4h")
    df_positions_1h = database.get_unrealized_pnl_by_bot(connection, bot="1h")

    # set decimal precision 
    df_positions_1d['Buy_Price'] = df_positions_1d['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))
    df_positions_4h['Buy_Price'] = df_positions_4h['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))
    df_positions_1h['Buy_Price'] = df_positions_1h['Buy_Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))

    df_positions_1d['Position_Value'] = df_positions_1d['Position_Value'].apply(lambda x:f'{{:.{8}f}}'.format(x))
    df_positions_4h['Position_Value'] = df_positions_4h['Position_Value'].apply(lambda x:f'{{:.{8}f}}'.format(x))
    df_positions_1h['Position_Value'] = df_positions_1h['Position_Value'].apply(lambda x:f'{{:.{8}f}}'.format(x))

    # print(df_positions_1d)
    # print(df_positions_4h)
    # print(df_positions_1h)

    # dataframe with totals
    results_df = pd.DataFrame()
    for timeframe, df_positions in [('1d', df_positions_1d), ('4h', df_positions_4h), ('1h', df_positions_1h)]:
        pnl_perc_sum = df_positions.PnL_Perc.sum()
        pnl_value_sum = df_positions.PnL_Value.sum()
        positions = len(df_positions) 
        df_new = pd.DataFrame({
                'Bot': [timeframe],
                'PnL_Perc': [pnl_perc_sum],
                'PnL_Value': [pnl_value_sum],
                'Positions': [positions]})
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # Calculate the sums of the PnLs and positions
    sum_pnl_perc = results_df['PnL_Perc'].sum()
    sum_pnl_value = results_df['PnL_Value'].sum()
    max_num_positions = config.get_setting("max_number_of_open_positions")
    sum_positions = f"{results_df['Positions'].sum()}/{max_num_positions}"
    
    results_df['Positions'] = results_df['Positions'].astype(str)
    # Add a new row at the end of the dataframe with the sum values
    results_df.loc[len(results_df)] = ['TOTAL', sum_pnl_perc, sum_pnl_value, sum_positions]

    # format the pnl_perc with 2 decimal places
    results_df['PnL_Perc'] = results_df['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    # format the pnl_value decimal places depending on trade against
    results_df['PnL_Value'] = results_df['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    
    # format the pnl_perc and pnl_value decimal places
    df_positions_1d['PnL_Perc'] = df_positions_1d['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_positions_1d['PnL_Value'] = df_positions_1d['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    
    df_positions_4h['PnL_Perc'] = df_positions_4h['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_positions_4h['PnL_Value'] = df_positions_4h['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    
    df_positions_1h['PnL_Perc'] = df_positions_1h['PnL_Perc'].apply(lambda x:'{:.2f}'.format(x))
    df_positions_1h['PnL_Value'] = df_positions_1h['PnL_Value'].apply(lambda x:f'{{:.{num_decimals}f}}'.format(x))
    
    
    return results_df, df_positions_1d, df_positions_4h, df_positions_1h

# define a function to set the background color of the rows based on pnl_value
def set_pnl_color(val):
    if val is not None:
        val = float(val)
        color = '#E9967A' if val < 0 else '#8FBC8F' if val > 0 else ''
        return f'background-color: {color}'

def reset_form_open(state):
    if 'reset_form_open' in  st.session_state:
        st.session_state.reset_form_open = state

def reset_password_submitted(state):
    if 'reset_password_submitted' in  st.session_state:
        st.session_state.reset_password_submitted = state

def format_func_strategies_main(option):
        return dict_strategies_main[option]

def format_func_strategies_btc(option):
        return dict_strategies_btc[option]

def format_func_strategies(option):
        return dict_strategies[option]

def main():

    # Initialization
    # if 'name' not in  st.session_state:
    #     st.session_state.name = ''
    # if 'username' not in  st.session_state:
    #     st.session_state.username = ''
    # if 'user_password' not in  st.session_state:
    #     st.session_state.user_password = 'None'
    # if 'reset_form_open' not in st.session_state:
    #     st.session_state.reset_form_open = False
    # if 'reset_password_submitted' not in  st.session_state:
    #     st.session_state.reset_password_submitted = False
    # if 'authentication_status' not in  st.session_state:
    #     st.session_state.authentication_status = None

    # connect to database
    global connection
    connection = database.connect()

    df_users = database.get_all_users(connection)
    # Convert the DataFrame to a dictionary
    credentials = df_users.to_dict('index')
    formatted_credentials = {'usernames': {}}
    # Iterate over the keys and values of the original `credentials` dictionary
    for username, user_info in credentials.items():
        # Add each username and its corresponding user info to the `formatted_credentials` dictionary
        formatted_credentials['usernames'][username] = user_info

    
    # get strategies
    df_strategies_main = database.get_strategies_for_main(connection)
    df_strategies_btc = database.get_strategies_for_btc(connection)
    df_strategies = database.get_all_strategies(connection)
    global dict_strategies_main, dict_strategies_btc, dict_strategies
    #create a dictionary with code and name columns
    dict_strategies_main = dict(zip(df_strategies_main['Id'], df_strategies_main['Name']))
    dict_strategies_btc = dict(zip(df_strategies_btc['Id'], df_strategies_btc['Name']))
    dict_strategies = dict(zip(df_strategies['Id'], df_strategies['Name']))

    global authenticator

    st.title(f'BEC Dashboard')

    authenticator = stauth.Authenticate(
        credentials=formatted_credentials,
        cookie_name="dashboard_cookie_name",
        key="dashboard_cookie_key",
        cookie_expiry_days=30
    )

    name, authentication_status, username = authenticator.login('Login', 'main')
    st.session_state.name = name
    st.session_state.username = username
    # st.session_state.user_password = authenticator.credentials['usernames'][username]['password']

    if authentication_status:
        authenticator.logout('Logout', 'sidebar')
        
        # reset_clicked = st.sidebar.button("Reset", on_click=reset_form_open(True))
        reset_clicked = st.sidebar.button("Reset", key="reset_clicked")
        if reset_clicked:
            reset_form_open(True)
        show_form_reset_password()

        # create_user_clicked = st.sidebar.button("Create User")
        # if create_user_clicked:
        #     create_user()

        st.sidebar.title(f'Welcome *{st.session_state.name}*')
        show_main_page()
    elif authentication_status == False:
        st.error('Username or password is incorrect')
    elif authentication_status == None:
        st.warning('Please enter your username and password')

if __name__ == "__main__":
    main()











