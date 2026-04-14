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
import utils.trading_service as trading_service

from my_backtesting import FOLDER_BACKTEST_RESULTS

# st.set_page_config(
#     page_title="BEC App",
#     page_icon="random",
#     layout="wide",
#     # initial_sidebar_state="auto",
#     menu_items={
#         'Get Help': 'https://github.com/jptsantossilva/BEC#readme',
#         'Report a bug': "https://github.com/jptsantossilva/BEC/issues/new",
#         'About': """# My name is BEC \n I am a Trading Bot and I'm trying to be an *extremely* cool app! 
#         \n This is my dad's 🐦 Twitter: [@jptsantossilva](https://twitter.com/jptsantossilva).
#         """
#     }
# )

# for testing purposes
# st.session_state

def realized_pnl():
    with tab_rpnl:
        # get years
        years = get_years()

        # Render an empty-state view without crashing the filter widgets.
        if len(years) == 0:
            st.info('There are no closed positions 🤞')
            year = str(datetime.now().year)
            month_number = 13
        else:
            filter_rpnl = st.container(horizontal=True, vertical_alignment="bottom")
            # col1, col2, col3, col4 = st.columns([4, 6, 4, 10], vertical_alignment='bottom')
            # years selectbox
            year = filter_rpnl.selectbox(
                'Year',
                (years),
                width=150
            )

            # get months
            months_dict = get_orders_by_month(year)
            month_names = list(months_dict.values())

            # months selectbox
            month_selected_name = filter_rpnl.selectbox(
                'Month',
                (month_names),
                width=200
            )

            disable_full_year = month_selected_name == None
            if month_selected_name == None:
                month_number = 1
            else: # get month number from month name using months dictionary 
                month_number = list(months_dict.keys())[list(months_dict.values()).index(month_selected_name)]

            if filter_rpnl.checkbox('Full Year', disabled=disable_full_year):
                month_number = 13

        result_closed_positions, trades_month_1d, trades_month_4h, trades_month_1h = calculate_realized_pnl(str(year), str(month_number))
        # print("\nPnL - Total")
        # print(result_closed_positions)

        st.header("Realized PnL - Total")
        result_closed_positions = result_closed_positions.style.map(set_pnl_color, subset=['PnL_Perc','PnL_Value'])
        st.dataframe(result_closed_positions, width="content", hide_index=True)    
    
        # print("Realized PnL - Detail")
        # print(trades_month_1d)
        # print(trades_month_4h)
        # print(trades_month_1h)

        st.header("Realized PnL - Detail")
        
        st.subheader("Bot 1d")
        st.dataframe(
            trades_month_1d.style.map(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            width="content",
            column_config = {
                "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                "Exit_Reason": st.column_config.TextColumn(width="large")
                }
        )
        
        st.subheader("Bot 4h")
        st.dataframe(
            trades_month_4h.style.map(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            width="content",
            column_config = {
                "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                "Exit_Reason": st.column_config.TextColumn(width="large")
                }
        )
        
        st.subheader("Bot 1h")
        st.dataframe(
            trades_month_1h.style.map(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            width="content",
            column_config = {
                "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
                "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
                "Exit_Reason": st.column_config.TextColumn(width="large")
                }
        )

        # print('\n----------------------------\n')

@st.dialog("Delete Position")
def delete_position(symbol, timeframe):
    st.info("As an example, use this to close a position when a symbol is delisted and you need to mark it as sold.")
    st.write(f"Are you sure you want to delete **{symbol}** from **{timeframe}** timeframe?")
    unit_price = st.number_input("Unit value", min_value=0.0, value=0.0, step=0.0001, format="%.8f", width=200)
    reason = st.text_input("Reason", value="Symbol delisted from exchange", max_chars=100, help="Reason for deleting the position.")
    if st.button("Delete", key="delete_position"):
        trading_service.delete_position(
            symbol=symbol,
            bot=timeframe,
            unit_price=float(unit_price),
            reason=reason
        ) 
        st.rerun()
        
def unrealized_pnl():
    with tab_upnl:
        def _highlight_trading_status(row):
            approved = str(row.get("Trading_Approved", "")).lower() in ("approved", "1", "true")
            color = '#8FBC8F' if approved else '#E9967A'
            change_style = f'background-color: {color}'
            return [
                change_style if col in ["Trading_Approved", "Trading_Rejection_Reasons"] else ""
                for col in row.index
            ]

        result_open_positions, positions_df_1d, positions_df_4h, positions_df_1h = calculate_unrealized_pnl()
        strategy_id = config.load_settings().strategy_id
        df_trading_status_all = database.get_top_performers_trading_status(strategy_id=strategy_id)

        def _prepare_trading_status_for_tf(tf: str):
            if df_trading_status_all.empty:
                return pd.DataFrame()
            df = df_trading_status_all[df_trading_status_all["Time_Frame"] == tf].copy()
            if df.empty:
                return df
            df["Trading_Approved"] = df["Trading_Approved"].apply(
                lambda x: "Approved" if int(x) == 1 else "Rejected"
            )
            df["Trading_Rejection_Reasons"] = df["Trading_Rejection_Reasons"].fillna("")
            df.loc[
                (df["Trading_Approved"] == "Rejected") & (df["Trading_Rejection_Reasons"] == ""),
                "Trading_Rejection_Reasons",
            ] = "Not evaluated yet"
            return df
        # print("\nUnrealized PnL - Total")
        # print('-------------------------------')
        # print(result_open_positions)

        if positions_df_1d.empty and positions_df_4h.empty and positions_df_1h.empty:
            st.info('There are no open positions 🤞') 

        st.header("Unrealized PnL - Total")    

        # Force column to string to avoid Arrow serialization errors (mixed int/str types like "14/24")
        result_open_positions['Positions'] = result_open_positions['Positions'].astype(str)

        result_open_positions = result_open_positions.style.map(set_pnl_color, subset=['PnL_Perc','PnL_Value'])
        st.dataframe(result_open_positions, width="content", hide_index=True)

        st.header(f"Unrealized PnL - Detail")
        
        st.subheader("Positions 1d")

        col_config = {
            "Id": None,
            "PnL_Perc": st.column_config.NumberColumn(format="%.2f"),
            "PnL_Value": st.column_config.NumberColumn(format=f"%.{num_decimals}f"),
            "TP1": st.column_config.CheckboxColumn(),
            "TP2": st.column_config.CheckboxColumn(),
            "TP3": st.column_config.CheckboxColumn(),
            "TP4": st.column_config.CheckboxColumn(),
            "RPS%": st.column_config.NumberColumn(help="Remaining Position Size (%)",
                                            #    format="%.2f",
                                                )
        }
        
        event_positions_1d = st.dataframe(
            positions_df_1d.style.map(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            width="content",
            key="positions_df_1d",
            column_config=col_config,
            hide_index=True,
            on_select="rerun",
            selection_mode=["single-row", "multi-column"],
        )
        with st.expander("Top performers eligibility 1d", expanded=False):
            st.caption("Backtesting Approval Rules status for top performers on 1d.")
            df_trading_status_1d = _prepare_trading_status_for_tf("1d")
            if df_trading_status_1d.empty:
                st.info("No trading approval data for 1d.")
            else:
                styled_status_1d = df_trading_status_1d.style.apply(_highlight_trading_status, axis=1)
                st.dataframe(styled_status_1d, width="content", hide_index=True)

        # event_positions_1d.selection

        row_1d_selected = len(event_positions_1d.selection.rows) > 0

        # Check if there's a selection
        if row_1d_selected:
            selected_row_index = event_positions_1d.selection.rows[0]  # Get the index of the selected row
            selected_symbol = positions_df_1d.loc[selected_row_index, 'Symbol']
            selected_bot = positions_df_1d.loc[selected_row_index, 'Bot']  

            # Show the selected Name
            # st.write(f"Selected Symbol {selected_symbol} and bot {selected_bot}")

            if st.button("Delete Position", key="delete_1d"):
                delete_position(symbol=selected_symbol, timeframe=selected_bot)
        
        
        #########################

        st.subheader("Positions 4h")
        
        event_positions_4h = st.dataframe(
            positions_df_4h.style.map(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            width="content",
            key="positions_df_4h",
            column_config=col_config,
            hide_index=True,
            on_select="rerun",
            selection_mode=["single-row", "multi-column"],
        )
        with st.expander("Top performers eligibility 4h", expanded=False):
            st.caption("Backtesting Approval Rules status for top performers on 4h.")
            df_trading_status_4h = _prepare_trading_status_for_tf("4h")
            if df_trading_status_4h.empty:
                st.info("No trading approval data for 4h.")
            else:
                styled_status_4h = df_trading_status_4h.style.apply(_highlight_trading_status, axis=1)
                st.dataframe(styled_status_4h, width="content", hide_index=True)

        # event_positions_1d.selection

        row_4h_selected = len(event_positions_4h.selection.rows) > 0

        # Check if there's a selection
        if row_4h_selected:
            selected_row_index = event_positions_4h.selection.rows[0]  # Get the index of the selected row
            selected_symbol = positions_df_4h.loc[selected_row_index, 'Symbol']
            selected_bot = positions_df_4h.loc[selected_row_index, 'Bot']  

            # Show the selected Name
            # st.write(f"Selected Symbol {selected_symbol} and bot {selected_bot}")

            if st.button("Delete Position", key="delete_4h"):
                delete_position(symbol=selected_symbol, timeframe=selected_bot)

        #########################
        
        st.subheader("Positions 1h")

        event_positions_1h = st.dataframe(
            positions_df_1h.style.map(set_pnl_color, subset=['PnL_Perc','PnL_Value']),
            width="content",
            key="positions_df_1h",
            column_config=col_config,
            hide_index=True,
            on_select="rerun",
            selection_mode=["single-row", "multi-column"],
        )
        with st.expander("Top performers eligibility 1h", expanded=False):
            st.caption("Backtesting Approval Rules status for top performers on 1h.")
            df_trading_status_1h = _prepare_trading_status_for_tf("1h")
            if df_trading_status_1h.empty:
                st.info("No trading approval data for 1h.")
            else:
                styled_status_1h = df_trading_status_1h.style.apply(_highlight_trading_status, axis=1)
                st.dataframe(styled_status_1h, width="content", hide_index=True)

        # event_positions_1d.selection

        row_1h_selected = len(event_positions_1h.selection.rows) > 0

        # Check if there's a selection
        if row_1h_selected:
            selected_row_index = event_positions_1h.selection.rows[0]  # Get the index of the selected row
            selected_symbol = positions_df_1h.loc[selected_row_index, 'Symbol']
            selected_bot = positions_df_1h.loc[selected_row_index, 'Bot']  

            # Show the selected Name
            # st.write(f"Selected Symbol {selected_symbol} and bot {selected_bot}")

            if st.button("Delete Position", key="delete_1h"):
                delete_position(symbol=selected_symbol, timeframe=selected_bot)

        #----------------------
        # Force Close Position
        st.header("Forced Sale")
        
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
                df_pos = database.get_positions_by_bot_symbol_position( bot=sell_bot, symbol=sell_symbol, position=1)
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
                sell_reason = f"Forced Sale of {sell_amount_perc}%"
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
                            reason=f"Forced Sale of {sell_amount_perc}%",
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
        top_perf = config.read_setting("trade_top_performance")
        st.subheader(f"Top {top_perf} Performers")
        st.caption("The top performers are those in accumulation phase (Price > 50DSMA and Price > 200DSMA and 50DSMA < 200DSMA) and bullish phase (Price > 50DSMA and Price > 200DSMA and 50DSMA > 200DSMA) and then sorted by the price above the 200-day moving average (DSMA) in percentage terms. [Click here for more details](https://twitter.com/jptsantossilva/status/1539976855469428738?s=20).")
        df_mp = database.get_all_symbols_by_market_phase()
        df_mp['Price'] = df_mp['Price'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['DSMA50'] = df_mp['DSMA50'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['DSMA200'] = df_mp['DSMA200'].apply(lambda x:f'{{:.{8}f}}'.format(x))
        df_mp['Perc_Above_DSMA50'] = df_mp['Perc_Above_DSMA50'].apply(lambda x:'{:.2f}'.format(x))
        df_mp['Perc_Above_DSMA200'] = df_mp['Perc_Above_DSMA200'].apply(lambda x:'{:.2f}'.format(x))
        st.dataframe(df_mp, width="content", hide_index=True)

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
        df_symbols_days_at_top = database.symbols_by_market_phase_Historical_get_symbols_days_at_top()
        st.dataframe(df_symbols_days_at_top, width="content", hide_index=True)

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
        df_s = database.get_all_signals_log(num_rows=100)
        st.dataframe(df_s, width="content")

@st.fragment
def blacklist():

    st.subheader("Blacklist")

    st.caption("""
        The blacklist allows you to exclude specific symbol tickers from trading.<br>
        When adding a ticker, enter only the base symbol (e.g., ETH, SOL, LTC) instead of the full trading pair (e.g., ETHUSDT, SOLUSDC, LTCBTC).""",
        unsafe_allow_html=True)

    df_blacklist = database.get_symbol_blacklist()

    # Hide 'Id' but keep it for internal tracking
    df_blacklist_display = df_blacklist[["Symbol"]]  # Only show 'Symbol' column

    # Allow user to edit the blacklist without showing 'Id'
    edited_blacklist_display = st.data_editor(df_blacklist_display, num_rows="dynamic", width="content")

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
        database.update_blacklist(edited_blacklist)

            # Remove deleted rows
        if not deleted_symbols.empty:
            database.delete_from_blacklist(deleted_symbols)

        st.rerun(scope="fragment")

def settings():
    with tab_settings:
        st.markdown("### Overview")
        # Compact header card
        ta = config.read_setting("trade_against")
        max_pos = config.read_setting("max_number_of_open_positions")
        total_locked = round(database.get_total_locked_values(), 2)
        cur_bal = round(binance.get_symbol_balance(ta), 2)
        avail = max(cur_bal - total_locked, 0)
        open_now = database.get_num_open_positions()
        rem = max(max_pos - open_now, 0)
        next_pos = 0 if rem == 0 else round(avail / rem, 2)
        # c1, c2, c3, c4, c5 = st.columns(5)
        # c1.metric("Trade Against", ta)
        # c2.metric("Max Open", max_pos)
        # c3.metric("Locked", f"{total_locked} {ta}")
        # c4.metric(f"Available {ta}", millify(avail, precision=2))
        # c5.metric(f"Next Pos Size {ta}", millify(next_pos, precision=2))

        c6, c7, c8, c9 = st.columns(4)
        c6.metric(f"Balance {ta}", millify(avail, precision=2))
        c7.metric(f"Locked {ta}", millify(total_locked, precision=2))    
        c8.metric("Available Positions", rem)
        c9.metric(f"Next Position Size {ta}", millify(next_pos, precision=2))
        
        # st.space("medium")
        # st.space("small")
        st.divider()
        
        st.markdown("### Strategies")
        with st.container(border=False):
            st.markdown("Main Strategy")
            st.caption(
                "Primary strategy used by the trading bots (1d/4h/1h) for buy/sell decisions. "
                "Also used in the daily refresh to apply approved backtesting results and update the symbols in Positions."
            )
            if "main_strategy" not in st.session_state:
                st.session_state.main_strategy = config.read_setting("main_strategy")
            st.selectbox(
                "Main Strategy", 
                list(dict_strategies_main.keys()), 
                key="main_strategy",
                on_change=lambda: config.update_setting("main_strategy", 
                st.session_state.main_strategy),
                format_func=format_func_strategies_main, 
                label_visibility="collapsed",
                width=400
            )
        
        with st.container(border=False):
            st.markdown("Bitcoin Strategy")
            st.caption(
                "Used by the Auto-switch logic to evaluate BTC market regime (bull/bear) on BTC/stablecoin. "
                "It controls when the app switches trade exposure between stablecoin and BTC."
            )
            if "btc_strategy" not in st.session_state:
                st.session_state.btc_strategy = config.read_setting("btc_strategy")
            st.selectbox(
                "BTC Strategy", 
                list(dict_strategies_btc.keys()), 
                key="btc_strategy",
                on_change=lambda: config.update_setting("btc_strategy", 
                st.session_state.btc_strategy),
                format_func=format_func_strategies_btc, 
                label_visibility="collapsed",
                width=400
            )
            
            with st.popover("Auto-switch (Advanced)"):
                st.caption(
                    "When enabled, BEC can close positions and convert balances between stablecoin and BTC "
                    "automatically. This can trigger full portfolio reallocation and materially change risk."
                )
                if "trade_against_switch" not in st.session_state:
                    st.session_state.trade_against_switch = config.read_setting("trade_against_switch")
                st.checkbox("Auto-switch Stablecoin/BTC", key="trade_against_switch",
                            on_change=lambda: config.update_setting("trade_against_switch", st.session_state.trade_against_switch))
                if "trade_against_switch_stablecoin" not in st.session_state:
                    st.session_state.trade_against_switch_stablecoin = config.read_setting("trade_against_switch_stablecoin")
                st.selectbox("Stablecoin for auto-switch", ["USDC","USDT"], key="trade_against_switch_stablecoin",
                            on_change=lambda: config.update_setting("trade_against_switch_stablecoin", st.session_state.trade_against_switch_stablecoin))

        st.space()

        st.markdown("### Trade Execution by Timeframe")
        with st.container():
            for schedule_name, lbl in [
                ("main_1d","Enable 1d"),
                ("main_4h","Enable 4h"),
                ("main_1h","Enable 1h"),
            ]:
                state_key = f"job_{schedule_name}_enabled"
                if state_key not in st.session_state:
                    st.session_state[state_key] = database.get_job_schedule_enabled(schedule_name)
                def _make_toggle(name=schedule_name, key=state_key):
                    database.set_job_schedule_enabled(name, bool(st.session_state[key]))
                st.toggle(lbl, key=state_key, on_change=_make_toggle,
                          help="""
                              **:green[Enabled]**: Buy new positions and sell existing ones based on the daily timeframe.  
                              **:red[Disabled]**: Will not buy new positions but will continue to attempt to sell existing positions based on sell strategy conditions.
                          """)
    
        st.space()

        st.markdown("### Position Sizing")
        with st.container(horizontal=True):
            
            # c1, c2 = st.columns(2)
            # Trade Against
            if "trade_against" not in st.session_state:
                st.session_state.trade_against = config.read_setting("trade_against")
            def on_ta(): 
                config.update_setting("trade_against", st.session_state.trade_against)
            st.selectbox(
                label="Trade Against", 
                options=["USDC","USDT","BTC"], 
                key="trade_against", 
                on_change=on_ta, 
                # width=200,
                )
            
            # Max open positions
            if "max_number_of_open_positions" not in st.session_state:
                st.session_state.max_number_of_open_positions = config.read_setting("max_number_of_open_positions")
            def on_max(): config.update_setting("max_number_of_open_positions", st.session_state.max_number_of_open_positions)
            st.number_input(
                label="Max Open Positions", 
                min_value=1, 
                step=1, 
                key="max_number_of_open_positions", 
                on_change=on_max, 
                # width=200,
                )

            # Min size & Stop loss
            # c3, c4 = st.columns(2)
            if "min_position_size" not in st.session_state:
                st.session_state.min_position_size = float(config.read_setting("min_position_size"))
            def on_min():
                MIN_USD = 20
                ta = st.session_state.trade_against
                if ta in ["USDC","USDT"]:
                    st.session_state.min_position_size = max(float(st.session_state.min_position_size), MIN_USD)
                else:
                    if float(st.session_state.min_position_size) >= MIN_USD:
                        st.session_state.min_position_size = 0.0001
                config.update_setting("min_position_size", st.session_state.min_position_size)
            min_kwargs = dict(min_value=20.0, step=10.0, format=None) if st.session_state.trade_against in ["USDC","USDT"] else dict(min_value=0.0001, step=0.0001, format="%.4f")
            st.number_input(
                label="Minimum Position Size", 
                key="min_position_size", 
                on_change=on_min,
                # width=200, 
                **min_kwargs)
            
            if "stop_loss" not in st.session_state:
                st.session_state.stop_loss = config.read_setting("stop_loss")
            def on_sl(): config.update_setting("stop_loss", st.session_state.stop_loss)
            st.number_input(
                label="Stop Loss %", 
                min_value=0, 
                step=1, 
                key="stop_loss", 
                on_change=on_sl, 
                # width=200,
                )

        # Top performers & Tradable ratio
        with st.container(horizontal=False):
            if "trade_top_performance" not in st.session_state:
                st.session_state.trade_top_performance = config.read_setting("trade_top_performance")
            st.slider(
                label="Trade Top Performance Symbols", 
                min_value=0, max_value=500, step=5,
                key="trade_top_performance",
                on_change=lambda: config.update_setting("trade_top_performance", st.session_state.trade_top_performance),
                # width=400
            )
            
            if "tradable_balance_ratio" not in st.session_state:
                st.session_state.tradable_balance_ratio = config.read_setting("tradable_balance_ratio")*100
            st.slider(
                label="Tradable Balance Ratio", 
                min_value=0, 
                max_value=100, 
                step=1, 
                format="%d%%", key="tradable_balance_ratio",
                on_change=lambda: config.update_setting("tradable_balance_ratio", st.session_state.tradable_balance_ratio/100),
                # width=400
            )

        st.space()

        st.markdown("### Take-Profit Levels")
        with st.container(border=False):
            def _tp_num(label, key_perc, key_amt, rps_value, rps_key):
                st.session_state[rps_key] = rps_value
                with st.container(horizontal=True):
                    st.number_input(
                        f"{label} (%)",
                        min_value=0,
                        step=1,
                        key=key_perc,
                        on_change=lambda k=key_perc: config.update_setting(k, st.session_state[k]),
                        help="The percentage increase in price at which the system will automatically trigger a sell order to secure profits.",
                        width=130,
                    )
                    st.number_input(
                        f"{label} Amount (%)",
                        min_value=5,
                        max_value=100,
                        step=5,
                        key=key_amt,
                        on_change=lambda k=key_amt: config.update_setting(k, st.session_state[k]),
                        help="The percentage of the position quantity to be sold when take profits level is achieved.",
                        width=130,
                    )
                    st.number_input(
                        f"RPS {label[-1]} (%)",
                        value=rps_value,
                        step=0.01,
                        disabled=True,
                        key=rps_key,
                        help="Remaining position size after selling TP Amount% of the remainder.",
                        width=80,
                    )

            # Ensure TP state is initialized before first RPS calculation.
            for key in [
                "take_profit_1",
                "take_profit_2",
                "take_profit_3",
                "take_profit_4",
                "take_profit_1_amount",
                "take_profit_2_amount",
                "take_profit_3_amount",
                "take_profit_4_amount",
            ]:
                if key not in st.session_state:
                    st.session_state[key] = config.read_setting(key)

            a1 = float(st.session_state.get("take_profit_1_amount", 0) or 0)
            a2 = float(st.session_state.get("take_profit_2_amount", 0) or 0)
            a3 = float(st.session_state.get("take_profit_3_amount", 0) or 0)
            a4 = float(st.session_state.get("take_profit_4_amount", 0) or 0)
            def rps_seq(amounts):
                r=100.0; out=[]
                for a in amounts: r*= (1-a/100.0); out.append(round(r,2))
                return out
            rps1,rps2,rps3,rps4 = rps_seq([a1,a2,a3,a4])
            _tp_num("TP1", "take_profit_1", "take_profit_1_amount", rps1, "rps1_widget")
            _tp_num("TP2", "take_profit_2", "take_profit_2_amount", rps2, "rps2_widget")
            _tp_num("TP3", "take_profit_3", "take_profit_3_amount", rps3, "rps3_widget")
            _tp_num("TP4", "take_profit_4", "take_profit_4_amount", rps4, "rps4_widget")

        st.space()

        st.markdown("### Locked Values")
        with st.container(border=False):
            if "lock_values" not in st.session_state:
                st.session_state.lock_values = config.read_setting("lock_values")
            st.checkbox("Lock values from partial sales", key="lock_values",
                        on_change=lambda: (database.release_all_values() if not st.session_state.lock_values else None,
                                        config.update_setting("lock_values", st.session_state.lock_values)),
                        help="""When **enabled**, means that any amount obtained from partially selling a position will be temporarily locked and cannot be used to purchase another position until the entire position is sold. 
                            \nWhen **disabled**, partial sales can be freely reinvested into new positions. It's important to note that this may increase the risk of larger position amounts, as funds from partial sales may be immediately reinvested without reservation.
                        """)
            if st.session_state.lock_values:
                st.caption("Note that disabling this option will **release all** locked values.")

            # with st.expander(label="Current Locked Values", expanded=True):
            df = database.get_all_locked_values()
            df_sel = df.copy()
            df_sel.insert(0, "Select", False)
            edited = st.data_editor(df_sel, hide_index=True,
                                    column_config={"Select": st.column_config.CheckboxColumn(), "Id": None},
                                    disabled=["Bot","Symbol","Locked_Amount","Locked_At"])
            picked = edited[edited.Select]
            if st.button("Unlock Selected", disabled=picked.empty):
                for _id in picked.Id.to_list():
                    if _id and _id > 0:
                        database.release_locked_value_by_id(_id)
                st.rerun()

        st.space()
        
        with st.container(border=False):
            st.markdown("### Telegram")
            if "bot_prefix" not in st.session_state:
                st.session_state.bot_prefix = config.read_setting("bot_prefix")
            st.text_input(
                label="Telegram Messages Prefix",
                key="bot_prefix",
                on_change=lambda: config.update_setting("bot_prefix", st.session_state.bot_prefix),
                width=200,
                help="When there are multiple instances of BEC running, the prefix is useful to distinguish which BEC the telegram message belongs to.")


def show_main_page():
    
    global trade_against
    trade_against = config.read_setting("trade_against") 

    global num_decimals
    num_decimals = 8 if trade_against == "BTC" else 2
    
    global tab_upnl, tab_rpnl, tab_top_perf, tab_signals, tab_blacklist, tab_settings
    tab_upnl, tab_rpnl, tab_signals, tab_top_perf, tab_blacklist, tab_settings = st.tabs(["Unrealized PnL", "Realized PnL", "Signals", "Top Performers", "Blacklist", "Settings"])

    realized_pnl()
    unrealized_pnl()
    signals()
    top_performers()

    with tab_blacklist:
        blacklist()
    
    settings()

# Get years from orders
def get_years():
    years = database.get_years_from_orders()
    return years

# get months with orders within the year
def get_orders_by_month(year: str):

    months = database.get_months_from_orders_by_year(year)

    month_dict = {}
    for month in months:
        month_name = calendar.month_name[month]
        month_dict[month] = month_name
    return month_dict
    
def calculate_realized_pnl(year: str, month: str):
    """
    Aggregates realized PnL by bot (1d, 4h, 1h) and TOTAL.
    Uses WEIGHTED PnL% per bot and TOTAL:
        weight = Sell_Position_Value = (sell_qty * sell_price)

    Args:
        year:  'YYYY' or None (if None, returns empty totals)
        month: '01'..'12' or '13' for 'all months of the given year'

    Returns:
        results_df: summary table with weighted PnL% per bot and TOTAL
        df_1d, df_4h, df_1h: detailed DataFrames for each bot
    """

    # Load SELL-side order details per bot (includes Sell_Position_Value)
    df_1d = database.get_orders_by_bot_side_year_month(bot="1d", side="SELL", year=year, month=str(month))
    df_4h = database.get_orders_by_bot_side_year_month(bot="4h", side="SELL", year=year, month=str(month))
    df_1h = database.get_orders_by_bot_side_year_month(bot="1h", side="SELL", year=year, month=str(month))

    # Prepare a helper to compute weighted PnL% for a given dataframe
    def _weighted_pnl_perc(df: pd.DataFrame) -> float:
        if df.empty:
            return 0.0
        # Ensure numeric types for weighting (avoid any stray strings from previous formatting)
        df_num = df.copy()
        df_num['Sell_Position_Value'] = pd.to_numeric(df_num.get('Sell_Position_Value', 0.0), errors='coerce').fillna(0.0)
        df_num['PnL_Perc'] = pd.to_numeric(df_num.get('PnL_Perc', 0.0), errors='coerce').fillna(0.0)
        denom = df_num['Sell_Position_Value'].sum()
        if denom == 0:
            return 0.0
        return float((df_num['PnL_Perc'] * df_num['Sell_Position_Value']).sum() / denom)
    
    # Build results per bot with WEIGHTED PnL%
    results_df = pd.DataFrame()
    for label, df_bot in [('1d', df_1d), ('4h', df_4h), ('1h', df_1h)]:
        if df_bot.empty:
            bot_weighted = 0.0
            pnl_value_sum = 0.0
            trades = 0
        else:
            bot_weighted = _weighted_pnl_perc(df_bot)
            pnl_value_sum = float(pd.to_numeric(df_bot['PnL_Value'], errors='coerce').fillna(0.0).sum())
            trades = len(df_bot)

        results_df = pd.concat([
            results_df,
            pd.DataFrame({
                'Bot': [label],
                'PnL_Perc': [bot_weighted],     # Weighted PnL% by Sell_Position_Value
                'PnL_Value': [pnl_value_sum],   # Sum of realized PnL value
                'Positions': [trades]           # Number of SELL trades in period
            })
        ], ignore_index=True)

    # TOTAL row: WEIGHTED across ALL bots (weights = Sell_Position_Value)
    all_frames = [df for df in (df_1d, df_4h, df_1h) if not df.empty]
    df_all = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    if df_all.empty:
        weighted_total = 0.0
        sum_pnl_value_total = 0.0
        trades_total = 0
    else:
        weighted_total = _weighted_pnl_perc(df_all)
        sum_pnl_value_total = float(pd.to_numeric(df_all['PnL_Value'], errors='coerce').fillna(0.0).sum())
        trades_total = len(df_all)

    # Append TOTAL row
    results_df.loc[len(results_df)] = ['TOTAL', weighted_total, sum_pnl_value_total, trades_total]

    # ---------- Display formatting ----------
    # Format summary numbers for display (keep numeric precision in detail tables below)
    results_df['PnL_Perc'] = results_df['PnL_Perc'].apply(lambda x: '{:.2f}'.format(float(x)))
    results_df['PnL_Value'] = results_df['PnL_Value'].apply(lambda x: f'{{:.{num_decimals}f}}'.format(float(x)))

    # Detail tables: keep high precision where relevant (8 decimals)
    def _fmt_detail(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        # Keep 8-decimal formatting for price/value columns (for readability in UI tables)
        if 'Buy_Price' in df.columns:
            df['Buy_Price'] = df['Buy_Price'].apply(lambda x: f'{float(x):.8f}')
        if 'Sell_Price' in df.columns:
            df['Sell_Price'] = df['Sell_Price'].apply(lambda x: f'{float(x):.8f}')
        if 'Sell_Position_Value' in df.columns:
            df['Sell_Position_Value'] = df['Sell_Position_Value'].apply(lambda x: f'{float(x):.8f}')
        if 'Buy_Position_Value' in df.columns:
            df['Buy_Position_Value'] = df['Buy_Position_Value'].apply(lambda x: f'{float(x):.8f}')
        # Percent and value formatting
        if 'PnL_Perc' in df.columns:
            df['PnL_Perc'] = df['PnL_Perc'].apply(lambda x: '{:.2f}'.format(float(x)))
        if 'PnL_Value' in df.columns:
            df['PnL_Value'] = df['PnL_Value'].apply(lambda x: f'{{:.{num_decimals}f}}'.format(float(x)))
        return df

    df_1d = _fmt_detail(df_1d)
    df_4h = _fmt_detail(df_4h)
    df_1h = _fmt_detail(df_1h)

    return results_df, df_1d, df_4h, df_1h

def calculate_unrealized_pnl():
    
    # Load positions by bot
    df_positions_1d = database.get_unrealized_pnl_by_bot(bot="1d")
    df_positions_4h = database.get_unrealized_pnl_by_bot(bot="4h")
    df_positions_1h = database.get_unrealized_pnl_by_bot(bot="1h")

    # Build results per bot using WEIGHTED PnL% (weights = Position_Value)
    results_df = pd.DataFrame()
    for timeframe, df_positions in [('1d', df_positions_1d), ('4h', df_positions_4h), ('1h', df_positions_1h)]:
        # Ensure numeric types for weighting (avoid any stray strings)
        if not df_positions.empty:
            df_bot = df_positions.copy()
            df_bot['Position_Value'] = pd.to_numeric(df_bot['Position_Value'], errors='coerce').fillna(0.0)
            df_bot['PnL_Perc'] = pd.to_numeric(df_bot['PnL_Perc'], errors='coerce').fillna(0.0)
            # Weighted PnL% for this bot
            bot_pos_val_sum = df_bot['Position_Value'].sum()
            if bot_pos_val_sum != 0:
                bot_weighted_pnl_perc = (df_bot['PnL_Perc'] * df_bot['Position_Value']).sum() / bot_pos_val_sum
            else:
                bot_weighted_pnl_perc = 0.0
            # Sum of PnL value and count of open positions for this bot
            pnl_value_sum = df_bot['PnL_Value'].sum()
            positions = len(df_bot)
        else:
            bot_weighted_pnl_perc = 0.0
            pnl_value_sum = 0.0
            positions = 0

        df_new = pd.DataFrame({
            'Bot': [timeframe],
            'PnL_Perc': [bot_weighted_pnl_perc],   # weighted per-bot PnL%
            'PnL_Value': [pnl_value_sum],
            'Positions': [positions]
        })
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # ---------- TOTAL: weighted PnL% across ALL positions ----------
    df_all = pd.concat([df_positions_1d, df_positions_4h, df_positions_1h], ignore_index=True)
    if not df_all.empty:
        df_all['Position_Value'] = pd.to_numeric(df_all['Position_Value'], errors='coerce').fillna(0.0)
        df_all['PnL_Perc'] = pd.to_numeric(df_all['PnL_Perc'], errors='coerce').fillna(0.0)

        total_position_value = df_all['Position_Value'].sum()
        if total_position_value != 0:
            weighted_pnl_perc_total = (df_all['PnL_Perc'] * df_all['Position_Value']).sum() / total_position_value
        else:
            weighted_pnl_perc_total = 0.0

        sum_pnl_value_total = results_df['PnL_Value'].sum()
        open_positions = len(df_all)
    else:
        weighted_pnl_perc_total = 0.0
        sum_pnl_value_total = 0.0
        open_positions = 0

    # Show "open / max" positions
    max_num_positions = config.read_setting("max_number_of_open_positions")
    positions_info_total = f"{open_positions}/{max_num_positions}"

    # Append TOTAL row (weighted)
    results_df.loc[len(results_df)] = ['TOTAL', weighted_pnl_perc_total, sum_pnl_value_total, positions_info_total]

    # ---------- Display formatting ----------
    # Format PnL% and PnL_Value for results table
    # format the pnl_perc with 2 decimal places
    results_df['PnL_Perc'] = results_df['PnL_Perc'].apply(lambda x: '{:.2f}'.format(float(x)))
    # format the pnl_value decimal places depending on trade against
    results_df['PnL_Value'] = results_df['PnL_Value'].apply(lambda x: f'{{:.{num_decimals}f}}'.format(float(x)))

    # Format detail DataFrames
    for df_ref in (df_positions_1d, df_positions_4h, df_positions_1h):
        if not df_ref.empty:
            df_ref['Buy_Price'] = df_ref['Buy_Price'].apply(lambda x: f'{{:.{8}f}}'.format(x))
            df_ref['Position_Value'] = df_ref['Position_Value'].apply(lambda x: f'{{:.{8}f}}'.format(x))
            df_ref['PnL_Perc'] = df_ref['PnL_Perc'].apply(lambda x: '{:.2f}'.format(float(x)))
            df_ref['PnL_Value'] = df_ref['PnL_Value'].apply(lambda x: f'{{:.{num_decimals}f}}'.format(float(x)))
    
    return results_df, df_positions_1d, df_positions_4h, df_positions_1h

# define a function to set the background color of the rows based on pnl_value
def set_pnl_color(val):
    if val is not None:
        val = float(val)
        color = '#E9967A' if val < 0 else '#8FBC8F' if val > 0 else ''
        return f'background-color: {color}'

def format_func_strategies_main(option):
        return dict_strategies_main[option]

def format_func_strategies_btc(option):
        return dict_strategies_btc[option]

def format_func_strategies(option):
        return dict_strategies[option]

def main():

    st.title(f'Trading Dashboard')
    # st.write(f"You are logged in as {st.session_state.role}.")

    # get strategies
    df_strategies_main = database.get_strategies_for_main()
    df_strategies_btc = database.get_strategies_for_btc()
    df_strategies = database.get_all_strategies()
    global dict_strategies_main, dict_strategies_btc, dict_strategies
    #create a dictionary with code and name columns
    dict_strategies_main = dict(zip(df_strategies_main['Id'], df_strategies_main['Name']))
    dict_strategies_btc = dict(zip(df_strategies_btc['Id'], df_strategies_btc['Name']))
    dict_strategies = dict(zip(df_strategies['Id'], df_strategies['Name']))

    show_main_page()
    
if __name__ in ("__main__", "__page__"):
    main()
