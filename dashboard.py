
import os
import time

import streamlit as st


import streamlit_authenticator as stauth

from streamlit_authenticator.utilities.exceptions import LoginError

import update
import utils.config as config
import utils.database as database
import utils.general as general
import utils.telegram as telegram

st.set_page_config(
    page_title="BEC App",
    page_icon="random",
    layout="wide",
    # initial_sidebar_state="auto",
    menu_items={
        "Get Help": "https://github.com/jptsantossilva/BEC#readme",
        "Report a bug": "https://github.com/jptsantossilva/BEC/issues/new",
        "About": """# My name is BEC \n I am a Trading Bot and I'm trying to be an *extremely* cool app! 
        \n This is my dad's 🐦 Twitter: [@jptsantossilva](https://twitter.com/jptsantossilva).
        """,
    },
)

# --- Top-of-page banner placeholder (must be above any other UI rendering)
TOP_NOTICE = st.empty()

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
    st.session_state.authentication_status = None
if "role" not in st.session_state:
    st.session_state.role = None

ROLES = [None, "Trading", "Market Analysis", "Admin"]

# constants (put near the top, once)
COOKIE_NAME = "dashboard_cookie_name"
COOKIE_KEY = "dashboard_cookie_key"


def logout():

    # now clear cookie and force a clean login
    try:
        authenticator.logout(location="unrendered")
    except Exception:
        pass
    # st.session_state["authentication_status"] = None
    # st.rerun()


def logout_page_view():
    st.title("Log out")
    st.write("Click confirm to end your session.")

    cols = st.columns((2), width=300)
    with cols[0]:
        if st.button("Confirm", icon=":material/check:"):
            logout()
    with cols[1]:
        if st.button("Cancel", icon=":material/cancel:"):
            st.switch_page("pages/trading.py")  # navigate to Trading page


# 👇 coloca isto acima de set_pages()
def unauthenticated_landing():
    st.empty()


def check_app_version():
    last_date = general.extract_date_from_local_changelog()
    if last_date:
        app_version = last_date
    else:
        app_version = "App version not found"
    st.sidebar.caption(
        f"**BEC** - {trade_against} - Version {app_version}"
    )

    def _running_in_docker() -> bool:
        if os.path.exists("/.dockerenv"):
            return True
        try:
            with open("/proc/1/cgroup", "r", encoding="utf-8") as f:
                data = f.read()
            return "docker" in data or "containerd" in data
        except Exception:
            return False

    github_last_date = general.extract_date_from_github_changelog()
    if github_last_date != last_date:
        # Render the banner at the very top, using the global placeholder
        global TOP_NOTICE
        with TOP_NOTICE.container():
            st.warning(
                "🚀 New version available! Click **UPDATE** to get the latest features. "
                "See the [Change Log](https://github.com/jptsantossilva/BEC/blob/main/CHANGELOG.md) for details."
            )
            update_version = st.button(
                "Update", key="update_version", icon=":material/deployed_code_update:"
            )
            if update_version:
                if _running_in_docker():
                    st.info(
                        "Update is managed via Docker. Run:\n"
                        "`docker compose pull && docker compose up -d`"
                    )
                else:
                    with st.spinner(
                        "🎉 Hold on tight! 🎉 Our elves are sprinkling magic dust on the app to make it even better."
                    ):
                        result = update.main()
                        st.code(result)

                        restart_time = 10
                        progress_text = f"App will restart in {restart_time} seconds."
                        my_bar = st.progress(0, text=progress_text)

                        for step in range(restart_time + 1):
                            progress_percent = step * 10
                            if progress_percent != 0:
                                restart_time -= 1
                                progress_text = (
                                    f"App will restart in {restart_time} seconds."
                                )
                            my_bar.progress(progress_percent, text=progress_text)
                            time.sleep(1)

                    # st.rerun()


def set_pages():
    role = st.session_state.role

    logout_page = st.Page(logout_page_view, title="Log out", icon=":material/logout:")
    user_page = st.Page("pages/user.py", title="User", icon=":material/person:")

    trading = st.Page(
        "pages/trading.py",
        title="Dashboard",
        icon=":material/currency_bitcoin:",
        default=(role == "Trading"),
    )

    balances = st.Page(
        "pages/balances.py",
        title="Balances",
        icon=":material/account_balance_wallet:",
        default=False,
    )

    backtesting_settings = st.Page(
        "pages/backtesting_settings.py",
        title="Backtest Settings",
        icon=":material/candlestick_chart:",
        default=False,
    )

    backtesting_results = st.Page(
        "pages/backtesting_results.py",
        title="Backtesting Results",
        icon=":material/query_stats:",
        default=False,
    )

    scheduled_jobs = st.Page(
        "pages/scheduled_jobs.py",
        title="Scheduled Jobs",
        icon=":material/pending_actions:",
        default=False,
    )

    bull_market_indicators_dashboard = st.Page(
        "pages/bull_market_indicators_dashboard.py",
        title="Dashboard",
        icon=":material/finance_mode:",
        default=(role == "Market Analysis"),
    )

    # --- Account pages ---
    account_pages = []
    if st.session_state.authentication_status:
        account_pages.extend([user_page, logout_page])

    # --- Role-based pages ---
    trading_pages = [trading, balances, scheduled_jobs]
    backtesting_pages = [backtesting_settings, backtesting_results]
    # market_analysis_pages = [bull_market_indicators_dashboard]
    page_dict = {}
    if st.session_state.authentication_status:
        if role in ["Trading", "Admin"]:
            page_dict["Trading"] = trading_pages
            page_dict["Backtesting"] = backtesting_pages
        # if role in ["Market Analysis", "Trading", "Admin"]:
        #     page_dict["Bull Market Indicators"] = market_analysis_pages

    if page_dict or account_pages:
        pg = st.navigation({"Account": account_pages} | page_dict)
    else:
        pg = st.navigation(
            [st.Page(unauthenticated_landing, title="Welcome", icon=":material/lock:")]
        )

    pg.run()


def show_main_page():

    # st.session_state

    settings = config.load_settings()

    global trade_against
    trade_against = settings.trade_against

    global num_decimals
    num_decimals = 8 if trade_against == "BTC" else 2
    # num_decimals = 2

    check_app_version()


def forgot_password():
    try:
        username_forgot_pw, email_forgot_password, random_password = (
            authenticator.forgot_password("Forgot password")
        )
        if username_forgot_pw:
            st.success("New password sent securely")
            # Random password to be transferred to user securely
        elif username_forgot_pw == False:
            st.error("Username not found")
    except Exception as e:
        st.error(e)


def render_forgot_password_widget():
    """
    Displays the 'Forgot password' widget and, if successful,
    saves the new password (hashed) in the database.
    """
    try:
        username_fp, email_fp, new_plain_pw = authenticator.forgot_password(
            location="main",
            fields={
                "Form name": "Forgot password",
                "Username": "Username",
                "Captcha": "Captcha",
                "Submit": "Generate new password",
            },
            captcha=True,  # enable captcha to avoid abuse
            send_email=False,  # set to True if you have SMTP configured (see step 8 of the library)
            clear_on_submit=True,
            key="forgot_pw_after_login",
        )

        if username_fp:
            # Hash the new password
            new_pw_hashed = stauth.Hasher.hash(new_plain_pw)
            # Persist in the database
            database.update_user_password(username=username_fp, password=new_pw_hashed)

            st.success(
                "A new password has been generated and sent to telegram Signals channel."
            )
            msg = f"User '{username_fp}' just reset their password using the 'Forgot password' feature.\nNew password in the next message."
            msg = telegram.telegram_prefix_signals_sl + msg
            telegram.send_telegram_message(
                telegram.telegram_token_signals, telegram.EMOJI_PASSWORD_RESET, msg
            )

            telegram.send_password_only(telegram.telegram_token_signals, new_plain_pw)

            st.info("Please log in again with your new password.")

        elif username_fp is False:
            st.error("Username not found.")
    except Exception as e:
        st.error(e)


def set_authentication():
    df_users = database.get_all_users()
    # Convert the DataFrame to a dictionary
    credentials = df_users.to_dict("index")
    formatted_credentials = {"usernames": {}}
    # Iterate over the keys and values of the original `credentials` dictionary
    for username, user_info in credentials.items():
        # Add each username and its corresponding user info to the `formatted_credentials` dictionary
        formatted_credentials["usernames"][username] = user_info

    st.session_state.setdefault("credentials", formatted_credentials)

    global authenticator

    authenticator = stauth.Authenticate(
        credentials=st.session_state["credentials"],
        cookie_name=COOKIE_NAME,
        key=COOKIE_KEY,
        cookie_expiry_days=30,
    )

    st.session_state["authenticator"] = authenticator

    # If we just logged out, remove the cookie *now*, before login() reads it
    if st.session_state.get("logout"):
        try:
            authenticator.cookie_manager.delete(COOKIE_NAME)
            authenticator.cookie_manager.delete(COOKIE_NAME, path="/")  # extra safety
        except Exception:
            pass
        st.session_state["logout"] = False

    # Centered login column for better UX
    left_col, center_col, right_col = st.columns([1, 1, 1])
    with center_col:
        # Put the heading BEFORE the login widget
        if st.session_state.authentication_status in [False, None]:
            st.title("BEC Trading")

        # --- LOGIN with recovery for "User not authorized" ---
        try:
            authenticator.login()
        except LoginError:
            # Clear old cookies/token that might still reference the previous username
            try:
                authenticator.cookie_manager.delete(COOKIE_NAME)
                authenticator.cookie_manager.delete(COOKIE_NAME, path="/")
            except Exception:
                pass

            # Remove old authentication state from session
            for k in ("authentication_status", "username", "name", "role"):
                st.session_state.pop(k, None)

            # Notify user and force re-login
            st.info(
                "Your previous session became invalid after the username change. Please log in again."
            )
            authenticator.login()

        if st.session_state.authentication_status == False:
            st.error("Username or password is incorrect")
        elif st.session_state.authentication_status == None:
            st.info("Please enter your username and password")

        # If not authenticated, show forgot password widget
        if st.session_state.authentication_status in [False, None]:
            st.write("<br>", unsafe_allow_html=True)
            with st.expander("Having trouble logging in?"):
                render_forgot_password_widget()

    # Set role after a successful login
    if st.session_state.authentication_status is True:
        st.session_state.role = "Trading"  # or only set if not already set:


def main():
    authentication_status = st.session_state.authentication_status

    if authentication_status:
        show_main_page()


if __name__ == "__main__":
    set_authentication()
    set_pages()
    main()
