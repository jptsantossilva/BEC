
import os
import time
import hashlib
from urllib.parse import urlparse

import streamlit as st
from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx


import streamlit_authenticator as stauth

from streamlit_authenticator.utilities.exceptions import LoginError

import bec.update as update
import bec.utils.config as config
import bec.utils.database as database
from bec.utils.env_loader import load_env_file
import bec.utils.general as general
import bec.utils.telegram as telegram
from bec.page_config import configure_page

load_env_file(override=True)

configure_page()

st.logo("static/bec-logo.svg", size="large", icon_image=":material/currency_bitcoin:")

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
if "logout" not in st.session_state:
    st.session_state.logout = False

ROLES = [None, "Trading", "Market Analysis", "Admin"]

# constants (put near the top, once)
COOKIE_NAME = os.getenv("BEC_DASHBOARD_COOKIE_NAME", "dashboard_cookie_name")
COOKIE_KEY = database.get_or_create_secret_setting(
    "dashboard_cookie_key",
    length=48,
    comment="Auto-generated secret used to sign dashboard login cookies.",
)
COOKIE_SIGNING_KEY = hashlib.sha256(COOKIE_KEY.encode("utf-8")).hexdigest()


def logout():

    # now clear cookie and force a clean login
    try:
        authenticator.logout(location="unrendered")
    except Exception:
        pass
    st.session_state["logout"] = True
    for key in ("authentication_status", "username", "name", "role"):
        st.session_state[key] = None
    st.rerun()


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


def requested_page_path_from_url():
    ctx = get_script_run_ctx()
    if not ctx or not ctx.context_info:
        return ""

    parsed_url = urlparse(ctx.context_info.url or "")
    path = parsed_url.path.strip("/")
    if not path:
        return ""

    return path.rsplit("/", 1)[-1]


def restore_requested_page_from_url(pages_by_url_path, default_page):
    ctx = get_script_run_ctx()
    if not ctx:
        return

    requested_path = requested_page_path_from_url()
    requested_page = pages_by_url_path.get(requested_path)
    if not requested_page:
        return

    intended_hash = ctx.pages_manager.intended_page_script_hash
    intended_name = ctx.pages_manager.intended_page_name
    fallback_hashes = {"", ctx.pages_manager.main_script_hash}

    if intended_hash and intended_hash not in fallback_hashes:
        return

    if intended_name and intended_name not in {"", default_page.url_path}:
        return

    # On a hard browser refresh, Streamlit can rerun the app before the frontend
    # sends the selected page hash. Preserve the URL-selected page instead of
    # falling back to the default dashboard.
    ctx.pages_manager.set_script_intent(
        requested_page._script_hash,
        requested_path,
    )


def check_app_version():
    last_date = general.extract_date_from_local_changelog()
    if last_date:
        app_version = last_date
    else:
        app_version = "App version not found"
    st.sidebar.page_link(
        "https://jptsantossilva.github.io/BEC/",
        label="User Manual",
        icon=":material/menu_book:",
    )
    st.sidebar.markdown(
        f"""
        <div style="line-height:1.15; margin-top:0.15rem;">
            <div style="font-size:0.82rem; color:rgba(49, 51, 63, 0.6);">BEC - {trade_against}</div>
            <div style="font-size:0.78rem; color:rgba(49, 51, 63, 0.6); margin-top:0.08rem;">Version {app_version}</div>
        </div>
        """,
        unsafe_allow_html=True,
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

    def _default_dockge_url() -> str:
        try:
            headers = dict(st.context.headers)
            host = headers.get("X-Forwarded-Host") or headers.get("Host")
            proto = headers.get("X-Forwarded-Proto") or "http"
            if host:
                host = host.split(",")[0].strip()
                hostname = host.split(":")[0]
                return f"{proto}://{hostname}:5001"
        except Exception:
            pass
        return "http://localhost:5001"

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
                    with st.container(border=True):
                        st.markdown("**Update via Docker**")
                        st.write("Update is managed via Docker, run this command in your server terminal:")
                        st.code(
                            "docker compose pull && docker compose up -d",
                            language="bash",
                        )
                        st.link_button(
                            "Open Dockge",
                            _default_dockge_url(),
                            icon=":material/open_in_new:",
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
    user_page = st.Page(
        "pages/user.py",
        title="User",
        icon=":material/person:",
        url_path="user",
    )

    trading = st.Page(
        "pages/trading.py",
        title="Dashboard",
        icon=":material/currency_bitcoin:",
        url_path="trading",
        default=True,
    )

    balances = st.Page(
        "pages/balances.py",
        title="Balances",
        icon=":material/account_balance_wallet:",
        url_path="balances",
        default=False,
    )

    backtesting_settings = st.Page(
        "pages/backtesting_settings.py",
        title="Backtest Settings",
        icon=":material/candlestick_chart:",
        url_path="backtesting_settings",
        default=False,
    )

    backtesting_results = st.Page(
        "pages/backtesting_results.py",
        title="Backtesting Results",
        icon=":material/query_stats:",
        url_path="backtesting_results",
        default=False,
    )

    monte_carlo_analysis = st.Page(
        "pages/monte_carlo_analysis.py",
        title="Monte Carlo Analysis",
        icon=":material/ssid_chart:",
        url_path="monte_carlo_analysis",
        default=False,
    )

    scheduled_jobs = st.Page(
        "pages/scheduled_jobs.py",
        title="Scheduled Jobs",
        icon=":material/pending_actions:",
        url_path="scheduled_jobs",
        default=False,
    )

    bull_market_indicators_dashboard = st.Page(
        "pages/bull_market_indicators_dashboard.py",
        title="Dashboard",
        icon=":material/finance_mode:",
        url_path="bull_market_indicators_dashboard",
        default=False,
    )

    pages_by_url_path = {
        "user": user_page,
        "trading": trading,
        "balances": balances,
        "scheduled_jobs": scheduled_jobs,
        "backtesting_settings": backtesting_settings,
        "backtesting_results": backtesting_results,
        "monte_carlo_analysis": monte_carlo_analysis,
        "bull_market_indicators_dashboard": bull_market_indicators_dashboard,
    }
    restore_requested_page_from_url(pages_by_url_path, trading)

    # --- Account pages ---
    account_pages = []
    if st.session_state.authentication_status:
        account_pages.extend([user_page, logout_page])

    # --- Role-based pages ---
    trading_pages = [trading, balances, scheduled_jobs]
    backtesting_pages = [backtesting_settings, backtesting_results, monte_carlo_analysis]
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
        pg.run()
    else:
        pg = st.navigation(
            [
                user_page,
                logout_page,
                trading,
                balances,
                scheduled_jobs,
                backtesting_settings,
                backtesting_results,
                monte_carlo_analysis,
                bull_market_indicators_dashboard,
            ],
            position="hidden",
        )
        unauthenticated_landing()


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
        cookie_key=COOKIE_SIGNING_KEY,
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
        for key in ("authentication_status", "username", "name", "role"):
            st.session_state[key] = None
        st.session_state["logout"] = False
        st.rerun()

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
