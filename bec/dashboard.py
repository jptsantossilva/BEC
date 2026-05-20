
import os
import time
import hashlib
import secrets
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

load_env_file(override=True)

st.logo("static/bec-logo.svg", size="large", icon_image=":material/currency_bitcoin:")

# --- Top-of-page banner placeholder (must be above any other UI rendering)
TOP_NOTICE = st.empty()

# for testing purposes
# st.session_state

def initialize_session_state():
    defaults = {
        "name": "",
        "username": "",
        "user_password": "None",
        "reset_form_open": False,
        "reset_password_submitted": False,
        "authentication_status": None,
        "role": None,
        "logout": False,
        "redirect_to_dashboard_after_login": False,
        "show_docker_update_instructions": False,
        "run_local_update": False,
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


initialize_session_state()

ROLES = [None, "Trading", "Market Analysis", "Admin"]

# constants (put near the top, once)
COOKIE_NAME = os.getenv("BEC_DASHBOARD_COOKIE_NAME", "dashboard_cookie_name")
COOKIE_KEY = database.get_or_create_secret_setting(
    "dashboard_cookie_key",
    length=48,
    comment="Auto-generated secret used to sign dashboard login cookies.",
)
COOKIE_SIGNING_KEY = hashlib.sha256(COOKIE_KEY.encode("utf-8")).hexdigest()
authenticator = None


def rotate_dashboard_cookie_key():
    global COOKIE_KEY, COOKIE_SIGNING_KEY

    COOKIE_KEY = secrets.token_urlsafe(48)
    database.set_setting("dashboard_cookie_key", COOKIE_KEY)
    COOKIE_SIGNING_KEY = hashlib.sha256(COOKIE_KEY.encode("utf-8")).hexdigest()


def logout():
    try:
        authenticator.authentication_controller.logout()
    except Exception:
        pass
    rotate_dashboard_cookie_key()
    st.session_state["logout"] = True
    st.session_state["logout_complete"] = True
    for key in ("authentication_status", "username", "name", "role"):
        st.session_state[key] = None
    clear_authentication_cookie()
    st.rerun()


def logout_page_view():
    if st.session_state.get("logout_complete"):
        st.switch_page("dashboard.py")

    st.title("Log out")
    st.write("Are you sure you want to log out?")

    actions = st.container(horizontal=True)
    if actions.button("Confirm", icon=":material/check:", key="confirm_logout"):
        st.info("Logging out...")
        logout()
    if actions.button("Cancel", icon=":material/cancel:", key="cancel_logout"):
        st.switch_page("pages/trading.py")


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
        """
        <div style="margin-top:0.7rem; margin-bottom:0.45rem;">
            <a
                href="https://ko-fi.com/C0C3TDKPG"
                target="_blank"
                rel="noopener noreferrer"
                style="
                    display:inline-flex;
                    align-items:center;
                    gap:0.45rem;
                    padding:0.36rem 0.58rem;
                    border:1px solid rgba(49, 51, 63, 0.16);
                    border-radius:0.45rem;
                    background:rgba(255, 255, 255, 0.45);
                    color:rgb(49, 51, 63);
                    text-decoration:none;
                    font-size:0.86rem;
                    font-weight:500;
                    line-height:1.1;
                "
            >
                <svg width="20" height="20" viewBox="0 0 241 194" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false" style="flex:0 0 auto;">
                    <path d="M96.1344 193.911C61.1312 193.911 32.6597 178.256 15.9721 149.829C1.19788 124.912 -0.00585938 97.9229 -0.00585938 67.7662C-0.00585938 49.8876 5.37293 34.3215 15.5413 22.7466C24.8861 12.1157 38.1271 5.22907 52.8317 3.35378C70.2858 1.14271 91.9848 0.958984 114.545 0.958984C151.259 0.958984 161.63 1.4088 176.075 2.85328C195.29 4.76026 211.458 11.932 222.824 23.5955C234.368 35.4428 240.469 51.2624 240.469 69.3627V72.9994C240.469 103.885 219.821 129.733 191.046 136.759C188.898 141.827 186.237 146.871 183.089 151.837L183.006 151.964C172.869 167.632 149.042 193.918 103.401 193.918H96.1281L96.1344 193.911Z" fill="white"/>
                    <path d="M15.1975 67.7674C15.1975 37.5285 33.3866 21.164 54.7559 18.4334C70.8987 16.387 90.906 16.1589 114.544 16.1589C151.372 16.1589 160.919 16.6151 174.559 17.9772C206.617 21.1576 225.255 40.937 225.255 69.3577V72.9941C225.255 99.3687 205.932 120.966 179.786 123.234C177.74 130.058 174.559 136.874 170.238 143.698C160.235 159.156 140.228 178.707 103.4 178.707H96.1264C66.1155 178.707 42.9277 165.751 29.0595 142.107C16.7814 121.422 15.1912 98.4563 15.1912 67.7674" fill="#202020"/>
                    <path d="M32.2469 67.9899C32.2469 97.3168 34.0654 116.184 43.6127 133.689C54.5225 153.924 74.3018 161.653 96.8117 161.653H103.857C133.411 161.653 147.736 147.329 155.693 134.829C159.558 128.462 162.966 121.417 164.784 112.547L166.147 106.864H174.332C192.521 106.864 208.208 92.09 208.208 73.2166V69.8082C208.208 48.6669 195.024 37.5228 172.058 34.7987C159.102 33.6646 151.372 33.2084 114.538 33.2084C89.7602 33.2084 72.0272 33.4364 58.6152 35.4828C39.7483 38.2134 32.2407 48.8951 32.2407 67.9899" fill="white"/>
                    <path d="M166.158 83.6801C166.158 86.4107 168.204 88.4572 171.841 88.4572C183.435 88.4572 189.802 81.8619 189.802 70.9523C189.802 60.0427 183.435 53.2195 171.841 53.2195C168.204 53.2195 166.158 55.2657 166.158 57.9963V83.6866V83.6801Z" fill="#202020"/>
                    <path d="M54.5321 82.3198C54.5321 95.732 62.0332 107.326 71.5807 116.424C77.9478 122.562 87.9515 128.93 94.7685 133.022C96.8147 134.157 98.8611 134.841 101.136 134.841C103.866 134.841 106.134 134.157 107.959 133.022C114.782 128.93 124.779 122.562 130.919 116.424C140.694 107.332 148.195 95.7383 148.195 82.3198C148.195 67.7673 137.286 54.8115 121.599 54.8115C112.28 54.8115 105.912 59.5882 101.136 66.1772C96.8147 59.582 90.2259 54.8115 80.9001 54.8115C64.9855 54.8115 54.5256 67.7673 54.5256 82.3198" fill="#FF5A16"/>
                </svg>
                <span>Buy me a coffee</span>
            </a>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    st.sidebar.markdown(
        f"""
        <br>
        <div style="line-height:1.15; margin-top:0.15rem;">
            <div style="font-size:0.82rem; color:rgba(49, 51, 63, 0.6);">BEC - {trade_against}</div>
            <div style="font-size:0.78rem; color:rgba(49, 51, 63, 0.6); margin-top:0.08rem;">Version {app_version}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    def _running_in_docker() -> bool:
        if os.getenv("BEC_UPDATE_MODE", "").lower() == "docker":
            return True
        if os.path.exists("/.dockerenv"):
            return True
        if os.path.exists("/app/docker/entrypoint.sh") and os.path.exists("/app/persist"):
            return True
        try:
            with open("/proc/1/cgroup", "r", encoding="utf-8") as f:
                data = f.read()
                data = "docker"
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

    def _show_docker_update_panel() -> None:
        with st.container(border=True):
            st.markdown("**Update via Docker**")
            st.write("Update is managed via Docker, run this command in your server terminal:")
            st.code(
                "docker compose pull && docker compose up -d",
                language="bash",
            )
            st.link_button(
                "Open Dockge",
                os.getenv("BEC_DOCKGE_URL") or _default_dockge_url(),
                icon=":material/open_in_new:",
            )

    def _docker_update_requested() -> bool:
        return (
            bool(st.session_state.show_docker_update_instructions)
            or st.query_params.get("docker_update") == "1"
        )

    def _request_app_update() -> None:
        if _running_in_docker():
            st.session_state.show_docker_update_instructions = True
            st.query_params["docker_update"] = "1"
        else:
            st.session_state.run_local_update = True

    github_last_date = general.extract_date_from_github_changelog()
    if github_last_date != last_date:
        # Render the banner at the very top, using the global placeholder
        global TOP_NOTICE
        with TOP_NOTICE.container():
            st.warning(
                "🚀 New version available! Click **UPDATE** to get the latest features. "
                "See the [Change Log](https://github.com/jptsantossilva/BEC/blob/main/CHANGELOG.md) for details."
            )
            st.button(
                "Update",
                key="update_version",
                icon=":material/deployed_code_update:",
                on_click=_request_app_update,
            )

            if st.session_state.pop("run_local_update", False):
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

            if _docker_update_requested():
                _show_docker_update_panel()
    else:
        st.session_state.show_docker_update_instructions = False
        if st.query_params.get("docker_update"):
            del st.query_params["docker_update"]


def set_pages():
    initialize_session_state()
    role = st.session_state.role

    logout_page = st.Page(
        logout_page_view,
        title="Log out",
        icon=":material/logout:",
        url_path="logout",
    )
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

    strategy_builder = st.Page(
        "pages/strategy_builder.py",
        title="Strategy Builder",
        icon=":material/account_tree:",
        url_path="strategy_builder",
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
        "logout": logout_page,
        "user": user_page,
        "trading": trading,
        "balances": balances,
        "scheduled_jobs": scheduled_jobs,
        "strategy_builder": strategy_builder,
        "backtesting_settings": backtesting_settings,
        "backtesting_results": backtesting_results,
        "monte_carlo_analysis": monte_carlo_analysis,
        "bull_market_indicators_dashboard": bull_market_indicators_dashboard,
    }
    if st.session_state.pop("redirect_to_dashboard_after_login", False):
        st.switch_page("pages/trading.py")

    restore_requested_page_from_url(pages_by_url_path, trading)

    # --- Account pages ---
    account_pages = []
    if st.session_state.authentication_status:
        account_pages.extend([user_page, logout_page])

    # --- Role-based pages ---
    trading_pages = [trading, balances, scheduled_jobs]
    strategy_pages = [strategy_builder]
    backtesting_pages = [backtesting_settings, backtesting_results, monte_carlo_analysis]
    # market_analysis_pages = [bull_market_indicators_dashboard]
    page_dict = {}
    if st.session_state.authentication_status:
        if role in ["Trading", "Admin"]:
            page_dict["Trading"] = trading_pages
            page_dict["Strategy"] = strategy_pages
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
                strategy_builder,
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


def format_authenticator_credentials(df_users):
    credentials = df_users.to_dict("index")
    formatted_credentials = {"usernames": {}}
    for username, user_info in credentials.items():
        formatted_credentials["usernames"][str(username).lower()] = dict(user_info)
    return formatted_credentials


def clear_authentication_cookie():
    if authenticator is None:
        return

    cookie_controller = getattr(authenticator, "cookie_controller", None)
    if cookie_controller:
        cookie_model = getattr(cookie_controller, "cookie_model", None)
        cookie_manager = getattr(cookie_model, "cookie_manager", None)
        if cookie_manager:
            try:
                cookie_manager.delete(COOKIE_NAME, key="delete_dashboard_auth_cookie")
            except KeyError:
                pass
            return
        try:
            cookie_controller.delete_cookie()
        except KeyError:
            pass
        return

    cookie_manager = getattr(authenticator, "cookie_manager", None)
    if cookie_manager:
        cookie_manager.delete(COOKIE_NAME)
        cookie_manager.delete(COOKIE_NAME, path="/")


def restore_login_from_cookie():
    try:
        authenticator.login(location="unrendered")
    except LoginError:
        clear_authentication_cookie()
        for key in ("authentication_status", "username", "name", "role"):
            st.session_state[key] = None
        st.info(
            "Your previous session became invalid after the username change. Please log in again."
        )


def render_login_form():
    with st.form(key="bec_login_form"):
        st.subheader("Login")
        username = st.text_input(
            "Username",
            key="bec_login_username",
            autocomplete="username",
        )
        password = st.text_input(
            "Password",
            type="password",
            key="bec_login_password",
            autocomplete="current-password",
        )
        submitted = st.form_submit_button("Login")

    if not submitted:
        return

    if not username or not password:
        st.session_state.authentication_status = None
        return

    try:
        authenticated = authenticator.authentication_controller.login(username, password)
    except LoginError as e:
        st.session_state.authentication_status = False
        st.error(e)
        return

    if authenticated:
        st.session_state.logout = False
        st.session_state.logout_complete = False
        st.session_state.redirect_to_dashboard_after_login = True
        authenticator.cookie_controller.set_cookie()
        st.rerun()
    else:
        st.session_state.authentication_status = False


def set_authentication():
    initialize_session_state()

    df_users = database.get_all_users()
    formatted_credentials = format_authenticator_credentials(df_users)
    st.session_state["credentials"] = formatted_credentials

    global authenticator

    authenticator = stauth.Authenticate(
        credentials=st.session_state["credentials"],
        cookie_name=COOKIE_NAME,
        cookie_key=COOKIE_SIGNING_KEY,
        cookie_expiry_days=30,
    )

    st.session_state["authenticator"] = authenticator

    if st.session_state.get("logout"):
        for key in ("authentication_status", "username", "name", "role"):
            st.session_state[key] = None
    else:
        restore_login_from_cookie()

    # Centered login column for better UX
    left_col, center_col, right_col = st.columns([1, 1, 1])
    with center_col:
        # Put the heading BEFORE the login widget
        if st.session_state.authentication_status in [False, None]:
            st.title("BEC Trading")
            render_login_form()

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
    initialize_session_state()
    authentication_status = st.session_state.authentication_status

    if authentication_status:
        show_main_page()


if __name__ == "__main__":
    set_authentication()
    set_pages()
    main()
