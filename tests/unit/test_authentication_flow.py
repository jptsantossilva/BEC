from types import SimpleNamespace

import pandas as pd
import pytest

import bec.dashboard as dashboard
from bec.page_config import configure_page


class SessionState(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        self[name] = value


class FakeLoginForm:
    def __init__(self, username="", password="", submitted=False):
        self.username = username
        self.password = password
        self.submitted = submitted
        self.subheaders = []
        self.inputs = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def subheader(self, text):
        self.subheaders.append(text)

    def text_input(self, label, **kwargs):
        self.inputs.append((label, kwargs))
        if label == "Username":
            return self.username
        if label == "Password":
            return self.password
        return ""

    def form_submit_button(self, label):
        return self.submitted


class FakeStreamlit:
    def __init__(self, form=None, clicked_buttons=None):
        self.session_state = SessionState()
        self.form_instance = form or FakeLoginForm()
        self.clicked_buttons = set(clicked_buttons or [])
        self.buttons = []
        self.titles = []
        self.writes = []
        self.errors = []
        self.infos = []
        self.rerun_called = False
        self.stop_called = False
        self.switch_page_target = None
        self.page_config = None
        self.markdown_calls = []

    def form(self, key):
        self.form_key = key
        return self.form_instance

    def subheader(self, text):
        self.form_instance.subheader(text)

    def text_input(self, label, **kwargs):
        return self.form_instance.text_input(label, **kwargs)

    def form_submit_button(self, label):
        return self.form_instance.form_submit_button(label)

    def title(self, message):
        self.titles.append(str(message))

    def write(self, message, **kwargs):
        self.writes.append((str(message), kwargs))

    def container(self, **kwargs):
        return self

    def button(self, label, **kwargs):
        self.buttons.append((label, kwargs))
        key = kwargs.get("key")
        return key in self.clicked_buttons or label in self.clicked_buttons

    def error(self, message):
        self.errors.append(str(message))

    def info(self, message):
        self.infos.append(str(message))

    def rerun(self):
        self.rerun_called = True

    def stop(self):
        self.stop_called = True

    def switch_page(self, target):
        self.switch_page_target = target

    def set_page_config(self, **kwargs):
        self.page_config = kwargs

    def markdown(self, body, **kwargs):
        self.markdown_calls.append((body, kwargs))


class FakeAuthenticationController:
    def __init__(self, result=True, exc=None):
        self.result = result
        self.exc = exc
        self.calls = []
        self.logout_calls = 0

    def login(self, username, password):
        self.calls.append((username, password))
        if self.exc:
            raise self.exc
        return self.result

    def logout(self):
        self.logout_calls += 1


class FakeCookieController:
    def __init__(self):
        self.set_calls = 0
        self.delete_calls = 0
        self.cookie_model = SimpleNamespace(cookie_manager=FakeCookieManager())

    def set_cookie(self):
        self.set_calls += 1

    def delete_cookie(self):
        self.delete_calls += 1


class FakeCookieManager:
    def __init__(self):
        self.delete_calls = []

    def delete(self, cookie, key=None):
        self.delete_calls.append((cookie, key))


def _authenticator(result=True, exc=None):
    return SimpleNamespace(
        authentication_controller=FakeAuthenticationController(result=result, exc=exc),
        cookie_controller=FakeCookieController(),
    )


def test_format_authenticator_credentials_normalizes_usernames():
    df_users = pd.DataFrame(
        [
            {
                "username": "Admin",
                "email": "admin@example.com",
                "name": "Admin",
                "password": "hashed",
            }
        ]
    ).set_index("username")

    credentials = dashboard.format_authenticator_credentials(df_users)

    assert list(credentials["usernames"]) == ["admin"]
    assert credentials["usernames"]["admin"]["email"] == "admin@example.com"
    assert credentials["usernames"]["admin"]["password"] == "hashed"


def test_render_login_form_submits_credentials_and_sets_cookie(monkeypatch):
    fake_st = FakeStreamlit(FakeLoginForm("Admin", "secret", submitted=True))
    fake_authenticator = _authenticator(result=True)
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)

    dashboard.render_login_form()

    assert fake_authenticator.authentication_controller.calls == [("Admin", "secret")]
    assert fake_authenticator.cookie_controller.set_calls == 1
    assert fake_st.session_state.redirect_to_dashboard_after_login is True
    assert fake_st.rerun_called is True
    assert fake_st.form_key == "bec_login_form"
    assert fake_st.form_instance.inputs[0][1]["key"] == "bec_login_username"
    assert fake_st.form_instance.inputs[1][1]["key"] == "bec_login_password"


def test_render_login_form_requires_username_and_password(monkeypatch):
    fake_st = FakeStreamlit(FakeLoginForm("admin", "", submitted=True))
    fake_authenticator = _authenticator(result=True)
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)

    dashboard.render_login_form()

    assert fake_st.session_state.authentication_status is None
    assert fake_authenticator.authentication_controller.calls == []
    assert fake_authenticator.cookie_controller.set_calls == 0
    assert fake_st.rerun_called is False


def test_render_login_form_marks_failed_authentication(monkeypatch):
    fake_st = FakeStreamlit(FakeLoginForm("admin", "wrong", submitted=True))
    fake_authenticator = _authenticator(result=False)
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)

    dashboard.render_login_form()

    assert fake_authenticator.authentication_controller.calls == [("admin", "wrong")]
    assert fake_st.session_state.authentication_status is False
    assert fake_authenticator.cookie_controller.set_calls == 0
    assert fake_st.rerun_called is False


def test_clear_authentication_cookie_uses_current_cookie_controller(monkeypatch):
    fake_authenticator = _authenticator()
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)

    dashboard.clear_authentication_cookie()

    cookie_manager = fake_authenticator.cookie_controller.cookie_model.cookie_manager
    assert cookie_manager.delete_calls == [
        (dashboard.COOKIE_NAME, "delete_dashboard_auth_cookie")
    ]


def test_rotate_dashboard_cookie_key_persists_new_signing_key(monkeypatch):
    original_cookie_key = dashboard.COOKIE_KEY
    original_signing_key = dashboard.COOKIE_SIGNING_KEY
    saved_settings = []
    monkeypatch.setattr(dashboard.database, "set_setting", lambda name, value: saved_settings.append((name, value)))

    try:
        dashboard.rotate_dashboard_cookie_key()

        assert saved_settings == [("dashboard_cookie_key", dashboard.COOKIE_KEY)]
        assert dashboard.COOKIE_KEY != original_cookie_key
        assert dashboard.COOKIE_SIGNING_KEY != original_signing_key
    finally:
        dashboard.COOKIE_KEY = original_cookie_key
        dashboard.COOKIE_SIGNING_KEY = original_signing_key


def test_logout_clears_auth_state_cookie_and_returns_to_dashboard(monkeypatch):
    fake_st = FakeStreamlit()
    rotated_keys = []
    fake_st.session_state.update(
        {
            "authentication_status": True,
            "username": "admin",
            "name": "Admin",
            "role": "Trading",
        }
    )
    fake_authenticator = _authenticator()
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)
    monkeypatch.setattr(dashboard.database, "set_setting", lambda name, value: rotated_keys.append((name, value)))

    dashboard.logout()

    assert fake_authenticator.authentication_controller.logout_calls == 1
    assert rotated_keys
    assert rotated_keys[0][0] == "dashboard_cookie_key"
    assert rotated_keys[0][1] == dashboard.COOKIE_KEY
    assert fake_authenticator.cookie_controller.delete_calls == 0
    cookie_manager = fake_authenticator.cookie_controller.cookie_model.cookie_manager
    assert cookie_manager.delete_calls == [
        (dashboard.COOKIE_NAME, "delete_dashboard_auth_cookie")
    ]
    assert fake_st.session_state["logout"] is True
    assert fake_st.session_state["logout_complete"] is True
    assert fake_st.session_state["authentication_status"] is None
    assert fake_st.session_state["username"] is None
    assert fake_st.session_state["name"] is None
    assert fake_st.session_state["role"] is None
    assert fake_st.rerun_called is True


def test_logout_still_expires_browser_cookie_when_authenticator_logout_fails(monkeypatch):
    fake_st = FakeStreamlit()
    rotated_keys = []
    fake_st.session_state.update(
        {
            "authentication_status": True,
            "username": "admin",
            "name": "Admin",
            "role": "Trading",
        }
    )
    fake_authenticator = _authenticator()
    fake_authenticator.authentication_controller.logout = lambda: (_ for _ in ()).throw(
        RuntimeError("logout failed")
    )
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)
    monkeypatch.setattr(dashboard.database, "set_setting", lambda name, value: rotated_keys.append((name, value)))

    dashboard.logout()

    assert rotated_keys
    assert fake_authenticator.cookie_controller.delete_calls == 0
    cookie_manager = fake_authenticator.cookie_controller.cookie_model.cookie_manager
    assert cookie_manager.delete_calls == [
        (dashboard.COOKIE_NAME, "delete_dashboard_auth_cookie")
    ]
    assert fake_st.session_state["authentication_status"] is None
    assert fake_st.rerun_called is True


def test_logout_page_view_asks_for_confirmation_without_logging_out(monkeypatch):
    fake_st = FakeStreamlit()
    fake_authenticator = _authenticator()
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)

    dashboard.logout_page_view()

    assert fake_st.titles == ["Log out"]
    assert fake_st.writes == [("Are you sure you want to log out?", {})]
    assert fake_st.buttons == [
        ("Confirm", {"icon": ":material/check:", "key": "confirm_logout"}),
        ("Cancel", {"icon": ":material/cancel:", "key": "cancel_logout"}),
    ]
    assert fake_authenticator.authentication_controller.logout_calls == 0
    assert fake_st.stop_called is False


def test_logout_page_view_redirects_after_completed_logout(monkeypatch):
    fake_st = FakeStreamlit()
    fake_st.session_state["logout_complete"] = True
    fake_authenticator = _authenticator()
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)

    dashboard.logout_page_view()

    assert fake_st.switch_page_target == "dashboard.py"
    assert fake_authenticator.authentication_controller.logout_calls == 0


def test_logout_page_view_cancel_returns_to_trading_without_logging_out(monkeypatch):
    fake_st = FakeStreamlit(clicked_buttons={"cancel_logout"})
    fake_authenticator = _authenticator()
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)

    dashboard.logout_page_view()

    assert fake_st.switch_page_target == "pages/trading.py"
    assert fake_authenticator.authentication_controller.logout_calls == 0
    assert fake_st.stop_called is False


def test_logout_page_view_confirm_logs_out(monkeypatch):
    fake_st = FakeStreamlit(clicked_buttons={"confirm_logout"})
    fake_authenticator = _authenticator()
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)
    monkeypatch.setattr(dashboard.database, "set_setting", lambda name, value: None)

    dashboard.logout_page_view()

    assert fake_st.infos == ["Logging out..."]
    assert fake_authenticator.authentication_controller.logout_calls == 1
    assert fake_st.session_state["logout"] is True
    assert fake_st.session_state["logout_complete"] is True
    assert fake_st.rerun_called is True


def test_restore_login_from_cookie_clears_invalid_cookie(monkeypatch):
    fake_st = FakeStreamlit()
    fake_authenticator = _authenticator()
    fake_authenticator.login = lambda location: (_ for _ in ()).throw(
        dashboard.LoginError("User not authorized")
    )
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "authenticator", fake_authenticator)

    dashboard.restore_login_from_cookie()

    cookie_manager = fake_authenticator.cookie_controller.cookie_model.cookie_manager
    assert cookie_manager.delete_calls == [
        (dashboard.COOKIE_NAME, "delete_dashboard_auth_cookie")
    ]
    assert fake_st.session_state["authentication_status"] is None
    assert fake_st.session_state["username"] is None
    assert fake_st.session_state["name"] is None
    assert fake_st.session_state["role"] is None
    assert fake_st.infos == [
        "Your previous session became invalid after the username change. Please log in again."
    ]


def test_configure_page_uses_wide_layout(monkeypatch):
    fake_st = FakeStreamlit()
    monkeypatch.setattr("bec.page_config.st", fake_st)

    configure_page()

    assert fake_st.page_config["layout"] == "wide"
    assert fake_st.page_config["page_title"] == "BEC App"
    assert any(
        ".block-container" in body
        and '[data-testid="stMainBlockContainer"]' in body
        and "max-width: none !important" in body
        and "width: 100% !important" in body
        for body, _ in fake_st.markdown_calls
    )
    assert all(kwargs.get("unsafe_allow_html") is True for _, kwargs in fake_st.markdown_calls)
