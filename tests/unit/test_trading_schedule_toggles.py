import importlib


trading = importlib.import_module("pages.trading")


class FakeStreamlit:
    def __init__(self, session_state=None):
        self.session_state = dict(session_state or {})
        self.toggles = []
        self.errors = []

    def toggle(self, label, *, key, on_change, disabled, help):
        self.toggles.append(
            {
                "label": label,
                "key": key,
                "value": self.session_state[key],
                "on_change": on_change,
                "disabled": disabled,
                "help": help,
            }
        )

    def error(self, message):
        self.errors.append(str(message))


def _schedule_state(values, schedule_name):
    return values[schedule_name]


def test_trade_schedule_toggles_replace_stale_and_missing_session_values(monkeypatch):
    values = {"main_1d": True, "main_4h": False, "main_1h": True}
    fake_st = FakeStreamlit(
        {
            "_job_toggle_exchange": "kraken",
            "job_main_1d_enabled": False,
            "job_main_4h_enabled": True,
        }
    )
    monkeypatch.setattr(trading, "st", fake_st)
    monkeypatch.setattr(
        trading.database,
        "get_job_schedule_enabled",
        lambda name: _schedule_state(values, name),
    )

    trading._render_trade_schedule_toggles()

    assert "_job_toggle_exchange" not in fake_st.session_state
    assert {
        toggle["key"]: toggle["value"]
        for toggle in fake_st.toggles
    } == {
        "job_main_1d_enabled": True,
        "job_main_4h_enabled": False,
        "job_main_1h_enabled": True,
    }


def test_trade_schedule_toggles_refresh_database_values_on_every_render(monkeypatch):
    values = {"main_1d": False, "main_4h": False, "main_1h": False}
    fake_st = FakeStreamlit()
    monkeypatch.setattr(trading, "st", fake_st)
    monkeypatch.setattr(
        trading.database,
        "get_job_schedule_enabled",
        lambda name: _schedule_state(values, name),
    )

    trading._render_trade_schedule_toggles()
    values.update({"main_1d": True, "main_4h": True, "main_1h": True})
    trading._render_trade_schedule_toggles()

    assert [toggle["value"] for toggle in fake_st.toggles[-3:]] == [
        True,
        True,
        True,
    ]


def test_trade_schedule_toggle_callback_updates_the_matching_schedule(monkeypatch):
    values = {"main_1d": False, "main_4h": False, "main_1h": False}
    updates = []
    fake_st = FakeStreamlit()
    monkeypatch.setattr(trading, "st", fake_st)
    monkeypatch.setattr(
        trading.database,
        "get_job_schedule_enabled",
        lambda name: _schedule_state(values, name),
    )

    def set_enabled(name, enabled):
        updates.append((name, enabled))
        values[name] = enabled

    monkeypatch.setattr(trading.database, "set_job_schedule_enabled", set_enabled)

    trading._render_trade_schedule_toggles()
    toggle = next(
        item for item in fake_st.toggles if item["key"] == "job_main_4h_enabled"
    )
    fake_st.session_state[toggle["key"]] = True
    toggle["on_change"]()

    assert updates == [("main_4h", True)]
    assert values["main_4h"] is True


def test_rejected_trade_schedule_change_restores_database_value(monkeypatch):
    values = {"main_1d": False, "main_4h": True, "main_1h": True}
    fake_st = FakeStreamlit()
    monkeypatch.setattr(trading, "st", fake_st)
    monkeypatch.setattr(
        trading.database,
        "get_job_schedule_enabled",
        lambda name: _schedule_state(values, name),
    )

    def reject_change(name, enabled):
        raise ValueError(f"{name} cannot be enabled")

    monkeypatch.setattr(trading.database, "set_job_schedule_enabled", reject_change)

    trading._render_trade_schedule_toggles()
    toggle = next(
        item for item in fake_st.toggles if item["key"] == "job_main_1d_enabled"
    )
    fake_st.session_state[toggle["key"]] = True
    toggle["on_change"]()

    assert fake_st.session_state["job_main_1d_enabled"] is False
    assert fake_st.errors == ["main_1d cannot be enabled"]
