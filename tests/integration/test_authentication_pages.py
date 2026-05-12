from streamlit.testing.v1 import AppTest


def test_login_page_renders_without_authentication_errors():
    app = AppTest.from_file("dashboard.py", default_timeout=10)

    app.run()

    assert not app.exception
    assert [title.value for title in app.title] == ["BEC Trading"]
    assert "Login" in [subheader.value for subheader in app.subheader]
    assert "Please enter your username and password" in [info.value for info in app.info]

    login_inputs = {
        text_input.key: text_input.label
        for text_input in app.text_input
        if text_input.key in {"bec_login_username", "bec_login_password"}
    }
    assert login_inputs == {
        "bec_login_username": "Username",
        "bec_login_password": "Password",
    }

    login_buttons = [button for button in app.button if button.label == "Login"]
    assert len(login_buttons) == 1
