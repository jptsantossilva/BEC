from types import SimpleNamespace

import pytest
import requests

from bec.utils import telegram


class _SuccessfulResponse:
    def raise_for_status(self):
        return None


def test_shared_request_helper_forwards_supported_payloads_and_timeout(monkeypatch):
    calls = []
    response = _SuccessfulResponse()
    files = {"document": object()}

    monkeypatch.setattr(
        telegram.requests,
        "post",
        lambda url, **kwargs: calls.append((url, kwargs)) or response,
    )
    monkeypatch.setattr(telegram, "telegram_timeout", 7)

    result = telegram._send_telegram_request(
        "https://api.telegram.org/bottest-token/sendDocument",
        operation="test_document",
        params={"query": "value"},
        data={"chat_id": "chat"},
        files=files,
    )

    assert result is response
    assert calls == [
        (
            "https://api.telegram.org/bottest-token/sendDocument",
            {
                "params": {"query": "value"},
                "data": {"chat_id": "chat"},
                "files": files,
                "timeout": 7,
            },
        )
    ]


@pytest.mark.parametrize(
    ("error", "category"),
    [
        (
            requests.exceptions.HTTPError(
                "server rejected https://api.telegram.org/botsecret-token/sendMessage",
                response=SimpleNamespace(status_code=429),
            ),
            "HTTP error: HTTPError, status=429",
        ),
        (
            requests.exceptions.ConnectionError(
                "failed https://api.telegram.org/botsecret-token/sendMessage"
            ),
            "Connection error: ConnectionError",
        ),
        (
            requests.exceptions.Timeout(
                "timed out https://api.telegram.org/botsecret-token/sendMessage"
            ),
            "Timeout: Timeout",
        ),
        (
            requests.exceptions.RequestException(
                "failed https://api.telegram.org/botsecret-token/sendMessage"
            ),
            "Request error: RequestException",
        ),
    ],
)
def test_shared_request_helper_handles_and_safely_logs_request_errors(
    monkeypatch, capsys, error, category
):
    logged = []

    if isinstance(error, requests.exceptions.HTTPError):
        class _HttpFailureResponse:
            def raise_for_status(self):
                raise error

        monkeypatch.setattr(
            telegram.requests,
            "post",
            lambda *args, **kwargs: _HttpFailureResponse(),
        )
    else:
        def fail_request(*args, **kwargs):
            raise error

        monkeypatch.setattr(telegram.requests, "post", fail_request)

    monkeypatch.setattr(
        telegram.logging,
        "error",
        lambda message: logged.append(message),
    )

    result = telegram._send_telegram_request(
        "https://api.telegram.org/botsecret-token/sendMessage",
        operation="send_test_message",
        params={"chat_id": "chat"},
    )

    assert result is None
    assert category in capsys.readouterr().out
    assert category in logged[0]
    assert "send_test_message" in logged[0]
    assert "secret-token" not in logged[0]


def test_warning_delivery_continues_when_errors_destination_fails(monkeypatch):
    calls = []
    logged = []

    def post(url, params=None, timeout=None):
        calls.append(url)
        if "boterrors-token/" in url:
            raise requests.exceptions.Timeout("errors destination unavailable")
        return _SuccessfulResponse()

    monkeypatch.setattr(telegram.requests, "post", post)
    monkeypatch.setattr(telegram.logging, "error", logged.append)
    monkeypatch.setattr(telegram.database, "get_active_exchange_display_name", lambda: "Kraken")
    monkeypatch.setattr(telegram, "telegram_chat_id", "chat")
    monkeypatch.setattr(telegram, "telegram_token_errors", "errors-token")
    monkeypatch.setattr(telegram, "bot_prefix", "BEC")

    telegram.send_telegram_message(
        "main-token",
        telegram.EMOJI_WARNING,
        "warning",
    )

    assert calls == [
        "https://api.telegram.org/boterrors-token/sendMessage",
        "https://api.telegram.org/botmain-token/sendMessage",
    ]
    assert "send_telegram_message.errors" in logged[0]


def test_long_message_to_errors_token_is_sent_in_all_parts(monkeypatch):
    calls = []

    monkeypatch.setattr(
        telegram.requests,
        "post",
        lambda url, params=None, timeout=None: (
            calls.append((url, params, timeout)) or _SuccessfulResponse()
        ),
    )
    monkeypatch.setattr(telegram, "telegram_chat_id", "chat")
    monkeypatch.setattr(telegram, "telegram_token_errors", "errors-token")

    telegram.send_telegram_message(
        telegram.telegram_token_errors,
        None,
        "x" * 5000,
        include_prefix=False,
    )

    assert len(calls) == 2
    assert all("boterrors-token/sendMessage" in url for url, _params, _timeout in calls)
    assert all("Part [" in params["text"] for _url, params, _timeout in calls)


@pytest.mark.parametrize(
    ("sender_name", "field_name", "file_name", "expected_data"),
    [
        ("send_telegram_photo", "photo", "image.png", None),
        (
            "send_telegram_file",
            "document",
            "report.txt",
            {"chat_id": "chat"},
        ),
    ],
)
def test_media_senders_close_files_after_request(
    monkeypatch,
    tmp_path,
    sender_name,
    field_name,
    file_name,
    expected_data,
):
    captured = {}
    (tmp_path / file_name).write_bytes(b"content")

    def post(url, **kwargs):
        captured["file"] = kwargs["files"][field_name]
        captured["data"] = kwargs.get("data")
        assert captured["file"].closed is False
        return _SuccessfulResponse()

    monkeypatch.setattr(telegram.requests, "post", post)
    monkeypatch.setattr(telegram.os, "getcwd", lambda: str(tmp_path))
    monkeypatch.setattr(telegram, "telegram_chat_id", "chat")

    getattr(telegram, sender_name)("token", file_name)

    assert captured["file"].closed is True
    assert captured["data"] == expected_data
