from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"
REQUIRED_PASSWORD_EXPRESSION = (
    "${SQLITE_WEB_PASSWORD:?SQLITE_WEB_PASSWORD must be set in .env}"
)


def test_sqlite_web_requires_external_password_and_has_no_weak_default():
    compose_text = COMPOSE_PATH.read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_text)
    sqlite_web = compose["services"]["sqlite_web"]
    former_weak_default = "_".join(("change", "this", "password"))

    assert former_weak_default not in compose_text
    assert sqlite_web["environment"]["SQLITE_WEB_PASSWORD"] == (
        REQUIRED_PASSWORD_EXPRESSION
    )
    assert 'SQLITE_WEB_PASSWORD=""' in ENV_EXAMPLE_PATH.read_text(encoding="utf-8")


def test_sqlite_web_is_published_only_on_host_loopback():
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    sqlite_web = compose["services"]["sqlite_web"]

    assert sqlite_web["ports"] == ["127.0.0.1:8081:8081"]
    assert "--host" in sqlite_web["command"]
    host_index = sqlite_web["command"].index("--host")
    assert sqlite_web["command"][host_index + 1] == "0.0.0.0"
