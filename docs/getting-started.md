# Getting Started

## What This Page Is For

Use this page to install BEC with Docker and open the dashboard for the first time.

## Requirements

Recommended setup:

- Ubuntu server or Ubuntu desktop;
- Docker Engine;
- Docker Compose plugin;
- Binance API key and secret;
- Telegram bot tokens, optional but recommended;
- OpenAI API key, optional and only needed for AI analysis.

You do not need to install Python or Python packages manually when using Docker. The BEC Docker image includes Python 3.12 and all required Python dependencies.

Other operating systems may work if they support Docker, but the recommended and tested setup is Ubuntu with Docker Compose.

## Recommended Installation: Ubuntu + Docker

Install Docker on Ubuntu by following the official Docker instructions:

[Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)

After Docker is installed, create a working folder and download the default BEC files:


```bash
mkdir -p /opt/bec
cd /opt/bec
curl -fsSL https://raw.githubusercontent.com/jptsantossilva/BEC/main/docker-compose.yml -o docker-compose.yml
curl -fsSL https://raw.githubusercontent.com/jptsantossilva/BEC/main/.env.example -o .env
```

Edit `.env` and add your credentials:

```ini
# Binance API
binance_api="..."
binance_secret="..."

# Telegram
telegram_chat_id="..."
telegram_token_closed_positions="..."
telegram_token_errors="..."
telegram_token_main="..."
telegram_token_signals="..."

# OpenAI
OPENAI_API_KEY="..."
BEC_OPENAI_MODEL="gpt-5.5"
```

OpenAI is only needed for AI analysis in Backtesting Results. If you do not use AI analysis, leave `OPENAI_API_KEY` empty and keep the default `BEC_OPENAI_MODEL`.

The SQLite web password is configured in `docker-compose.yml` with `SQLITE_WEB_PASSWORD`. Change the default value before exposing SQLite web to any network.

Start BEC:

```bash
sudo docker compose pull
sudo docker compose up -d
```

## Open The Apps

- Dashboard: `http://localhost:8080`
- SQLite web: `http://localhost:8081`

On a fresh install, use the default admin login shown in the project release notes or change it immediately after first login.

## First Checks

After login:

1. Open **Trading > Dashboard** and confirm the app loads.
2. Open **Trading > Scheduled Jobs** and check which jobs are enabled.
3. Open **Backtesting > Backtest Settings** and review cash, commission, risk, and approval rules.
4. Check container logs if data or jobs are not updating.

```bash
docker compose logs -f dashboard
docker compose logs -f jobs_runner
```
