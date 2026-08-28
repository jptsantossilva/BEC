# BEC

BEC is a free, open-source, self-hosted spot trading platform for Binance,
Kraken, and OKX. It combines automated strategy execution, exchange-scoped
backtesting, market-phase ranking, risk controls, Streamlit dashboards, and
Telegram notifications in a single application.

> [!IMPORTANT]
> BEC supports multiple exchange integrations, but operates with exactly one
> app-wide active exchange at a time. It does not trade simultaneously across
> exchanges.

[User Manual](https://jptsantossilva.github.io/BEC/) ·
[Changelog](https://github.com/jptsantossilva/BEC/blob/main/CHANGELOG.md) ·
[License](https://github.com/jptsantossilva/BEC/blob/main/LICENSE.md)

## Supported exchanges

| Exchange | Market data and backtesting | Spot order execution |
|---|---|---|
| Binance | Supported | Native Binance workflow |
| Kraken | Supported | Explicitly gated live workflow with durable order intents and reconciliation |
| OKX | Supported | Explicitly gated live workflow with durable order intents and reconciliation |

Exchange data, symbols, fees, rankings, backtests, positions, orders, and
balances are kept isolated. Switching the active exchange is blocked while
open positions or unsettled orders exist.

BEC supports spot markets only. Futures, margin, withdrawals, transfers,
staking, lending, arbitrage, and simultaneous multi-exchange trading are not
supported.

## Highlights

- Automated strategy execution on 1-day, 4-hour, and 1-hour timeframes.
- Direct spot-market discovery for the selected quote asset, including USDC,
  USDT, and BTC where supported by the active exchange.
- Daily market-phase ranking to select assets in accumulation or bullish
  conditions.
- Exchange-scoped backtesting, approval rules, Strategy Quality Score, and
  Monte Carlo robustness analysis.
- Backtesting reports with charts, statistics, configuration, trades, HTML
  download, PDF export, and optional AI analysis.
- Risk controls including hard stops, ATR trailing stops, configurable
  take-profit levels, position sizing, and exit-reason tracking.
- Durable order intents and reconciliation for supported gated live workflows,
  protecting against duplicate submissions after uncertain exchange responses.
- Automatic scheduling for trading cycles, market-phase rebuilds, signals, and
  order reconciliation.
- Streamlit dashboard for PnL, balances, positions, strategies, backtests,
  market analysis, schedules, diagnostics, and exchange settings.
- Telegram notifications for trades, execution summaries, signals, and
  operational errors.
- Base-asset blacklist management across native and canonical exchange symbol
  formats.
- BTC Supply Profit/Loss macro/on-chain indicator with local SQLite caching,
  configurable thresholds, and optional Telegram alerts.

Market analysis signals are informational only. They do not create orders or
change position sizing, stops, take profits, symbol selection, backtesting, or
strategy logic.

## Dashboard

![BEC dashboard](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/dashboard.png)

Backtesting report example for BTC/USDC on the 1-day timeframe:

![BEC backtesting report](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/backtesting-report.png)

[Open the interactive backtesting report](https://jptsantossilva.github.io/BEC/BTCUSDC-ema_cross_with_market_phases-1d.html).

## Quick start with Docker

### Requirements

- A Linux host with Docker Engine and Docker Compose.
- API credentials only if you intend to use private balance checks or order
  execution.
- A strong password for the bundled SQLite administration interface.
- Telegram credentials if notifications are required.
- An OpenAI API key only if AI backtesting analysis is required.

Never enable withdrawal permissions on an exchange API key. Use only the
minimum read/trade permissions required by BEC and configure an IP allowlist
where the exchange supports one.

### Install

```bash
mkdir -p /opt/bec
cd /opt/bec

curl -fsSL https://raw.githubusercontent.com/jptsantossilva/BEC/main/docker-compose.yml -o docker-compose.yml
curl -fsSL https://raw.githubusercontent.com/jptsantossilva/BEC/main/.env.example -o .env

openssl rand -base64 32
nano .env
```

Copy the generated password into `SQLITE_WEB_PASSWORD`. Replace the sample
`xxx` values downloaded from `.env.example`; leave unused exchange and optional
integration credentials empty:

```ini
# Binance production
binance_api=""
binance_secret=""

# Kraken production
KRAKEN_API_KEY=""
KRAKEN_API_SECRET=""

# OKX production
OKX_API_KEY=""
OKX_API_SECRET=""
OKX_API_PASSPHRASE=""

# Required by Docker Compose
SQLITE_WEB_PASSWORD=""

# Optional Telegram integration
telegram_chat_id=""
telegram_token_closed_positions=""
telegram_token_errors=""
telegram_token_main=""
telegram_token_signals=""

# Optional AI analysis
OPENAI_API_KEY=""
BEC_OPENAI_MODEL="gpt-5.5"
```

`OPENAI_API_KEY` is needed only for AI analysis in Backtesting Results.
`BEC_OPENAI_MODEL` is optional and defaults to `gpt-5.5`.

Start BEC:

```bash
sudo docker compose pull
sudo docker compose up -d
```

The stack provides:

- **Dashboard** at `http://localhost:8080`.
- **SQLite web** at `http://localhost:8081`, password-protected and bound only
  to the Docker host loopback interface.
- **Jobs runner** for scheduled trading, analysis, and reconciliation tasks.

### First-run checklist

1. Open the dashboard and select the intended active exchange.
2. Configure its quote asset and exchange-specific backtesting fee.
3. Run the public API health check and inspect the available spot markets.
4. Configure strategies, sizing, risk controls, and schedules.
5. Run and approve exchange-specific backtests before enabling live buys.
6. Configure credentials and run the read-only private API validation.
7. For Kraken or OKX, review the gated live-execution requirements in the
   dashboard before enabling buy or sell operations.
8. Keep live flags and automatic trading schedules disabled until the public
   and private operational checks are complete.

## Operations

Useful commands:

```bash
docker compose ps
docker compose logs -f dashboard
docker compose logs -f jobs_runner
docker compose restart
docker compose down
```

`docker compose down` stops the stack while preserving its persistent volume.

To access SQLite web from another computer, use an SSH tunnel instead of
publishing port 8081:

```bash
ssh -L 8081:127.0.0.1:8081 user@your-server
```

Keep the tunnel open and browse to `http://127.0.0.1:8081` locally.

> [!CAUTION]
> `docker compose down -v` deletes the persistent database volume. Do not use
> it when updating an existing installation unless permanent data removal is
> intentional.

## Updating

```bash
cd /opt/bec
sudo docker compose pull
sudo docker compose up -d
```

Review the [changelog](https://github.com/jptsantossilva/BEC/blob/main/CHANGELOG.md)
before updating a production installation.

### Updating with Dockge

If you prefer a web interface, BEC can be managed as a Docker Compose stack in
[Dockge](https://github.com/louislam/dockge).

1. Install Dockge and give it access to the BEC stack directory:

   ```bash
   sudo mkdir -p /opt/dockge/data
   cd /opt/dockge
   ```

2. Create `/opt/dockge/compose.yaml`:

   ```yaml
   services:
     dockge:
       image: louislam/dockge:1
       restart: unless-stopped
       ports:
         - "127.0.0.1:5001:5001"
       volumes:
         - /var/run/docker.sock:/var/run/docker.sock
         - ./data:/app/data
         - /opt/bec:/opt/bec
       environment:
         - DOCKGE_STACKS_DIR=/opt/bec
   ```

3. Start Dockge:

   ```bash
   docker compose -f /opt/dockge/compose.yaml up -d
   ```

4. Open `http://localhost:5001`, create the administrator account, and import
   the BEC stack. Use **Update**, or **Pull** followed by **Redeploy**, when a
   new BEC image is available.

Keep Dockge private and administrator-only. Do not expose it publicly without
TLS, strong authentication, and network access restrictions.

## Project layout

- `bec/` contains the application package, exchange adapters, trading logic,
  backtesting, strategies, and utilities.
- `pages/` contains the Streamlit dashboard pages.
- `docs/` contains the VitePress user manual.
- `dashboard.py`, `jobs_runner.py`, and `main.py` are compatibility entrypoints
  used by Docker, Streamlit, and scheduled commands.
- `docker/`, `Dockerfile`, and `docker-compose.yml` define the container runtime.
- `data.db`, `main.log`, and generated backtesting output are local runtime
  state and must not be committed.

## Development

BEC targets Python 3.12.

```bash
python -m pip install -r requirements.txt
python -m pytest tests
streamlit run dashboard.py --server.port=8080
```

Build the documentation site with:

```bash
npm install
npm run docs:build
```

## Disclaimer

BEC is provided for educational purposes only. Trading involves substantial
risk and automated trading can lose money. Use this software at your own risk,
never trade money you cannot afford to lose, and independently verify its
behavior before enabling live orders. The project is provided without warranty,
and its authors and contributors accept no responsibility for trading results.

## License

BEC is released under the [MIT License](https://github.com/jptsantossilva/BEC/blob/main/LICENSE.md).

Copyright © 2026 [João Silva](https://github.com/jptsantossilva)

## Support the project

If BEC is useful to you, you can support its continued development:

<a href="https://ko-fi.com/C0C3TDKPG" target="_blank"><img height="36" style="border:0;height:36px" src="https://storage.ko-fi.com/cdn/kofi3.png?v=3" alt="Support BEC on Ko-fi" /></a>
