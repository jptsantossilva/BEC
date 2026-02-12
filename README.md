Like my work?

<a href='https://ko-fi.com/C0C3TDKPG' target='_blank'><img height='36' style='border:0px;height:36px;' src='https://storage.ko-fi.com/cdn/kofi3.png?v=3' border='0' alt='Buy Me a Coffee at ko-fi.com' /></a>

# BEC
Free and open-source automated trading app for Binance spot, combining multi-strategy execution, daily market-phase ranking, automated backtesting, a web dashboard, and Telegram notifications.

## Installation
### Local deploy with Docker


```bash
mkdir -p bec
cd bec

curl -fsSL https://raw.githubusercontent.com/jptsantossilva/BEC/main/docker-compose.yml -o docker-compose.yml
curl -fsSL https://raw.githubusercontent.com/jptsantossilva/BEC/main/.env.example -o .env

nano .env
```
Fill `.env` with your Binance and Telegram credentials.

```ini
# Binance API
binance_api=
binance_secret=

# Telegram
telegram_chat_id=
telegram_token_closed_positions=
telegram_token_errors=
telegram_token_main=
telegram_token_signals=
```
```bash
sudo docker compose pull
sudo docker compose up -d
```

What you get:
- **Dashboard**: web UI at `http://localhost:8080` for monitoring PnL, balances, open positions, scheduled jobs, backtesting results, and settings
- **SQLite web**: admin interface at `http://localhost:8081` to inspect the database.

Useful commands:
- `docker compose ps` — show running containers and their status.
- `docker compose logs -f dashboard` — live logs from the dashboard.
- `docker compose logs -f jobs_runner` — live logs from the scheduler.
- `docker compose restart` — restart all services.
- `docker compose down` — stop and remove containers (keeps volumes).

To remove everything:
```bash
docker compose down -v
```

## Updates
Follow the last updates from the change log [here](https://github.com/jptsantossilva/BEC/blob/main/CHANGELOG.md)

## Features
- Trades on 1-day, 4-hour, and 1-hour timeframes.
- Trades against stable pairs (USDT/USDC) or BTC.
- Daily market-phase ranking to select symbols in accumulation or bullish conditions.
- Backtesting across all available strategies and timeframes to select the best performers.
- Auto-scheduling of jobs (bot runs, market phase rebuilds, signals) via the built-in scheduler.
- Web dashboard with realized/unrealized PnL, balances, top performers, backtesting results, and settings.
- Telegram notifications for bot execution, position changes, and warnings.
- Blacklist management to exclude symbols from trading.

![dashboard](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/dashboard.png)

## Disclaimer
This software is for educational purposes only. Use the software at **your own risk**. The authors and all affiliates assume **no responsibility for your trading results**. **Do not risk money that you are afraid to lose**. There might be **bugs** in the code. This software does not come with **any warranty**.

## 📝 License

This project is [MIT](https://github.com/jptsantossilva/BEC/blob/main/LICENSE.md) licensed.

Copyright © 2026 (https://github.com/jptsantossilva)
