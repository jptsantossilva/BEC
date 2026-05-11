# Troubleshooting

## What This Page Is For

Use this page when the dashboard loads but data, jobs, reports, or integrations are not working as expected.

## Check Logs First

Most issues are visible in container logs:

```bash
docker compose logs -f dashboard
docker compose logs -f jobs_runner
```

Use `dashboard` logs for UI errors and `jobs_runner` logs for scheduled jobs, backtests, and Monte Carlo jobs.

## Missing Credentials

If Binance, Telegram, or AI features fail, check `.env`.

Required for trading and market data:

- `binance_api`
- `binance_secret`

Required for Telegram alerts:

- `telegram_chat_id`
- Telegram token values

Required only for AI analysis:

- `OPENAI_API_KEY`

## No Backtesting Results

Check that jobs are enabled and running. Then open **Backtesting > Backtesting Results** and inspect the Backtesting Queue.

If jobs are stuck, restart the services:

```bash
docker compose restart
```

## Missing Reports

Backtesting and Monte Carlo reports are stored under the persistent backtest results folder. In Docker, BEC links this path into:

```text
/app/static/backtest_results
```

If a report is missing, rerun the selected backtest or Monte Carlo job and check the job log.

## SQLite Web

SQLite web is available at:

```text
http://localhost:8081
```

Change the default `SQLITE_WEB_PASSWORD` in `docker-compose.yml` before exposing the service to any network.
